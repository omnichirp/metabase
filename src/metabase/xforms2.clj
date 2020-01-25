(ns metabase.xforms2
  (:require [clojure.core.async :as a]
            [metabase
             [driver :as driver]
             [test :as mt]
             [util :as u]]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn])
  (:import [java.sql Connection JDBCType ResultSet ResultSetMetaData Types]
           javax.sql.DataSource))

;; New QP style

(defn- jdbc-spec []
  (sql-jdbc.conn/db->pooled-connection-spec (mt/with-driver :postgres (mt/db))))

(defn- datasource ^DataSource [] (:datasource (jdbc-spec)))

(def read-column-fn nil) ; NOCOMMIT

(defmulti read-column-fn
  "Should return a zero-arg function that will fetch the value of the column from the current row."
  {:arglists '([driver rs rsmeta i])}
  (fn [driver _ ^ResultSetMetaData rsmeta ^long col-idx]
    [(driver/dispatch-on-initialized-driver driver) (.getColumnType rsmeta col-idx)])
  :hierarchy #'driver/hierarchy)

(defmethod read-column-fn :default
  [_ ^ResultSet rs _ ^long col-idx]
  ^{:name (format "(.getObject rs %d)" col-idx)}
  (fn []
    (.getObject rs col-idx)))

(defn- get-object-of-class-fn [^ResultSet rs, ^long col-idx, ^Class klass]
  ^{:name (format "(.getObject rs %d %s)" col-idx (.getCanonicalName klass))}
  (fn []
    (.getObject rs col-idx klass)))

(defmethod read-column-fn [:sql-jdbc Types/TIMESTAMP]
  [_ rs _ i]
  (get-object-of-class-fn rs i java.time.LocalDateTime))

(defn- log-readers [driver ^ResultSetMetaData rsmeta fns]
  (doseq [^Integer i (range 1 (inc (.getColumnCount rsmeta)))]
    (printf "Reading %s column %d (JDBC type: %s, DB type: %s) with %s\n"
            driver
            i
            (or (u/ignore-exceptions
                  (.getName (JDBCType/valueOf (.getColumnType rsmeta i))))
                (.getColumnType rsmeta i))
            (.getColumnTypeName rsmeta i)
            (let [f (nth fns (dec i))]
              (or (:name (meta f))
                  f)))))

(defn- read-row-fn [driver rs ^ResultSetMetaData rsmeta]
  (let [fns (for [col-idx (range 1 (inc (.getColumnCount rsmeta)))]
              (read-column-fn driver rs rsmeta (long col-idx)))]
    (log-readers driver rsmeta fns)
    (apply juxt fns)))

(defn- col-meta [^ResultSetMetaData rsmeta]
  (mapv
   (fn [^Integer i]
     {:name      (or (.getColumnLabel rsmeta i)
                     (.getColumnName rsmeta i))
      :jdbc_type (u/ignore-exceptions
                   (.getName (JDBCType/valueOf (.getColumnType rsmeta i))))
      :db_type   (.getColumnTypeName rsmeta i)})
   (range 1 (inc (.getColumnCount rsmeta)))))

(declare reducible-result-set)

(defn- reducible-results
  [driver ^String sql]
  (reify
    clojure.lang.IReduce
    (reduce [this rf]
      (.reduce ^clojure.lang.IReduceInit this rf (rf)))

    clojure.lang.IReduceInit
    (reduce [_ rf init]
      (println "<Running query>")
      (with-open [conn (doto (.getConnection (datasource))
                         (.setAutoCommit false)
                         (.setReadOnly true)
                         (.setTransactionIsolation Connection/TRANSACTION_READ_UNCOMMITTED))
                  stmt (doto (.prepareStatement conn sql
                                                ResultSet/TYPE_FORWARD_ONLY
                                                ResultSet/CONCUR_READ_ONLY
                                                ResultSet/CLOSE_CURSORS_AT_COMMIT)
                         (.setFetchDirection ResultSet/FETCH_FORWARD))
                  rs   (.executeQuery stmt)]
        (reducible-result-set driver rs rf init)))))

(defn- reducible-result-set [driver ^ResultSet rs rf init]
  (let [rsmeta       (.getMetaData rs)
        results-meta (rf init {:cols (col-meta rsmeta)})
        read-row     (read-row-fn driver rs rsmeta)]
    (println "<Consuming results>")
    (loop [result init]
      (if-not (.next rs)
        result
        (let [row    (read-row)
              result (rf result results-meta row)]
          (if (reduced? result)
            @result
            (recur result)))))))

(defn- print-rows-rf
  ([] 0)

  ([row-count] {:rows row-count})

  ([row-count results-meta]
   (println "results meta ->\n" (u/pprint-to-str 'blue results-meta))
   row-count)

  ([row-count _ row]
   (println (format "ROW %d ->" (inc row-count)) (pr-str row))
   (inc row-count)))

(defn- print-rows-to-writer-rf [^java.io.Writer writer]
  (fn
    ([] 0)

    ([row-count] {:rows row-count})

    ([_ results-meta]
     (.write writer (str "results meta -> " (pr-str results-meta) "\n"))
     results-meta)

    ([row-count _ row]
     (.write writer (format "ROW %d -> %s\n" (inc row-count) (pr-str row)))
     (inc row-count))))

(defn- add-a-column-xform [rf]
  (fn
    ([]
     (rf))

    ([result]
     (rf result))

    ([result results-meta]
     (rf result (update results-meta :cols (fn [cols]
                                             (conj (vec cols) {:name :extra-col})))))

    ([acc results-meta row]
     (rf acc results-meta (conj row "Neat!")))))

(defn- middleware-1 [qp]
  (fn [query results-xform chans]
    (println "IN MIDDLEWARE 1!")
    (qp query results-xform chans)))

(defn- add-a-column-middleware [qp]
  (fn [query results-xform chans]
    (qp query (comp results-xform add-a-column-xform) chans)))

(defn- async-middleware [qp]
  (fn [query results-xform {:keys [cancel-chan], :as chans}]
    (let [futur (future
                  (println "Sleep 500.")
                  (Thread/sleep 500)
                  (println "Done sleeping.")
                  (qp query results-xform chans))]
      (a/go
        (when (a/<! (:cancel-chan chans))
          (future-cancel futur))))
    nil))

(defn- build-pipeline [f]
  (-> f
      middleware-1
      async-middleware
      add-a-column-middleware))

(def ^{:arglists '([query results-xform chans])} pipeline
  (build-pipeline
   (fn [query results-xform {:keys [cancel-chan out-chan rf]}]
     (try
       (let [result (transduce results-xform rf (reducible-results :postgres query))]
         (a/put! out-chan (or result :no-result)))
       (catch Throwable e
         (a/put! out-chan e))
       (finally
         (a/close! cancel-chan)
         (a/close! out-chan))))))

(defn process-query-async [query rf]
  (println "Starting query.")
  (let [out-chan    (a/promise-chan)
        cancel-chan (a/promise-chan)
        chans       {:out-chan out-chan, :cancel-chan (a/promise-chan)}
        ;; TODO - should use a bounded threadpool for this
        futur       (future (pipeline query identity (assoc chans :rf rf)))]
    (a/go
      (when (a/<! (:cancel-chan chans))
        (println "Query canceled.")
        (future-cancel futur)
        (a/put! out-chan {:status :canceled})
        (a/close! out-chan)
        (a/close! cancel-chan)))
    chans))

(defn process-query [query rf]
  ;; TODO - timeout?
  (let [{:keys [out-chan]} (process-query-async query rf)
        result             (a/<!! out-chan)]
    (println "<< QUERY FINISHED >>")
    (when (instance? Throwable result)
      (throw result))
    result))


;;; ------------------------------------------------------ test ------------------------------------------------------

(defn- x []
  (process-query "SELECT * FROM users ORDER BY id ASC LIMIT 5;" print-rows-rf))

(defn- x2 []
  (with-open [w (clojure.java.io/writer "/Users/cam/Desktop/test.txt")]
    (process-query "SELECT * FROM users ORDER BY id ASC LIMIT 5;" (print-rows-to-writer-rf w))))

(defn- maps-rf
  ([] [])
  ([acc] acc)
  ([_ results-meta] results-meta)
  ([acc results-meta row] (conj acc (zipmap (map (comp keyword :name) (:cols results-meta))
                                            row))))

(defn- y []
  (process-query "SELECT * FROM users ORDER BY id ASC LIMIT 5;" maps-rf))

(defn- z []
  (let [{:keys [cancel-chan out-chan]} (process-query-async "SELECT * FROM users ORDER BY id ASC LIMIT 5;" maps-rf)]
    (a/put! cancel-chan :cancel)
    (a/<!! out-chan)))
