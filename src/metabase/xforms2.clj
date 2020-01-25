(ns metabase.xforms2
  (:require [metabase
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
  (let [rsmeta   (.getMetaData rs)
        col-meta (rf init (col-meta rsmeta))
        read-row (read-row-fn driver rs rsmeta)]
    (println "<Consuming results>")
    (loop [result init]
      (if-not (.next rs)
        result
        (let [row    (read-row)
              result (rf result col-meta row)]
          (if (reduced? result)
            @result
            (recur result)))))))

(defn- print-rows-rf
  ([] 0)

  ([row-count] {:rows row-count})

  ([row-count col-meta]
   (println "COLS ->" (pr-str col-meta))
   row-count)

  ([row-count _ row]
   (println (format "ROW %d ->" (inc row-count)) (pr-str row))
   (inc row-count)))

(defn- print-rows-to-writer-rf [^java.io.Writer writer]
  (fn
    ([] 0)

    ([row-count] {:rows row-count})

    ([_ col-meta]
     (.write writer (str "COLS -> " (pr-str (map :name col-meta)) "\n"))
     col-meta)

    ([row-count _ row]
     (.write writer (format "ROW %d -> %s\n" (inc row-count) (pr-str row)))
     (inc row-count))))

(defn- rows-xform [rf]
  (fn
    ([]
     (rf))

    ([result]
     (rf result))

    ([result results-meta]
     (rf result (conj (vec results-meta) {:name :extra-col})))

    ([acc results-meta row]
     (rf acc results-meta (conj row "Neat!")))))

#_(defn process-query [driver query rf]
  (transduce
   (comp rows-xform rows-xform)
   rf
   (reducible-results driver query)))


(defn middleware-1 [qp]
  (fn [query respond raise xform rf canceled-chan]
    (println "<IN MIDDLEWARE 1>")
    (qp query respond raise xform rf canceled-chan)))

(defn- middleware-1 [qp]
  (fn [query xform rf]
    (println "IN MIDDLEWARE 1!")
    (qp query xform rf)))

(defn- middleware-2 [qp]
  (fn [query xform rf]
    (qp query (comp xform rows-xform) rf)))

(defn- pipeline [f]
  (-> f
      middleware-1
      middleware-2))

(defn process-query [driver query rf]
  ((pipeline
     (fn [query xform rf]
       (transduce xform rf (reducible-results driver query))))
   query
   identity
   rf))


;;; ------------------------------------------------------ test ------------------------------------------------------

(defn- x []
  (process-query :postgres "SELECT * FROM users ORDER BY id ASC LIMIT 5;" print-rows-rf))

(defn- x2 []
  (with-open [w (clojure.java.io/writer "/Users/cam/Desktop/test.txt")]
    (process-query :postgres "SELECT * FROM users ORDER BY id ASC LIMIT 5;" (print-rows-to-writer-rf w))))

(defn- maps-rf
  ([] [])
  ([acc] acc)
  ([_ results-meta] results-meta)
  ([acc col-meta row] (conj acc (zipmap (map (comp keyword :name) col-meta)
                                        row))))

(defn- y []
  (process-query :postgres "SELECT * FROM users ORDER BY id ASC LIMIT 5;" maps-rf))
