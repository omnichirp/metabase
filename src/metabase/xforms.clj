(ns metabase.xforms
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

(defn- consume-results
  "Consumes rows in ResultSet `rs` using reducing fn `rf`."
  [driver rf ^ResultSet rs]
  (let [rsmeta   (.getMetaData rs)
        col-meta (col-meta rsmeta)
        read-row (read-row-fn driver rs rsmeta)
        rf       (rf col-meta)]
    (println "<Consuming results>")
    (loop [result (rf)]
      (if-not (.next rs)
        (rf result)
        (let [row    (read-row)
              result (rf result row)]
          (if (reduced? result)
            @result
            (recur result)))))))

(defn- run-query
  [driver ^String sql rf]
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
    (consume-results driver rf rs)))

(defn- print-rows-rf [col-meta]
  (println "COLS ->" (pr-str (map :name col-meta)))
  (fn
    ([] 0)

    ([row-count] {:rows row-count})

    ([row-count row]
     (println (format "ROW %d ->" (inc row-count)) (pr-str row))
     (inc row-count))))

(defn- rows->maps-rf [col-meta]
  (let [col-names (map (comp keyword :name) col-meta)]
    (fn
      ([] [])
      ([acc] acc)
      ([acc row]
       (conj acc (zipmap col-names row))))))

(defn- print-rows-to-writer-rf [^java.io.Writer writer, col-meta]
  (.write writer (str "COLS -> " (pr-str (map :name col-meta)) "\n"))
  (fn
    ([] 0)

    ([row-count] {:rows row-count})

    ([row-count row]
     (.write writer (format "ROW %d -> %s\n" (inc row-count) (pr-str row)))
     (inc row-count))))

(defn- x []
  (with-open [w (clojure.java.io/writer "/Users/cam/Desktop/test.txt")]
    (let [result (run-query :postgres "SELECT * FROM users ORDER BY id ASC LIMIT 5;" (partial print-rows-to-writer-rf w))]
      (println "Done." result))))




#_(defn- middleware [qp]
    (fn [query respond rows-xform raise canceled-chan]
      (qp query respond rows-xform raise canceled-chan)))
