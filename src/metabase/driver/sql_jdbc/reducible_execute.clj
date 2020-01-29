(ns metabase.driver.sql-jdbc.reducible-execute
  (:require [clojure.core.async :as a]
            [metabase
             [driver :as driver]
             [util :as u]]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.query-processor
             [interface :as qp.i]
             [store :as qp.store]])
  (:import [java.sql Connection JDBCType ResultSet ResultSetMetaData Types]
           javax.sql.DataSource))

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

(defn- reducible-query [driver ^DataSource datasource ^String sql params raise cancel-chan]
  {:pre [(string? sql) (seq sql)]}
  (reify
    clojure.lang.IReduceInit
    (reduce [_ rf init-fn]
      (try
        (locking println (println "<Opening connection>"))
        (with-open [conn (doto (.getConnection datasource)
                           (.setAutoCommit false)
                           (.setReadOnly true)
                           (.setTransactionIsolation Connection/TRANSACTION_READ_UNCOMMITTED))
                    stmt (doto (.prepareStatement conn sql
                                                  ResultSet/TYPE_FORWARD_ONLY
                                                  ResultSet/CONCUR_READ_ONLY
                                                  ResultSet/CLOSE_CURSORS_AT_COMMIT)
                           (.setFetchDirection ResultSet/FETCH_FORWARD))]
          (dorun
           (map-indexed
            (fn [i param]
              (println "Set param" (inc i) "->" (pr-str param)) ; NOCOMMIT
              (metabase.driver.sql-jdbc.execute/set-parameter driver stmt (inc i) param))
            params))
          (with-open [rs (.executeQuery stmt)]
            ;; if cancel-chan gets a message, cancel the PreparedStatement
            (a/go
              (when (a/<! cancel-chan)
                (locking println (println "Query canceled, calling PreparedStatement.cancel()"))
                (.cancel stmt)))
            (let [rsmeta       (.getMetaData rs)
                  read-row     (read-row-fn driver rs rsmeta)
                  results-meta {:cols (col-meta rsmeta)}]
              (qp.i/maybe-with-open [init (init-fn)]
                (loop [result (rf init results-meta)]
                  (if-not (.next rs)
                    (do
                      (locking println (println "<All rows consumed.>"))
                      result)
                    (let [row    (read-row)
                          result (rf result results-meta row)]
                      (if (reduced? result)
                        @result
                        (recur result)))))))))
        (catch Throwable e
          (raise e))
        (finally
          (locking println (println "<closing connection>")))))))

(defn execute-query-reducible
  "Default implementation of `driver/execute-query-reducible` for `:sql-jdbc` drivers."
  [driver {{:keys [query params]} :native} respond raise cancel-chan]
  (let [{:keys [datasource]} (sql-jdbc.conn/db->pooled-connection-spec (qp.store/database))]
    (respond (reducible-query driver datasource query params raise cancel-chan))))
