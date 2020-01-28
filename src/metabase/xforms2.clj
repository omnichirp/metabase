(ns metabase.xforms2
  (:require [clojure.core.async :as a]
            [metabase
             [sql-jdbc-xforms :as sql-jdbc-xforms]
             [util :as u]]))

(set! *warn-on-reflection* true)

;; Signatures:
;;
;; (Driver) execute-query:
;;
;; (execute-query query respond raise cancel-chan) -> reducible result
;;
;; Middleware:
;;
;; (middleware qp) -> (fn [query xform respond raise cancel-chan])
;;
;; qp5:
;;
;; (qp5 query rf respond raise cancel-chan) -> ?
;;
;; qp2
;;
;; (qp query rf) -> ?
;;
;; qp1
;;
;; (qp1 query) -> ?

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                              Sample Middleware/QP                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- execute* [query xform respond raise cancel-chan]
  (println "execute* raise:" raise)
  (sql-jdbc-xforms/execute-query query (partial respond xform) raise cancel-chan))

(defn- build-qp5 [middleware]
  (let [qp (reduce
            (fn [qp middleware]
              (middleware qp))
            execute*
            middleware)]
    (fn qp5* [query rf respond raise cancel-chan]
      (letfn [(respond* [xform result]
                (if-not (instance? clojure.lang.IReduceInit result)
                  (respond result)
                  (try
                    (locking println (println "[REDUCING]"))
                    (respond (transduce xform rf (rf) result))
                    (locking println (println "[REDUCED]"))
                    nil
                    (catch Throwable e
                      (raise e)))))]
        (try
          (qp query identity respond* raise cancel-chan)
          (catch Throwable e
            (raise e)))))))

(defn- async-qp2 [qp5 timeout-ms]
  (fn [query rf]
    (let [result-chan (a/promise-chan)
          cancel-chan (a/promise-chan)]
      ;; wait for results, then pass to result-chan
      (a/go
        (let [[val port] (a/alts!! [result-chan (a/timeout timeout-ms)])]
          (locking println (println "async-qp2 got result" (class val) "from port" (if (= port result-chan) "result-chan" "timeout chan")))
          (when-not (= port result-chan)
            (a/put! cancel-chan :cancel)
            (a/put! result-chan {:status  :timed-out
                                 :message (format "Timed out after %s." (u/format-milliseconds timeout-ms))})
            (a/close! cancel-chan)
            (a/close! result-chan))))
      ;; listen for cancelation and reply with {:status :canceled}
      (a/go
        (when (a/<! cancel-chan)
          (locking println (println "async-qp2 got cancel message (canceling query)"))
          (a/put! result-chan {:status :canceled})
          (a/close! result-chan)
          (a/close! cancel-chan)))
      (letfn [(async-qp2-respond [result]
                (locking println (println "async-qp2-respond got result" (class result)))
                (when (some? result)
                  (a/put! result-chan result))
                (a/close! result-chan)
                (a/close! cancel-chan))
              (async-qp2-raise [e]
                (locking println (println "async-qp2-raise got exception" (class e) (.getMessage ^Throwable e)))
                (a/put! result-chan e)
                (a/close! result-chan)
                (a/close! cancel-chan))]
        (qp5 query rf async-qp2-respond async-qp2-raise cancel-chan))
      {:result-chan result-chan
       :cancel-chan cancel-chan})))

(defn- sync-qp2 [qp5 timeout-ms]
  (let [qp2 (async-qp2 qp5 timeout-ms)]
    (fn [query rf]
      (let [{:keys [result-chan]} (qp2 query rf)
            result                (a/<!! result-chan)]
        (if (instance? Throwable result)
          (throw result)
          result)))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                               Default/Test Impls                                               |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn default-rf
  ([] {:data {:rows []}})

  ([results] results)

  ([results results-meta]
   (update results :data merge results-meta))

  ([results _ row]
   (update-in results [:data :rows] conj row)))

(defn- middleware-1 [qp]
  (fn [query xform respond raise cancel-chan]
    (locking println (println "IN MIDDLEWARE 1!"))
    (qp query xform respond raise cancel-chan)))

(defn- add-a-column-xform [rf]
  (fn
    ([result]
     (rf result))

    ([result results-meta]
     (rf result (update results-meta :cols (fn [cols]
                                             (conj (vec cols) {:name (format "extra-col-%d" (inc (count (:cols results-meta))))})))))

    ([acc results-meta row]
     (rf acc results-meta (conj row "Neat!")))))

(defn- add-a-column-middleware
  "Adds an extra column to the results."
  [qp]
  (fn [query xform respond raise cancel-chan]
    (let [xform' (comp xform add-a-column-xform)]
      (qp query xform' respond raise cancel-chan))))

(defn- async-middleware
  "Adds some async sleeping."
  [qp]
  (fn [query xform respond raise cancel-chan]
    (let [futur (future
                  (try
                    (locking println (println "Sleep 50."))
                    (Thread/sleep 50)
                    (locking println (println "Done sleeping."))
                    (qp query xform respond raise cancel-chan)
                    (catch Throwable e
                      (raise e))))]
      (a/go
        (when (a/<! cancel-chan)
          (locking println (println "In async-middleware, canceling sleep."))
          (future-cancel futur)))
      nil)))

(defn- async-cancel-middleware
  "Runs query on a separate thread and cancel-chans it, returning `:cancel-chaned` response when query is cancel-chaned."
  [qp]
  (fn [query xform respond raise cancel-chan]
    (let [futur (future
                  (try
                    (qp query xform respond raise cancel-chan)
                    (catch Throwable e
                      (raise e))))]
      (a/go
        (when (a/<! cancel-chan)
          (locking println (println "In async-cancel-middleware, canceling query."))
          (future-cancel futur)))
      nil)))

(defn add-ex-info-middleware [qp]
  (fn [query xform respond raise canceled-chan]
    (letfn [(raise' [e]
              (raise (ex-info "WHoopS!" {:extra-info? true} e)))]
      (qp query xform respond raise' canceled-chan))))

(def default-middleware
  [add-a-column-middleware async-middleware middleware-1 add-ex-info-middleware async-cancel-middleware])

(def ^{:arglists '([query rf respond raise cancel-chan])} default-qp5
  (u/profile "Build qp5"
    (build-qp5 default-middleware)))

;; ;;; +----------------------------------------------------------------------------------------------------------------+
;; ;;; |                                             Sample RFs / Test Fns                                              |
;; ;;; +----------------------------------------------------------------------------------------------------------------+

(defn- print-rows-rf
  ([] 0)

  ([row-count] row-count)

  ([row-count results-meta]
   (locking println (println "results meta ->\n" (u/pprint-to-str 'blue results-meta)))
   row-count)

  ([row-count _ row]
   (locking println (println (u/format-color 'yellow "ROW %d ->" (inc row-count)) (pr-str row)))
   (inc row-count)))

(defn- print-rows-to-writer-rf [^java.io.Writer writer]
  (fn
    ([] 0)

    ([row-count] {:rows row-count})

    ([row-count results-meta]
     (.write writer (str "results meta -> " (pr-str results-meta) "\n"))
     row-count)

    ([row-count _ row]
     (.write writer (format "ROW %d -> %s\n" (inc row-count) (pr-str row)))
     (inc row-count))))

(defn- maps-rf
  ([] {})

  ([[_ results]] results)

  ([results results-meta]
   [(mapv (comp keyword :name) (:cols results-meta))
    (merge results-meta results)])

  ([[col-names results] _ row]
   [col-names
    (update results :rows (fn [rows]
                            (conj rows (zipmap col-names row))))]))


;;; ------------------------------------------------------ test ------------------------------------------------------

(def ^{:arglists '([query] [query rf])} process-query-async
  (let [qp (async-qp2 default-qp5 5000)]
    (fn
      ([query]
       (qp query default-rf))
      ([query rf]
       (qp query rf)))))

(def ^{:arglists '([query] [query rf])} process-query
  (let [qp (sync-qp2 default-qp5 5000)]
    (fn
      ([query]
       (qp query default-rf))
      ([query rf]
       (qp query rf)))))

(defn- default-example []
  (process-query "SELECT * FROM users ORDER BY id ASC LIMIT 5;"))

(defn- print-rows-example []
  (process-query "SELECT * FROM users ORDER BY id ASC LIMIT 5;" print-rows-rf))

(defn- print-rows-to-file-example []
  (with-open [w (clojure.java.io/writer "/Users/cam/Desktop/test2.txt")]
    (let [rf (print-rows-to-writer-rf w)]
      (process-query "SELECT * FROM users ORDER BY id ASC LIMIT 5;" rf))))

(defn- maps-example []
  (process-query "SELECT * FROM users ORDER BY id ASC LIMIT 5;" maps-rf))

(defn- cancel-chan-example []
  (let [{:keys [cancel-chan result-chan]} (process-query-async "SELECT * FROM users ORDER BY id ASC LIMIT 5;" maps-rf)]
    (a/put! cancel-chan :cancel-chan)
    (a/<!! result-chan)))

(defn- exception-example []
  (process-query "SELECT asdasdasd;"))

;;; ------------------------------------------------ Userland Example ------------------------------------------------

(defn- exception-chain [^Throwable e]
  (->> (iterate
        (fn [^Throwable e]
          (when-let [cause (or (when (instance? java.sql.SQLException e)
                                 (.getNextException ^java.sql.SQLException e))
                               (.getCause e))]
            (when-not (= e cause)
              cause)))
        e)
       (take-while some?)
       reverse))

(defn- format-exception [^Throwable e]
  (merge
   {:type (.getCanonicalName (class e))
    :message    (.getMessage e)
    :stacktrace (u/filtered-stacktrace e)}
   (when-let [data (ex-data e)]
     {:data data})
   (when (instance? java.sql.SQLException e)
     {:state (.getSQLState ^java.sql.SQLException e)})))

(defn- exception-response [^Throwable e]
  (let [[e-info & more] (for [e (exception-chain e)]
                          (format-exception e))]
    (merge
     {:status :failed}
     e-info
     (when (seq more)
       {:via (vec more)}))))

;; TODO - or should this be some sort of middleware? For async situations
(defn userland-exception-middleware [qp]
  (fn [query xform respond _ canceled-chan]
    (println "respond:" respond)        ; NOCOMMIT
    (letfn [
            (raise [e]
              (respond nil (exception-response e)))]
      (qp query xform respond raise canceled-chan))))

(def ^{:arglists '([query] [query rf])} process-userland-query
  (let [qp (sync-qp2 (build-qp5 (concat default-middleware [userland-exception-middleware]))
                     5000)]
    (fn
      ([query]
       (qp query default-rf))
      ([query rf]
       (qp query rf)))))

(defn userland-query-example []
  (process-userland-query "SELECT asdadasds;"))
