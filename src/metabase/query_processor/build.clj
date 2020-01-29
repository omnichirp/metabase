(ns metabase.query-processor.build
  "Logic for building a function that can process MBQL queries."
  (:require [clojure.core.async :as a]
            [metabase
             [config :as config]
             [driver :as driver]
             [util :as u]]))

;; Signatures:
;;
;; (Driver) execute-query-reducible:
;;
;; (execute-query-reducible: driver query respond raise cancel-chan) -> reducible result
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

(def query-timeout-ms
  "Maximum amount of time to wait for a running query to complete before throwing an Exception."
  ;; I don't know if these numbers make sense, but my thinking is we want to enable (somewhat) long-running queries on
  ;; prod but for test and dev purposes we want to fail faster because it usually means I broke something in the QP
  ;; code
  (cond
    config/is-prod? (u/minutes->ms 20)
    config/is-test? (u/seconds->ms 30)
    config/is-dev?  (u/minutes->ms 3)))

(defn- execute-reducible* [query respond raise cancel-chan]
  (driver/execute-query-reducible driver/*driver* query respond raise cancel-chan))

(defn build-query-processor
  "Returns a query processor function with the signature

    (qp5 query rf respond raise canceled-chan)

  with a sequence of query processor `middleware`.

  If the query succeeds, the results of the query will transduced using `rf` and transforms added by `middleware`;
  `respond` will be called asynchronously with the final transduced result. If query execution fails at any point,
  `raise` will be called with the Exception that was thrown.

  If `execute-fn` is passed, it should have the signature

    (execute-fn query respond raise cancel-chan)

  and call `respond` with reducible results."
  ([middleware]
   (build-query-processor execute-reducible* middleware))

  ([execute-fn middleware]
   (let [qp (reduce
             (fn [qp middleware]
               (middleware qp))
             (fn [query xform respond raise cancel-chan]
               (execute-fn query (partial respond xform) raise cancel-chan))
             middleware)]
     (fn qp5* [query rf respond raise cancel-chan]
       (letfn [(respond* [xform result]
                 (if (a/poll! cancel-chan)
                   (locking println (println "Query already canceled, skipping run."))
                   (if-not (instance? clojure.lang.IReduceInit result)
                     (raise (ex-info "result is not a clojure.lang.IReduceInit"
                              {:result result}))
                     (try
                       (respond (transduce xform rf rf result))
                       (catch Throwable e
                         (raise e))))))]
         (try
           (qp query identity respond* raise cancel-chan)
           (catch Throwable e
             (raise e))))))))

(defn default-rf
  "Default reducing function. Returns the 'standard' map with `{:data {:cols ..., :rows ...}}`"
  ([] {:data {:rows []}})

  ([results] results)

  ([results results-meta]
   (update results :data merge results-meta))

  ([results _ row]
   (update-in results [:data :rows] conj row)))

(defn async-query-processor
  "Returns an async query processor function with the signatures

    (qp2 query) and (qp2 query rf)

  Calling this function returns a pair of `core.async` channels like

    {:result-chan (a/promise-chan), :exception-chan (a/promise-chan)}

  That can be used to listen for the results or cancel the query. Closing `result-chan` before the query completes
  will cancel the query.

  If a query is executed successfully, the results will be transduced using `rf` and transforms supplied by the
  middleware; the final result will be passed to `result-chan`. If query execution fails, the thrown Exception will be
  passed to `exception-chan`."
  ([qp5]
   (async-query-processor qp5 query-timeout-ms))

  ([qp5 timeout-ms]
   (fn qp*
     ([query]
      (qp* query default-rf))

     ([query rf]
      (let [result-chan    (a/promise-chan)
            exception-chan (a/promise-chan)
            cancel-chan    (a/promise-chan)]
        ;; If `result-chan` is closed before recieving a result, cancel the query
        (a/go
          (when-not (a/<! result-chan)
            (a/put! cancel-chan :cancel)
            (a/close! cancel-chan)
            (a/close! exception-chan)
            (a/close! result-chan)))
        ;; if `result-chan` doesn't get a result by `timeout-ms`, cancel the query and throw a timeout Exception
        (a/go
          (let [[_ port] (a/alts!! [result-chan (a/timeout timeout-ms)])]
            (when-not (= port result-chan)
              (a/put! cancel-chan :cancel)
              (a/put! exception-chan (ex-info (format "Timed out after %s." (u/format-milliseconds timeout-ms))
                                       {:status :timed-out}))
              (a/close! exception-chan)
              (a/close! cancel-chan)
              (a/close! result-chan))))
        (letfn [(async-qp2-respond [result]
                  (when (some? result)
                    (a/put! result-chan result))
                  (a/close! cancel-chan)
                  (a/close! exception-chan)
                  (a/close! result-chan))
                (async-qp2-raise [e]
                  (println "RAISED!" e)
                  (a/put! exception-chan e)
                  (a/close! exception-chan)
                  (a/close! cancel-chan)
                  (a/close! result-chan))]
          (try
            (qp5 query rf async-qp2-respond async-qp2-raise cancel-chan)
            (catch Throwable e
              (async-qp2-raise e))))
        {:result-chan result-chan, :exception-chan exception-chan})))))

(defn sync-query-processor
  "Returns a synchronous query processor function with the signatures

    (qp2 query) and (qp2 query rf)

  Calling this function synchronously returns successful query results (i.e., the reducing results using `rf` and
  transforms added by middleware) or throws an Exception if the query failed."
  ([qp5]
   (sync-query-processor qp5 query-timeout-ms))

  ([qp5 timeout-ms]
   (let [qp2 (async-query-processor qp5 timeout-ms)]
     (fn qp*
       ([query]
        (qp* query default-rf))

       ([query rf]
        (let [{:keys [result-chan exception-chan]} (qp2 query rf)
              [result port]                        (a/alts!! [result-chan exception-chan] :priority true)]
          (if (instance? Throwable result)
            (throw result)
            result)))))))
