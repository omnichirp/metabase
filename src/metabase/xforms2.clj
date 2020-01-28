(ns metabase.xforms2
  (:require [clojure.core.async :as a]
            [metabase
             [sql-jdbc-xforms :as sql-jdbc-xforms]
             [util :as u]]))

(set! *warn-on-reflection* true)

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                              Sample Middleware/QP                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- middleware-1 [qp]
  (fn [query xform respond raise cancel]
    (println "IN MIDDLEWARE 1!")
    (qp query xform respond raise cancel)))

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
  (fn [query xform respond raise cancel]
    (let [xform' (comp xform add-a-column-xform)]
      (qp query xform' respond raise cancel))))

(defn- async-middleware
  "Adds some async sleeping."
  [qp]
  (fn [query xform respond raise cancel]
    (let [futur   (atom nil)
          cancel' (fn []
                    (some-> @futur future-cancel)
                    (cancel))]
      (reset! futur (future
                      (println "Sleep 50.")
                      (Thread/sleep 50)
                      (println "Done sleeping.")
                      (try
                        (qp query xform respond raise cancel')
                        (catch Throwable e
                          (raise e)))))
      nil)))

(defn- async-cancel-middleware
  "Runs query on a separate thread and cancels it, returning `:canceled` response when query is canceled."
  [qp]
  (fn [query xform respond raise cancel]
    (let [futur   (atom nil)
          cancel' (fn []
                    (some-> @futur future-cancel)
                    (respond {:status :canceled})
                    (cancel))]
      (reset! futur (future
                      (try
                        (qp query xform respond raise cancel')
                        (catch Throwable e
                          (raise e)))))
      nil)))

(defn- execute* [rf]
  (fn execute*-qp5 [query xform respond raise cancel]
    (letfn [(respond' [result]
              (println "build-qp4 respond got result" (class result))
              (respond
               (try
                 (println "[REDUCING]")
                 (transduce xform rf (rf) result)
                 (finally
                   (println "[REDUCED]")))))]
      (try
        (sql-jdbc-xforms/execute-query query respond' raise cancel)
        (catch Throwable e
          (raise e))))))

(defn- build-qp4
  "Returns a QP fn with the signature `(qp-fn query respond raise cancel)`."
  [rf middleware]
  (u/profile "build query processor"
    (let [qp (reduce
              (fn [qp middleware]
                (middleware qp))
              (execute* rf)
              middleware)]
      (fn qp4* [query respond raise cancel]
        (try
          (qp query identity respond raise cancel)
          (catch Throwable e
            (raise e)))))))

(defn- async-qp1 [qp4 timeout-ms]
  (fn [query]
    (let [result-chan (a/promise-chan)
          cancel-chan (a/promise-chan)
          cancel      (constantly nil)]
      (a/go
        (when (a/<! cancel-chan)
          (println "async-qp1 got cancel message (canceling query)")
          (cancel)
          (a/put! result-chan {:status :canceled})
          (a/close! result-chan)
          (a/close! cancel-chan)))
      (a/go
        (let [[val port] (a/alts!! [result-chan (a/timeout timeout-ms)])]
          (println "async-qp1 got result" (class val) "from port" (if (= port result-chan) "result-chan" "timeout chan"))
          (when-not (= port result-chan)
            (a/put! cancel-chan :cancel)
            (a/put! result-chan {:status  :timed-out
                                 :message (format "Timed out after %s." (u/format-milliseconds timeout-ms))})
            (a/close! cancel-chan)
            (a/close! result-chan))))
      (qp4
       query
       (fn async-qp1-respond [result]
         (println "async-qp1-respond got result" (class result))
         (a/put! result-chan result)
         (a/close! result-chan)
         (a/close! cancel-chan))
       (fn async-qp1-raise [e]
         (println "async-qp1-raise got exception" (class e))
         (a/put! result-chan e)
         (a/close! result-chan)
         (a/close! cancel-chan))
       cancel)
      {:result-chan result-chan
       :cancel-chan cancel-chan})))

(defn- sync-qp1 [qp4 timeout-ms]
  (let [qp1 (async-qp1 qp4 timeout-ms)]
    (fn [query]
      (let [{:keys [result-chan]} (qp1 query)
            result                (a/<!! result-chan)]
        (if (instance? Throwable result)
          (throw result)
          result)))))

;;; ------------------------------------------------- Default Impls --------------------------------------------------

(defn default-rf
  ([] {:data {:rows []}})

  ([results] results)

  ([results results-meta]
   (update results :data merge results-meta))

  ([results _ row]
   (update-in results [:data :rows] conj row)))

(def default-middleware
  [add-a-column-middleware async-middleware middleware-1 async-cancel-middleware])

(def ^{:arglists '([query respond raise cancel])} default-qp4
  (build-qp4 default-rf default-middleware))

;; ;; TODO - or should this be some sort of middleware? For async situations
;; (defn userland-exception-middleware [qp]
;;   (fn [query xform {:keys [out-chan], :as chans}]
;;     (letfn [(exception-response [^Throwable e]
;;               (merge
;;                {:message    (.getMessage e)
;;                 :stacktrace (u/filtered-stacktrace e)}
;;                (when-let [cause (.getCause e)]
;;                  {:cause (exception-response cause)})))]
;;       (let [new-out-chan (a/promise-chan)]
;;         ;; close `new-out-chan` if `out-chan` gets closed
;;         (a/go
;;           (a/<! out-chan)
;;           (a/close! new-out-chan))
;;         (a/go
;;           (when-let [result (a/<! new-out-chan)]
;;             (println "result:" result)  ; NOCOMMIT
;;             (if (instance? Throwable result)
;;               (a/>! out-chan (exception-response result))
;;               (a/>! out-chan result))
;;             (a/close! new-out-chan)
;;             (a/close! out-chan)))
;;         (qp query xform (assoc chans :out-chan new-out-chan))))))

;; #_(defn process-userland-query [query]
;;   (letfn [(exception-response [^Throwable e]
;;             (merge
;;              {:message    (.getMessage e)
;;               :stacktrace (u/filtered-stacktrace e)}
;;              (when-let [cause (.getCause e)]
;;                {:cause (exception-response cause)})))]
;;     (try
;;       (process-query query)
;;       (catch Throwable e
;;         (assoc (exception-response e)
;;                :status :failed)))))

;; (defn process-userland-query [query]
;;   (process-query query identity default-rf (cons userland-exception-middleware default-middleware)))


;; ;;; +----------------------------------------------------------------------------------------------------------------+
;; ;;; |                                             Sample RFs / Test Fns                                              |
;; ;;; +----------------------------------------------------------------------------------------------------------------+

(defn- print-rows-rf
  ([] 0)

  ([row-count] row-count)

  ([row-count results-meta]
   (println "results meta ->\n" (u/pprint-to-str 'blue results-meta))
   row-count)

  ([row-count _ row]
   (println (u/format-color 'yellow "ROW %d ->" (inc row-count)) (pr-str row))
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

(def ^{:arglists '([query])} process-query-async
  (async-qp1 default-qp4 5000))

(def ^{:arglists '([query])} process-query
  (sync-qp1 default-qp4 5000))

(defn- default-example []
  (process-query "SELECT * FROM users ORDER BY id ASC LIMIT 5;"))

(defn- print-rows-example []
  (let [qp (sync-qp1 (build-qp4 print-rows-rf default-middleware) 5000)]
    (qp "SELECT * FROM users ORDER BY id ASC LIMIT 5;")))

(defn- print-rows-to-file-example []
  (with-open [w (clojure.java.io/writer "/Users/cam/Desktop/test.txt")]
    (let [rf (print-rows-to-writer-rf w)
          qp (sync-qp1 (build-qp4 rf default-middleware) 5000)]
      (qp "SELECT * FROM users ORDER BY id ASC LIMIT 5;"))))

(defn- maps-example []
  (let [qp (sync-qp1 (build-qp4 maps-rf default-middleware) 5000)]
    (qp "SELECT * FROM users ORDER BY id ASC LIMIT 5;")))

(defn- cancel-example []
  (let [{:keys [cancel-chan out-chan]} (process-query-async "SELECT * FROM users ORDER BY id ASC LIMIT 5;" maps-rf)]
    (a/put! cancel-chan :cancel)
    (a/<!! out-chan)))

(defn- exception-example []
  (process-query "SELECT asdasdasd;"))

#_(defn- userland-exception-example []
  (process-userland-query "SELECT asdasdasd;"))
