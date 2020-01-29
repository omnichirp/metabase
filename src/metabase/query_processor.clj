(ns metabase.query-processor
  "Preprocessor that does simple transformations to all incoming queries, simplifing the driver-specific
  implementations."
  (:require [medley.core :as m]
            [metabase.driver :as driver]
            [metabase.driver.util :as driver.u]
            [metabase.mbql.schema :as mbql.s]
            [metabase.util.i18n :refer [tru]]
            [schema.core :as s]))

(defn preprocess [query])

#_(def ^:private ^{:arglists '([query])} preprocess
  "Run all the preprocessing steps on a query, returning it in the shape it looks immediately before it would normally
  get executed by `execute-query`. One important thing to note: if preprocessing fails for some reason, `preprocess`
  will throw an Exception, unlike `process-query`. Why? Preprocessing is something we use internally, so wrapping
  catching Exceptions and wrapping them in frontend results format doesn't make sense.

  (NOTE: Don't use this directly. You either want `query->preprocessed` (for the fully preprocessed query) or
  `query->native` for the native form.)"
  ;; throwing pre-allocated exceptions can actually get optimized away into long jumps by the JVM, let's give it a
  ;; chance to happen here
  (let [quit-early-exception (Exception.)
        ;; the 'pivoting' function is just one that delivers the query in its current state into the promise we
        ;; conveniently attached to the query. Then it quits early by throwing our pre-allocated Exception...
        deliver-native-query
        (fn [{:keys [results-promise] :as query}]
          (deliver results-promise (dissoc query :results-promise))
          (throw quit-early-exception))

        ;; ...which ends up getting caught by the `catch-exceptions` middleware. Add a final post-processing function
        ;; around that which will return whatever we delivered into the `:results-promise`.
        receive-native-query
        (fn [qp]
          (fn [query]
            (let [results-promise (promise)
                  results         (binding [async-wait/*disable-async-wait* true]
                                    (qp (assoc query :results-promise results-promise)))]
              (if (realized? results-promise)
                @results-promise
                ;; if the results promise was never delivered, it means we never made it all the way to the
                ;; `deliver-native-query` portion of the QP pipeline; the results will thus be a failure message from
                ;; our `catch-exceptions` middleware. In 99.9% of cases we probably want to know right away that the
                ;; query failed instead of giving people a failure response and trying to get results from that. So do
                ;; everyone a favor and throw an Exception
                (let [results (m/dissoc-in results [:query :results-promise])]
                  (throw (ex-info (tru "Error preprocessing query") results)))))))]
    (receive-native-query (build-pipeline deliver-native-query))))

(def ^:private ^:dynamic *preprocessing-level* 0)

(def ^:private ^:const max-preprocessing-level 20)

(defn query->preprocessed
  "Return the fully preprocessed form for `query`, the way it would look immediately before `mbql->native` is called.
  Especially helpful for debugging or testing driver QP implementations."
  {:style/indent 0}
  [query]
  ;; record the number of recursive preprocesses taking place to prevent infinite preprocessing loops.
  (binding [*preprocessing-level* (inc *preprocessing-level*)]
    (when (>= *preprocessing-level* max-preprocessing-level)
      (throw (ex-info (str (tru "Infinite loop detected: recursively preprocessed query {0} times."
                                max-preprocessing-level))
               {:type :bug})))
    (-> query        (update :middleware assoc :disable-mbql->native? true)
        (update :preprocessing-level (fnil inc 0))
        preprocess
        (m/dissoc-in [:middleware :disable-mbql->native?]))))

#_(defn query->expected-cols
  "Return the `:cols` you would normally see in MBQL query results by preprocessing the query amd calling `annotate` on
  it."
  [{query-type :type, :as query}]
  (when-not (= query-type :query)
    (throw (Exception. (tru "Can only determine expected columns for MBQL queries."))))
  (qp.store/with-store
    (let [preprocessed (query->preprocessed query)
          results      ((annotate/add-column-info (constantly nil)) preprocessed)]
      (seq (:cols results)))))

(defn query->expected-cols [_])

#_(defn query->native
  "Return the native form for `query` (e.g. for a MBQL query on Postgres this would return a map containing the compiled
  SQL form). (Like `preprocess`, this function will throw an Exception if preprocessing was not successful.)

  (Currently, this function is mostly used by tests and in the REPL; `mbql-to-native/mbql->native` middleware handles
  simliar functionality for queries that are actually executed.)"
  {:style/indent 0}
  [query]
  (perms/check-current-user-has-adhoc-native-query-perms query)
  (let [results (preprocess query)]
    (or (get results :native)
        (throw (ex-info (tru "No native form returned.")
                 (or results {}))))))

(defn query->native [query])

(defn query->native-with-spliced-params
  "Return the native form for a `query`, with any prepared statement (or equivalent) parameters spliced into the query
  itself as literals. This is used to power features such as 'Convert this Question to SQL'.

  (Currently, this function is mostly used by tests and in the REPL; `splice-params-in-response` middleware handles
  simliar functionality for queries that are actually executed.)"
  {:style/indent 0}
  [query]
  ;; We need to preprocess the query first to get a valid database in case we're dealing with a nested query whose DB
  ;; ID is the virtual DB identifier
  (let [driver (driver.u/database->driver (:database (query->preprocessed query)))]
    (driver/splice-parameters-into-native-query driver
      (query->native query))))

(defn process-query [query])


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                      Userland Queries (Public Interface)                                       |
;;; +----------------------------------------------------------------------------------------------------------------+

;; The difference between `process-query` and the versions below is that the ones below are meant to power various
;; things like API endpoints and pulses, while `process-query` is more of a low-level internal function.
;;
;; Many moons ago the two sets of functions had different QP pipelines; these days the functions below are simply
;; convenience wrappers for `process-query` that include a few options to activate appropriate middleware for userland
;; queries. This middleware does things like saving QueryExecutions and adding max results constraints.

(s/defn process-query-and-save-execution!
  "Process and run a 'userland' MBQL query (e.g. one ran as the result of an API call, scheduled Pulse, MetaBot query,
  etc.). Returns results in a format appropriate for consumption by FE client. Saves QueryExecution row in application
  DB."
  {:style/indent 1}
  [query, options :- mbql.s/Info]
  (process-query
    (-> query
        (update :info merge options)
        (assoc-in [:middleware :userland-query?] true))))

(s/defn process-query-and-save-with-max-results-constraints!
  "Same as `process-query-and-save-execution!` but will include the default max rows returned as a constraint. (This
  function is ulitmately what powers most API endpoints that run queries, including `POST /api/dataset`.)"
  {:style/indent 1}
  [query, options :- mbql.s/Info]
  (let [query (assoc-in query [:middleware :add-default-userland-constraints?] true)]
    (process-query-and-save-execution! query options)))
