(ns metabase.query-processor-2
  (:require [metabase.driver :as driver]
            [metabase.query-processor.build :as qp.build]
            [metabase.query-processor.middleware
             [add-dimension-projections :as add-dim]
             [add-implicit-clauses :as implicit-clauses]
             [add-implicit-joins :as add-implicit-joins]
             [add-source-metadata :as add-source-metadata]
             [add-timezone-info :as add-timezone-info]
             [auto-bucket-datetimes :as bucket-datetime]
             [binning :as binning]
             [check-features :as check-features]
             [cumulative-aggregations :as cumulative-ags]
             [desugar :as desugar]
             [expand-macros :as expand-macros]
             [fetch-source-query :as fetch-source-query]
             [format-rows :as format-rows]
             [limit :as limit]
             [mbql-to-native :as mbql-to-native]
             [normalize-query :as normalize]
             [optimize-datetime-filters :as optimize-datetime-filters]
             [parameters :as parameters]
             [permissions :as perms]
             [pre-alias-aggregations :as pre-alias-ags]
             [reconcile-breakout-and-order-by-bucketing :as reconcile-bucketing]
             [resolve-database-and-driver :as resolve-database-and-driver]
             [resolve-fields :as resolve-fields]
             [resolve-joins :as resolve-joins]
             [resolve-source-table :as resolve-source-table]
             [splice-params-in-response :as splice-params-in-response]
             [store :as store]
             [validate :as validate]
             [wrap-value-literals :as wrap-value-literals]]))

(def default-middleware
  ;; ▼▼▼ POST-PROCESSING ▼▼▼  happens from TOP-TO-BOTTOM, e.g. the results of `f` are (eventually) passed to `limit`
  [#'mbql-to-native/mbql->native
   #_#'annotate/result-rows-maps->vectors
   #'check-features/check-features
   #'optimize-datetime-filters/optimize-datetime-filters
   #'wrap-value-literals/wrap-value-literals
   #_#'annotate/add-column-info
   #'perms/check-query-permissions
   #'pre-alias-ags/pre-alias-aggregations
   #'cumulative-ags/handle-cumulative-aggregations
   #'resolve-joins/resolve-joins
   #'add-implicit-joins/add-implicit-joins
   #'limit/limit
   #'format-rows/format-rows
   #'desugar/desugar
   #'binning/update-binning-strategy
   #'resolve-fields/resolve-fields
   #'add-dim/add-remapping
   #'implicit-clauses/add-implicit-clauses
   #'add-source-metadata/add-source-metadata-for-source-queries
   #'reconcile-bucketing/reconcile-breakout-and-order-by-bucketing
   #'bucket-datetime/auto-bucket-datetimes
   #'resolve-source-table/resolve-source-tables
   #'parameters/substitute-parameters
   #'expand-macros/expand-macros
   #'add-timezone-info/add-timezone-info
   #'splice-params-in-response/splice-params-in-response
   #'resolve-database-and-driver/resolve-database-and-driver
   #'fetch-source-query/resolve-card-id-source-tables
   #'store/initialize-store
   #'async-wait/wait-for-turn
   #_#'cache/maybe-return-cached-results ; TODO
   #'validate/validate-query
   #'normalize/normalize
   ;; TODO - don't need async-setup, but should add middleware to maintain a open query counter
   #_#'async/async-setup])

(def userland-middleware
  (concat
   default-middleware
   [#_#'results-metadata/record-and-return-metadata!
    #_#'row-count-and-status/add-row-count-and-status
    #_#'catch-exceptions/catch-exceptions
    #_#'process-userland-query/process-userland-query
    #_#'constraints/add-default-userland-constraints]))

(def ^{:arglists '([query] [query rf])} process-query-async
  (qp.build/async-query-processor
   (qp.build/build-query-processor default-middleware)))

(def ^{:arglists '([query] [query rf])} process-query
  (qp.build/sync-query-processor
   (qp.build/build-query-processor default-middleware)))

(require 'metabase.test)

;; NOCOMMIT
(defn x []
  (process-query {:database (driver/with-driver :postgres (metabase.test/id))
                  :type     :native
                  :native   {:query "SELECT * FROM users LIMIT 5;"}}))

(defn y []
  (process-query {:database (driver/with-driver :postgres (metabase.test/id))
                  :type     :query
                  :query    {:source-table (driver/with-driver :postgres (metabase.test/id :venues))
                             :aggregation  [[:cum-count]]
                             :breakout     [[:field-id (driver/with-driver :postgres (metabase.test/id :venues :price))]]
                             :filter       [:!= [:field-id (driver/with-driver :postgres (metabase.test/id :venues :name))] "Mohawk Bend"]
                             :limit        5}}))
