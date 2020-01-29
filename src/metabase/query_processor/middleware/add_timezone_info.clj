(ns metabase.query-processor.middleware.add-timezone-info
  (:require [metabase.query-processor.timezone :as qp.timezone]))

(defn- add-timezone-info-xform [xf]
  (fn
    ([]
     (xf))

    ([result]
     (xf result))

    ([result result-metadata]
     (xf result (merge
                 result-metadata
                 {:results_timezone (qp.timezone/results-timezone-id)}
                 (when-let [requested-timezone-id (qp.timezone/requested-timezone-id)]
                   {:requested_timezone requested-timezone-id}))))

    ([result result-metadata row]
     (xf result result-metadata row))))

(defn add-timezone-info
  "Add `:results_timezone` and `:requested_timezone` info to query results."
  [qp]
  (fn [query xform respond raise canceled-chan]
    (qp query (comp add-timezone-info-xform xform) respond raise canceled-chan)))
