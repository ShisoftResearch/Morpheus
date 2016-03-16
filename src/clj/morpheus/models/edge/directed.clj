(ns morpheus.models.edge.directed
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(def schema-fields
  [[:*start*  :cid]
   [:*end* :cid]])

(defmethods
  :directed ep
  (edge-base-schema [] schema-fields)
  (v1-vertex-field [] :*outbounds*)
  (v2-vertex-field [] :*inbounds*)
  (type-stick-body [] false)
  (edge-cell-vertex-fields
    [v1 v2]
    {:*start* v1
     :*end* v2})
  (vertex-fields [] #{:*start* :*end*}))