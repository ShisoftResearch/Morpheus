(ns morpheus.models.edge.directed
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(def schema-fields
  [[:*inbound*  :cid]
   [:*outbound* :cid]])

(defmethods
  :directed ep
  (edge-base-schema [] schema-fields)
  (v1-vertex-field [] :*inbounds*)
  (v2-vertex-field [] :*outbounds*)
  (type-stick-body [] false)
  (edge-cell-vertex-fields
    [v1 v2]
    {:*inbound* v1
     :*outbound* v2}))