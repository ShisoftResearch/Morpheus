(ns morpheus.models.edge.hyper
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(def schema-fields
  [[:*inbound*  :cid-array]
   [:*outbound* :cid-array]])

(defmethods
  :hyper ep
  (require-edge-cell? [] true)
  (edge-base-schema [] schema-fields)
  (edge-schema
    [base-schema fields]
    (concat base-schema fields))
  (v1-vertex-field [] :*inbounds*)
  (v2-vertex-field [] :*outbounds*)
  (type-stick-body [] true)
  (edge-cell-vertex-fields
    [v1 v2]
    {:*inbound* v1
     :*outbound* v2}))