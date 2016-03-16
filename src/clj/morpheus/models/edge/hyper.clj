(ns morpheus.models.edge.hyper
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(def schema-fields
  [[:*start*  :cid-array]
   [:*end* :cid-array]])

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
    {:*start* v1
     :*end* v2})
  (edges-from-cid-array
    [cid-array]
    )
  (vertex-fields [] #{:*start* :*end*}))