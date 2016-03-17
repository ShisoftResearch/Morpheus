(ns morpheus.models.edge.hyper
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(defmethods
  :hyper ep
  (require-edge-cell? [] true)
  (edge-schema
    [base-schema fields]
    (concat base-schema fields))
  (v1-vertex-field [] :*inbounds*)
  (v2-vertex-field [] :*outbounds*)
  (type-stick-body [] true)
  (edges-from-cid-array
    [cid-array]
    ))