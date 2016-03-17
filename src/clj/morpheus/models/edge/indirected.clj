(ns morpheus.models.edge.indirected
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(defmethods
  :indirected ep
  (edge-base-schema [] schema-fields)
  (v1-vertex-field [] :*neighbours*)
  (v2-vertex-field [] :*neighbours*)
  (type-stick-body [] false))