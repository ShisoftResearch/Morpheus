(ns morpheus.models.edge.directed
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(defmethods
  :directed ep
  (edge-base-schema [] schema-fields)
  (v1-vertex-field [] :*outbounds*)
  (v2-vertex-field [] :*inbounds*)
  (type-stick-body [] false))