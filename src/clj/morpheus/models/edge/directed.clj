(ns morpheus.models.edge.directed
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]
            [neb.core :as neb]))

(defmethods
  :directed ep
  (edge-base-schema [] schema-fields)
  (v1-vertex-field [] :*outbounds*)
  (v2-vertex-field [] :*inbounds*)
  (type-stick-body [] false)
  (delete-edge-cell [edge start end] (neb/delete-cell* (:*id* edge))))