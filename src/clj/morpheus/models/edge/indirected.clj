(ns morpheus.models.edge.indirected
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]
            [neb.core :as neb]))

(defmethods
  :indirected ep
  (edge-base-schema [] schema-fields)
  (v1-vertex-field [] :*neighbours*)
  (v2-vertex-field [] :*neighbours*)
  (type-stick-body [] false)
  (delete-edge-cell [edge start end] (neb/delete-cell* (:*id* edge))))