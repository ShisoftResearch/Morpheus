(ns morpheus.models.edge.indirected
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(def schema-fields
  [[:edge1  :cid]
   [:edge2  :cid]])

(defmethods
  :indirected ep
  (edge-base-schema [] schema-fields)
  (v1-vertex-field [] :neighbours)
  (v2-vertex-field [] :neighbours))