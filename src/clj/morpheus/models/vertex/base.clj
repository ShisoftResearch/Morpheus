(ns morpheus.models.vertex.base
  (:require [morpheus.utils :refer :all]
            [neb.core :as neb]))

(def vertex-relation-fields
  [[:*inbounds*     [:ARRAY :relations]]
   [:*outbounds*    [:ARRAY :relations]]
   [:*neighbours*   [:ARRAY :relations]]])

(def vertex-relation-field-keys
  (map first vertex-relation-fields))

(defmulties
  :body
  (assemble-vertex [neb-cell])
  (new-vertex [data])
  (update-vertex [id func-sym params])
  (cell-fields [fields]))