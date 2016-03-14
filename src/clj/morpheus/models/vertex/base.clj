(ns morpheus.models.vertex.base
  (:require [morpheus.utils :refer :all]))

(def vertex-relation-fields
  [[:inbounds     [:ARRAY :relations]]
   [:outbounds    [:ARRAY :relations]]
   [:neighbours   [:ARRAY :relations]]])

(defmulties
  :body
  (get-veterx [id])
  (reset-veterx [id val])
  (new-veterx [data])
  (update-in-veterx [id fnc & params])
  (cell-fields [fields]))