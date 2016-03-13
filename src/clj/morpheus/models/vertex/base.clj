(ns morpheus.models.vertex.base
  (:require [morpheus.utils :refer :all]))

(def vertex-relation-fields
  [[:inbounds     [:ARRAY :relations]]
   [:outbounds    [:ARRAY :relations]]
   [:neighbours   [:ARRAY :relations]]])

(def dynamic-veterx-schema-fields
  [[:data :obj]])

(defmulties
  :dynamic-fields?
  (get-veterx [id])
  (reset-veterx [id val])
  (new-veterx [data])
  (update-in-veterx [id fnc & params]))