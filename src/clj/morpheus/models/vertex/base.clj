(ns morpheus.models.vertex.base
  (:require [morpheus.utils :refer :all]
            [neb.core :as neb]))

(def vertex-relation-fields
  [[:inbounds     [:ARRAY :relations]]
   [:outbounds    [:ARRAY :relations]]
   [:neighbours   [:ARRAY :relations]]])

(defn cell-id-by-key [vp key]
  (let [{:keys [neb-sid name]} vp]
    (neb/cell-id-by-key (str "v-" neb-sid "-" name "-" key))))

(defn cell-id-by-data [vp data]
  (let [{:keys [key-field]} vp]
    (if key-field
      (cell-id-by-key vp (get data key-field))
      (neb/rand-cell-id))))


(defmulties
  :body
  (assumble-veterx [neb-cell])
  (reset-veterx [id val])
  (new-veterx [data])
  (update-in-veterx [id fnc & params])
  (cell-fields [fields]))