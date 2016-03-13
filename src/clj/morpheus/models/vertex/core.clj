(ns morpheus.models.vertex.core
  (:require [morpheus.utils :refer :all]
            [morpheus.models.base :refer [add-schema gen-id]]
            [morpheus.models.vertex.defined]
            [morpheus.models.vertex.dynamic]
            [morpheus.models.vertex.base :as vb]
            [morpheus.models.core :as core]
            [neb.core :as neb]))

(defn new-vertex-group [group-name group-props]
  (let [{:keys [fields dynamic-fields?]} group-props
        fields (if dynamic-fields? vb/dynamic-veterx-schema-fields (or fields []))
        fields (concat vb/vertex-relation-fields fields)]
    (core/add-schema :v group-name fields group-props)))

(defn fetch-group-props [group] (get @schemas group))

(defmacro wrap-base-ops [op]
  ;TODO This can be better for performance by avoid using apply
  `(defn ~op [group# & args#]
     (let [props# (fetch-group-props group#)]
       (apply ~(symbol "morpheus.models.vertex.base" (name op)) props# args#))))

(wrap-base-ops get-veterx)
(wrap-base-ops reset-veterx)
(wrap-base-ops new-veterx)
(wrap-base-ops update-in-veterx)