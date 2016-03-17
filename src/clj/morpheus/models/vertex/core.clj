(ns morpheus.models.vertex.core
  (:require [morpheus.utils :refer :all]
            [morpheus.models.base :refer [add-schema gen-schema-id]]
            [morpheus.models.vertex.defined]
            [morpheus.models.vertex.dynamic]
            [morpheus.models.vertex.base :as vb]
            [morpheus.models.core :as core]
            [neb.core :as neb]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [morpheus.models.base :as mb]))

(defn new-vertex-group [group-name group-props]
  (let [{:keys [fields]} group-props
        fields (vb/cell-fields group-props fields)]
    (core/add-schema :v group-name fields group-props)))

(defn veterx-group-props [group] (core/get-schema :v group))

(defmacro wrap-base-ops [op]
  ;TODO This can be better for performance by avoid using apply
  `(defn ~op [group# & args#]
     (let [props# (veterx-group-props group#)]
       (apply ~(symbol "morpheus.models.vertex.base" (name op)) props# args#))))

(wrap-base-ops reset-vertex)
(wrap-base-ops update-in-veterx)

(defn get-veterx-by-id [id]
  (let [neb-cell (neb/read-cell* id)
        neb-sid  (:*schema* neb-cell)
        morph-schema (mb/schema-by-neb-id neb-sid)]
    (assert (= :v (:stype morph-schema)) "This cell is not a veterx")
    (vb/assumble-vertex morph-schema neb-cell)))

(defn get-vertex-by-key [group key]
  (let [vp (veterx-group-props group)
        id (mb/cell-id-by-key :v vp key)]
    (get-veterx-by-id id)))

(defn new-vertex [group data]
  (vb/new-vertex (veterx-group-props group) data))

(defn update-vertex [vertex fn-sym & params]
  )