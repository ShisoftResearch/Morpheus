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

(defn get-veterx-by-id [id]
  (let [neb-cell (neb/read-cell* id)
        neb-sid  (:*schema* neb-cell)
        morph-schema (mb/schema-by-neb-id neb-sid)]
    (assert (= :v (:stype morph-schema)) "This cell is not a veterx")
    (vb/assemble-vertex morph-schema neb-cell)))

(defn get-vertex-by-key [group key]
  (let [vp (veterx-group-props group)
        id (mb/cell-id-by-key :v vp key)]
    (get-veterx-by-id id)))

(defn new-vertex [group data]
  (vb/new-vertex (veterx-group-props group) data))

(defn update-vertex-by-vp [vp id fn-sym & params]
  (vb/update-vertex vp id fn-sym params))

(defn update-vertex [vertex fn-sym & params]
  (apply update-vertex-by-vp
         (:*vp* vertex) (:*id* vertex) fn-sym params))

(defn- reset-vertex-cell-map [vertex value]
  (merge value
         (select-keys vertex vb/vertex-relation-field-keys)))

(defn reset-vertex-by-vp [vp id value]
  (update-vertex-by-vp vp id 'morpheus.models.vertex.core/reset-vertex-cell-map value))

(defn reset-vertex [vertex value]
  (reset-vertex-by-vp (:*vp* vertex) (:*id* vertex) value))