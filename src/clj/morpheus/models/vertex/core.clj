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

(defn new-vertex-group! [group-name group-props]
  (let [{:keys [fields]} group-props
        fields (vb/cell-fields group-props fields)]
    (core/add-model-schema :v group-name fields group-props)))

(def veterx-group-props vb/veterx-group-props)

(defn get-veterx-by-id [id]
  (when-let [neb-cell (neb/read-cell* id)]
    (let [neb-sid  (:*schema* neb-cell)
          morph-schema (mb/schema-by-neb-id neb-sid)]
      (assert (= :v (:stype morph-schema)) "This cell is not a veterx")
      (vb/assemble-vertex morph-schema neb-cell))))

(defn vertex-id-by-key [group key]
  (let [vp (vb/veterx-group-props group)]
    (mb/cell-id-by-key :v vp key)))

(defn vertex-by-key [group key]
  (get-veterx-by-id (vertex-id-by-key group key)))

(defn reload-vertex [vertex]
  (get-veterx-by-id (:*id* vertex)))

(defn new-vertex! [group data]
  (vb/new-vertex (vb/veterx-group-props group) data))

(defn update-vertex-by-vp [vp id fn-sym & params]
  (vb/update-vertex vp id fn-sym params))

(defn update-vertex! [vertex fn-sym & params]
  (apply update-vertex-by-vp
         (:*vp* vertex) (:*id* vertex) fn-sym params))

(defn reset-vertex-by-vp [vp id value]
  (update-vertex-by-vp vp id 'morpheus.models.vertex.base/reset-vertex-cell-map value))

(defn reset-vertex! [vertex value]
  (reset-vertex-by-vp (:*vp* vertex) (:*id* vertex) value))

(defn delete-vertex-by-id [id]
  (neb/write-lock-exec* id 'morpheus.models.vertex.base/delete-vertex*))

(defn delete-vertex! [vertex]
  (delete-vertex-by-id (:*id* vertex)))

(defn digest-vertex [id]
  (when (neb/cell-exists?* id)
    {:*id* id}))

(defn digest-vertex-by-key [group key]
  (digest-vertex (vertex-id-by-key group key)))