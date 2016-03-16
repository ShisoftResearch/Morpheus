(ns morpheus.models.edge.base
  (:require [morpheus.utils :refer :all]
            [morpheus.models.base :as mb]
            [neb.core :as neb]
            [cluster-connector.utils.for-debug :refer [spy $]]))

(defmulties
  :type
  (edge-base-schema [])
  (v1-vertex-field [])
  (v2-vertex-field [])
  (type-stick-body [])
  (edge-cell-vertex-fields [v1 v2])
  (vertex-fields []))

(defmulties
  :body
  (get-edge [])
  (update-edge [new-edge])
  (delete-edge [])
  (base-schema [])
  (require-edge-cell? [])
  (edge-schema [base-schema fields])
  (create-edge-cell [vertex-fields & args])
  (edges-from-cid-array [cid-list & [start-vertex]]))

(defn conj-into-list-cell [list-cell cell-id]
  (update list-cell :cid-array conj cell-id))

(defn record-edge-on-vertex [vertex edge-schema-id field cell-id & ]
  (let [cid-lists (get vertex field)
        cid-list-row (first (filter
                              (fn [item]
                                (= edge-schema-id (:sid item)))
                              cid-lists))
        list-cell-id (if cid-list-row
                       (:list-cid cid-list-row)
                       (neb/new-cell-by-ids (neb/rand-cell-id) @mb/cid-list-schema-id {:cid-array []}))]
    (neb/update-cell* list-cell-id 'morpheus.models.edge.base/conj-into-list-cell cell-id)
    (if-not cid-list-row
      (update vertex field conj {:sid edge-schema-id :list-cid list-cell-id})
      vertex)))