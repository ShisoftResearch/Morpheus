(ns morpheus.models.edge.base
  (:require [morpheus.utils :refer :all]
            [morpheus.models.base :as mb]
            [neb.core :as neb]
            [neb.cell :as neb-cell]
            [neb.trunk-store :as neb-ts]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [cluster-connector.native-cache.core :refer [defcache evict-cache-key]]
            [morpheus.models.core :as core]
            [com.climate.claypoole :as cp])
  (:import (java.util UUID)
           (org.shisoft.neb Trunk)
           (org.shisoft.neb.io type_lengths)))

(def schema-fields
  [[:*start*  :cid]
   [:*end*  :cid]])

(defmulties
  :type
  (edge-base-schema [])
  (v1-vertex-field [])
  (v2-vertex-field [])
  (type-stick-body [])
  (vertex-fields [])
  (delete-edge-cell [edge start end]))

(defmulties
  :body
  (update-edge [id func-sym params])
  (delete-edge [])
  (base-schema [])
  (require-edge-cell? [])
  (edge-schema [base-schema fields])
  (create-edge-cell [vertex-fields & args])
  (edges-from-cid-array [cid-list & [start-vertex]]))

(defn edge-cell-vertex-fields [v1 v2]
  {:*start* v1
   :*end* v2})

(def vertex-fields #{:*start* :*end*})

(def edge-list-max (dec (/ (Trunk/getMaxObjSize) type_lengths/cidLen)))

(defn append-edge-to-linked-lists [id]
  (let [cid-to-check (atom id)]
    ))

(defn conj-into-list-cell [list-cell cell-id]
  (update list-cell :cid-array conj cell-id))

(defn concat-into-list-cell [list-cell cell-ids]
  (update list-cell :cid-array concat cell-ids))

(defn extract-edge-cid-list [cid-lists edge-schema-id]
  (first (filter
           (fn [item]
             (= edge-schema-id (:sid item)))
           cid-lists)))

(defn extract-cell-list-id [vertex field edge-schema-id]
  (let [cid-lists (get vertex field)]
    (:list-cid (extract-edge-cid-list cid-lists edge-schema-id))))

(defn cid-list-id-by-vertex [v-id field edge-schema-id]
  (neb/cell-id-by-key (str v-id "-" field "-" edge-schema-id)))

(defn record-edge-on-vertex [vertex edge-schema-id field & ]
  (let [cid-list-row-id (extract-cell-list-id vertex field edge-schema-id)
        list-cell-id (or cid-list-row-id
                         (neb/new-cell-by-ids
                           (cid-list-id-by-vertex (:*id* vertex) field edge-schema-id)
                           @mb/cid-list-schema-id
                           {:next-list (UUID. 0 0) :cid-array []}))]
    (if-not cid-list-row-id
      (update vertex field conj {:sid edge-schema-id :list-cid list-cell-id})
      vertex)))

(defn vertex-edge-list [[vertex-id direction schema-id]]
  (let [id (cid-list-id-by-vertex vertex-id direction schema-id)]
    (when-not (neb/cell-exists?* id)
      (neb/update-cell* vertex-id 'morpheus.models.edge.base/record-edge-on-vertex
                        schema-id direction))
    id))

(defn rm-ve-list-item* [{:keys [cid-array] :as list-cell} target-cid]
  ;(assert ((set cid-array) target-cid) "target does not in the list")
  (update list-cell :cid-array #(remove-first (fn [x] (= target-cid x)) %)))

(defn rm-ve-list-item [list-cell target-cid]  (let [{:keys [cid-array] :as proced-cell} (rm-ve-list-item* list-cell target-cid)]
    (if (empty? cid-array)
      (do (mb/try-invoke-local-neb-cell
            neb-cell/delete-cell neb-ts/delete-cell list-cell) true)
      (do (mb/try-invoke-local-neb-cell
            neb-cell/replace-cell* neb-ts/replace-cell list-cell proced-cell) false))))

(defn rm-ve-relation [vertex direction es-id target-cid]
  (let [cid-list-cell-id (->> (get vertex direction)
                              (filter (fn [m] (= es-id (:sid m))))
                              (first) (:list-cid))
        empty-relation? (when cid-list-cell-id
                          (neb/write-lock-exec*
                            cid-list-cell-id
                            'morpheus.models.edge.base/rm-ve-list-item
                            target-cid))]
    (if empty-relation?
      (update vertex direction
              (fn [coll] (remove #(= cid-list-cell-id (:list-cid %)) coll)))
      vertex)))

(defn format-edge-cells [group-props direction edge]
  (let [pure-edge (dissoc edge :*schema* :*hash*)]
    (merge pure-edge
           {:*ep* group-props
            :*direction* direction})))