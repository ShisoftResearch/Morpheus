(ns morpheus.models.edge.base
  (:require [morpheus.utils :refer :all]
            [morpheus.models.base :as mb]
            [neb.core :as neb]
            [neb.cell :as neb-cell]
            [neb.trunk-store :as neb-ts]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [morpheus.models.core :as core]))

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