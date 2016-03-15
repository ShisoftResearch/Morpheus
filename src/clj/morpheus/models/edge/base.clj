(ns morpheus.models.edge.base
  (:require [morpheus.utils :refer :all]
            [morpheus.models.base :as mb]
            [neb.core :as neb]))

(defmulties
  :type
  (neighbours [])
  (inboundds [])
  (outbounds [])
  (neighbours [relationship])
  (inboundds [relationship])
  (outbounds [relationship])
  (edge-base-schema [])
  (v1-vertex-field [])
  (v2-vertex-field [])
  (type-stick-body []))

(defmulties
  :body
  (get-edge [])
  (update-edge [new-edge])
  (delete-edge [])
  (base-schema [])
  (require-edge-cell? [])
  (edge-schema [base-schema fields])
  (create-edge-cell [v1 v2 & args]))

(defn record-edge-on-vertex [vertex edge-schema-id field cell-id]
  (let [cid-lists (get vertex field)
        cid-list-row (first (filter
                              (fn [item]
                                (= edge-schema-id (:sid item)))
                              cid-lists))
        list-cell-id (if cid-list-row
                       (:list-cid cid-list-row)
                       (neb/new-cell-by-ids (neb/rand-cell-id) @mb/cid-list-schema-id {:cid-array []}))]
    (neb/update-cell* list-cell-id 'clojure.core/conj cell-id)
    (if-not cid-list-row
      (update-in vertex field conj
                 {:sid edge-schema-id :list-cid list-cell-id})
      vertex)))