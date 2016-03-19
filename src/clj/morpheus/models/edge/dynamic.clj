(ns morpheus.models.edge.dynamic
  (:require [morpheus.utils :refer :all]
            [morpheus.models.edge.base :refer :all]
            [neb.core :as neb]
            [morpheus.models.base :as mb]
            [morpheus.models.dynamic :as md]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(def dynamic-edge-schema-fields
  [[:*data* :obj]])

(defn update-edge* [neb-cell func-sym params]
  (md/update-dynamic-cell :*ep* neb-cell func-sym params))

(defmethods
  :dynamic ep
  (require-edge-cell? [] true)
  (edge-schema
    [base-schema fields]
    (concat base-schema fields dynamic-edge-schema-fields))
  (create-edge-cell
    [vertex-fields & [data]]
    (let [{:keys [neb-sid]} ep
          defined-fields (map first (:f (neb/get-schema-by-id neb-sid)))
          defined-map (select-keys (merge data vertex-fields) defined-fields)
          dynamic-map (apply dissoc data defined-fields)]
      (neb/new-cell-by-ids
        (mb/cell-id-by-data :e ep data) neb-sid
        (assoc defined-map :*data* dynamic-map))))
  (edges-from-cid-array
    [{:keys [cid-array] :as cid-list} & _]
    (map (comp md/assemble-dynamic-outcome neb/read-cell*) cid-array))
  (update-edge
    [id func-sym params]
    (format-edge-cells
      ep nil
      (neb/update-cell*
        id 'morpheus.models.edge.dynamic/update-edge*
        func-sym params))))