(ns morpheus.models.edge.dynamic
  (:require [morpheus.utils :refer :all]
            [morpheus.models.edge.base :refer :all]
            [neb.core :as neb]
            [morpheus.models.base :as mb]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(def dynamic-edge-schema-fields
  [[:*data* :obj]])

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
    (map
      (fn [edge-cid]
        (let [{:keys [*data*] :as edge} (neb/read-cell* edge-cid)]
          (merge (dissoc edge :*data*)
                 *data*)))
      cid-array)))