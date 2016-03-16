(ns morpheus.models.edge.defined
  (:require [morpheus.utils :refer :all]
            [morpheus.models.edge.base :refer :all]
            [neb.core :as neb]
            [morpheus.models.base :as mb]))

(defmethods
  :defined ep
  (require-edge-cell? [] true)
  (edge-schema
    [base-schema fields]
    (concat base-schema fields))
  (create-edge-cell
    [vertex-fields & [data]]
    (let [{:keys [neb-sid]} ep]
      (neb/new-cell-by-ids
        (mb/cell-id-by-data :e ep data) neb-sid
        (merge data vertex-fields))))
  (edges-from-cid-array
    [{:keys [cid-array] :as cid-list} & _]
    (map neb/read-cell* cid-array)))

