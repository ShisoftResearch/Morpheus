(ns morpheus.models.vertex.dynamic
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]
            [morpheus.models.base :as mb]
            [neb.core :as neb]
            [neb.cell :as neb-cell]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [morpheus.models.dynamic :as md]))

(def dynamic-veterx-schema-fields
  [[:*data* :obj]])

(defn update-vertex* [neb-cell func-sym params]
  (md/update-dynamic-cell :*vp* neb-cell func-sym params))

(defmethods
  :dynamic vp
  (assemble-vertex
    [neb-cell]
    (assoc (md/assemble-dynamic-outcome neb-cell) :*vp* vp))
  (new-vertex
    [data]
    (let [{:keys [neb-sid]} vp]
      (neb/new-cell-by-ids
        (mb/cell-id-by-data :v vp data) neb-sid
        (md/preproc-for-dynamic-write neb-sid data))))
  (update-vertex
    [id func-sym params]
    (assemble-vertex
      vp
      (neb/update-cell*
        id 'morpheus.models.vertex.dynamic/update-vertex*
        func-sym params)))
  (cell-fields
    [fields]
    (concat
      vertex-relation-fields fields
      dynamic-veterx-schema-fields)))