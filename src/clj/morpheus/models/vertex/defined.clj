(ns morpheus.models.vertex.defined
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]
            [morpheus.models.base :as mb]
            [morpheus.models.defined :as md]
            [neb.core :as neb]
            [cluster-connector.utils.for-debug :refer [spy]]))

(defn update-vertex* [neb-cell func-sym & params]
  (md/update-modeled-cell :*vp* neb-cell func-sym params))

(defmethods
  :defined vp
  (assemble-vertex
    [neb-cell]
    (merge {:*vp* vp} neb-cell))
  (new-vertex
    [data]
    (let [{:keys [neb-sid]} vp]
      (neb/new-cell-by-ids
        (mb/cell-id-by-data :v vp data) neb-sid data)))
  (update-vertex
    [id func-sym params]
    (assemble-vertex vp
      (apply neb/update-cell* id
             'morpheus.models.vertex.defined/update-vertex*
             func-sym params)))
  (cell-fields [fields] (concat vertex-relation-fields fields)))