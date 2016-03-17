(ns morpheus.models.vertex.defined
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]
            [morpheus.models.base :as mb]
            [neb.core :as neb]
            [cluster-connector.utils.for-debug :refer [spy]]))

(defmethods
  :defined vp
  (assemble-vertex
    [neb-cell]
    (merge {:*vp* vp} neb-cell))
  (reset-vertex
    [id val]
    )
  (new-vertex
    [data]
    (let [{:keys [neb-sid]} vp]
      (neb/new-cell-by-ids
        (mb/cell-id-by-data :v vp data) neb-sid data)))
  (update-vertex
    [id func-sym params]
    (apply neb/update-cell* id func-sym params))
  (cell-fields [fields] (concat vertex-relation-fields fields)))