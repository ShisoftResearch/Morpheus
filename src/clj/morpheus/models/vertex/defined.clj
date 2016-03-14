(ns morpheus.models.vertex.defined
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]
            [morpheus.models.base :as mb]
            [neb.core :as neb]))

(defmethods
  :defined vp
  (assumble-veterx
    [neb-cell]
    (merge {:*vp* vp} neb-cell))
  (reset-veterx
    [id val]
    )
  (new-veterx
    [data]
    (let [{:keys [neb-sid]} vp]
      (neb/new-cell-by-ids
        (cell-id-by-data vp data) neb-sid data)))
  (update-in-veterx
    [id fnc & params]
    )
  (cell-fields [fields] (concat vertex-relation-fields fields)))