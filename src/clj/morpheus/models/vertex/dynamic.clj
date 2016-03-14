(ns morpheus.models.vertex.dynamic
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]
            [neb.core :as neb]))

(def dynamic-veterx-schema-fields
  [[:*data* :obj]])

(defmethods
  :dynamic vp
  (assumble-veterx
    [neb-cell]
    (merge {:*vp* vp} (dissoc neb-cell :*data*) (:*data* neb-cell)))
  (reset-veterx
    [id val]
    )
  (new-veterx
    [data]
    (let [{:keys [neb-sid]} vp
          defined-fields (map first (:f (neb/get-schema-by-id neb-sid)))
          defined-map (select-keys data defined-fields)
          dynamic-map (apply dissoc data defined-fields)]
      (neb/new-cell-by-ids
        (cell-id-by-data vp data) neb-sid
        (assoc defined-map :*data* dynamic-map))))
  (update-in-veterx
    [id fnc & params]
    )
  (cell-fields
    [fields]
    (concat
      vertex-relation-fields fields
      dynamic-veterx-schema-fields)))