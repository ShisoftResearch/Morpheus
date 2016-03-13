(ns morpheus.models.vertex.dynamic
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]))

(def dynamic-veterx-schema-fields
  [[:*data* :obj]])

(defmethods
  :dynamic vp
  (get-veterx
    [id]
    )
  (reset-veterx
    [id val]
    )
  (new-veterx
    [data]
    )
  (update-in-veterx
    [id fnc & params]
    )
  (cell-fields
    [fields]
    (concat
      vertex-relation-fields fields
      dynamic-veterx-schema-fields)))