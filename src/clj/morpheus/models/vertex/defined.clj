(ns morpheus.models.vertex.defined
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]))

(defmethods
  :defined vp
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
  (cell-fields [fields] (concat vertex-relation-fields fields)))