(ns morpheus.models.vertex.dynamic
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]))

(defmethods
  true vp
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
    ))