(ns morpheus.models.vertex.defined
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]))

(defmethods
  false vp
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