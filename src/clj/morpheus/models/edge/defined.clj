(ns morpheus.models.edge.defined
  (:require [morpheus.utils :refer :all]
            [morpheus.models.edge.base :refer :all]))

(defmethods
  :defined ep
  (require-schema? [] true)
  (edge-schema
    [base-schema fields]
    (concat base-schema fields)))

