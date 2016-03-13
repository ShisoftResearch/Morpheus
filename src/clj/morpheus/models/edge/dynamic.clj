(ns morpheus.models.edge.dynamic
  (:require [morpheus.utils :refer :all]
            [morpheus.models.edge.base :refer :all]))

(def dynamic-edge-schema-fields
  [[:*data* :obj]])

(defmethods
  :dynamic
  (require-schema? [] true)
  (edge-schema
    [base-schema fields]
    (concat base-schema fields dynamic-edge-schema-fields)))