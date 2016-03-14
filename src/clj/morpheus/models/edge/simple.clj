(ns morpheus.models.edge.simple
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(defmethods
  :simple ep
  (require-schema? [] false)
  (edge-base-schema [] nil)
  (edge-schema [base-schema fields] nil))