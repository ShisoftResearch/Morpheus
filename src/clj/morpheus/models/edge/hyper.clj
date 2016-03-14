(ns morpheus.models.edge.hyper
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(def schema-fields
  [[:inbound  :cid-array]
   [:outbound :cid-array]])

(defmethods
  :hyper ep
  (require-schema? [] true)
  (edge-base-schema [] schema-fields)
  (edge-schema
    [base-schema fields]
    (concat base-schema fields)))