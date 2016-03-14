(ns morpheus.models.edge.directed
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]))

(def schema-fields
  [[:inbound  :cid]
   [:outbound :cid]])

(defmethods
  :directed ep
  (edge-base-schema [] schema-fields))