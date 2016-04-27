(ns morpheus.tests.chained-list
  (:require [midje.sweet :refer :all]
            [morpheus.tests.server :refer [with-server]]
            [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]
            [morpheus.models.edge.base :as eb]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(facts
  "Chained Edge cid lists"
  (let [max-list-items eb/max-list-size]
    (with-server
      (fact "Create schemas"
            (new-vertex-group! :item {:body :defined :key-field :name
                                      :fields [[:name :text]]}) => anything
            (new-edge-group! :rel {:type :directed :body :simple}) => anything)
      (fact "Create Vertex"
            (new-vertex! :item {:name "v1"}) => anything
            (new-vertex! :item {:name "v2"}) => anything)
      (let [v1 (vertex-by-key :item "v1")
            v2 (vertex-by-key :item "v2")]
        (spy max-list-items)
        (fact "Create edges that can fit in one list"
              (apply link-group! v1 :rel (repeat max-list-items v2)))
        (fact "Check Degree - single"
              (degree (reload-vertex v1) :direction :*outbounds*) => max-list-items)
        (fact "Check list link - single"
              )
        (fact "Create edges that can fit in two list"
              (apply link-group! v1 :rel (repeat (* 2 max-list-items) v2)))
        (fact "Check Degree - duo"
              (degree (reload-vertex v1) :direction :*outbounds*) => (* 2 max-list-items))
        (fact "Check list link - duo"
              )))))
