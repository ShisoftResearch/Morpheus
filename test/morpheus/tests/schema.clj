(ns morpheus.tests.schema
  (:require [midje.sweet :refer :all]
            [morpheus.tests.server :refer [with-server]]
            [morpheus.models.core :refer :all]
            [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]
            [morpheus.core :refer [start-server shutdown-server]]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(facts
  "Schemas"
  (with-server
    (fact "Add Dynamic Veterx Schema"
          (new-vertex-group :test-dynamic-veterx {:type :dynamic}) => anything)
    (fact "Get Dynamic Veterx Schema"
          (veterx-group-props :test-dynamic-veterx) => (contains {:stype :v, :name :test-dynamic-veterx}))
    (fact "Add Defined Veterx Schema"
          (new-vertex-group :test-defined-veterx {:type :defined :fields [[:id :int]]}) => anything)
    (fact "Get Defined Veterx Schema"
          (veterx-group-props :test-defined-veterx) => (contains {:stype :v, :name :test-defined-veterx}))
    (fact "Add Dynamic Edge Schema"
          (new-edge-group :text-e-dy {:type :directed :body :dynamic}) => anything)
    (fact "Get Dynamic Edge Schema"
          (edge-group-props :text-e-dy) => (contains {:stype :e :type :directed :body :dynamic :name :text-e-dy}))))
