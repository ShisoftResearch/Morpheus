(ns morpheus.tests.modeling
  (:require [midje.sweet :refer :all]
            [morpheus.tests.server :refer [with-server]]
            [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(facts
  "Creating Veterx and Edges"
  (with-server
    (fact "Create Veterx Schema"
          (new-vertex-group :actor {:body :dynamic :key-field :name}) => anything
          (new-vertex-group :movie {:body :dynamic :key-field :name}) => anything)
    (fact "Create Edge Schema"
          (new-edge-group :acted   {:type :directed :body :dynamic}) => anything)
    (fact "New Veterxs"
          (new-vertex :actor {:name "Morgan Freeman" :age 78}) => anything
          (get-veterx-by-key :actor "Morgan Freeman") => (contains {:name "Morgan Freeman" :age 78}))))