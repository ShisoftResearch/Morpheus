(ns morpheus.tests.modeling
  (:require [midje.sweet :refer :all]
            [morpheus.tests.server :refer [with-server]]
            [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]))

(facts
  "Creating Veterx and Edges"
  (with-server
    (fact "Create Veterx Schema"
          (new-vertex-group :actor {:body :dynamic}) => anything
          (new-vertex-group :movie {:body :dynamic}) => anything)
    (fact "Create Edge Schema"
          (new-edge-group :acted   {:type :directed :body :dynamic}) => anything)
    ))