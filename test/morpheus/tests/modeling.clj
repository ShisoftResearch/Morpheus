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
          (new-vertex-group :movie {:body :defined :key-field :name
                                    :fields [[:name :text]
                                             [:year :short]]}) => anything)
    (fact "Create Edge Schema"
          (new-edge-group :acted-in {:type :directed :body :dynamic}) => anything)
    (fact "New Veterxs"
          (new-vertex :actor {:name "Morgan Freeman"        :age 78}) => anything
          (new-vertex :movie {:name "Batman Begins"         :year 2005}) => anything
          (new-vertex :movie {:name "The Dark Knight"       :year 2008}) => anything
          (new-vertex :movie {:name "The Dark Knight Rises" :year 2012}) => anything
          (new-vertex :movie {:name "Oblivion"              :year 2013}) => anything)
    (fact "Check Veterxs"
          (get-veterx-by-key :actor "Morgan Freeman") => (contains {:name "Morgan Freeman" :age 78})
          (get-veterx-by-key :movie "Batman Begins")  => (contains {:name "Batman Begins"  :year 2005})
          (get-veterx-by-key :movie "Oblivion") => (contains {:name "Oblivion" :year 2013}))
    (fact "Create Edges"
          (let [morgan-freeman (get-veterx-by-key :actor "Morgan Freeman")
                batman-begins  (get-veterx-by-key :movie "Batman Begins")
                dark-knight    (get-veterx-by-key :movie "The Dark Knight")
                oblivion       (get-veterx-by-key :movie "Oblivion")]
            (create-edge morgan-freeman :acted-in batman-begins {:as "Lucius Fox"}) => anything
            (create-edge morgan-freeman :acted-in dark-knight {:as "Lucius Fox"}) => anything
            (create-edge morgan-freeman :acted-in oblivion {:as "Malcolm Beech"}) => anything))))