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
          (new-vertex-group :movie {:body :defined :key-field :name
                                    :fields [[:name :text]
                                             [:year :short]]}) => anything
          (new-vertex-group :people {:body :dynamic :key-field :name}) => anything)
    (fact "Create Edge Schema"
          (new-edge-group :acted-in {:type :directed :body :dynamic}) => anything
          (new-edge-group :spouse {:type :indirected :body :simple}) => anything)
    (fact "New Veterxs"
          (new-vertex :people {:name "Morgan Freeman"        :age 78}) => anything
          (new-vertex :movie {:name "Batman Begins"         :year 2005}) => anything
          (new-vertex :movie {:name "The Dark Knight"       :year 2008}) => anything
          (new-vertex :movie {:name "The Dark Knight Rises" :year 2012}) => anything
          (new-vertex :movie {:name "Oblivion"              :year 2013}) => anything
          (new-vertex :people {:name "Jeanette Adair Bradshaw"}) => anything)
    (fact "Check Veterxs"
          (get-vertex-by-key :people "Morgan Freeman") => (contains {:name "Morgan Freeman" :age 78})
          (get-vertex-by-key :movie "Batman Begins") => (contains {:name "Batman Begins"  :year 2005})
          (get-vertex-by-key :movie "Oblivion") => (contains {:name "Oblivion" :year 2013}))
    (fact "Create Edges"
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")
                batman-begins  (get-vertex-by-key :movie "Batman Begins")
                dark-knight    (get-vertex-by-key :movie "The Dark Knight")
                oblivion       (get-vertex-by-key :movie "Oblivion")
                jeanette-adair-bradshaw (get-vertex-by-key :people "Jeanette Adair Bradshaw")]
            (create-edge morgan-freeman :acted-in batman-begins {:as "Lucius Fox"}) => anything
            (create-edge morgan-freeman :acted-in dark-knight {:as "Lucius Fox"}) => anything
            (create-edge morgan-freeman :acted-in oblivion {:as "Malcolm Beech"}) => anything
            (create-edge morgan-freeman :spouse jeanette-adair-bradshaw) => anything))
    (fact "Read Edges"
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")
                batman-begins  (get-vertex-by-key :movie "Batman Begins")]
            (neighbours morgan-freeman) => (contains [(contains {:name :spouse, :type :indirected, :direction :*neighbours*})
                                                      (contains {:name :acted-in, :type :directed, :direction :*outbounds*})]
                                                     :gaps-ok :in-any-order)
            (neighbours morgan-freeman) => #(= 4 (count %))
            (neighbours morgan-freeman :directions :*outbounds*) => (just [(contains {:name :acted-in, :type :directed, :direction :*outbounds*})
                                                                           (contains {:name :acted-in, :type :directed, :direction :*outbounds*})
                                                                           (contains {:name :acted-in, :type :directed, :direction :*outbounds*})])
            (neighbours morgan-freeman :relationships :spouse) => (just [(contains {:name :spouse, :type :indirected, :direction :*neighbours*})])
            (neighbours batman-begins) => (just [(contains {:name :acted-in :type :directed :direction :*inbounds*})])))
    (fact "Update Vertex"
          )))