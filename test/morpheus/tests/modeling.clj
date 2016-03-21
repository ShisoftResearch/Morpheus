(ns morpheus.tests.modeling
  (:require [midje.sweet :refer :all]
            [morpheus.tests.server :refer [with-server]]
            [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(facts
  "CRUD for Veterxies and Edges"
  (with-server
    (fact "Create Veterx Schema"
          (new-vertex-group! :movie {:body  :defined :key-field :name
                                    :fields [[:name :text]
                                             [:year :short]
                                             [:directed-by :obj]]}) => anything
          (new-vertex-group! :people {:body :dynamic :key-field :name}) => anything)
    (fact "Create Edge Schema"
          (new-edge-group! :acted-in {:type :directed :body :dynamic}) => anything
          (new-edge-group! :spouse {:type :indirected :body :simple}) => anything)
    (fact "New Veterxs"
          (new-vertex! :people {:name "Morgan Freeman"        :age 78}) => anything
          (new-vertex! :movie {:name "Batman Begins"         :year 2005}) => anything
          (new-vertex! :movie {:name "The Dark Knight"       :year 2008}) => anything
          (new-vertex! :movie {:name "The Dark Knight Rises" :year 2012}) => anything
          (new-vertex! :movie {:name "Oblivion"              :year 2010}) => anything
          (new-vertex! :people {:name "Jeanette Adair Bradshaw"}) => anything)
    (fact "Check Veterxs"
          (vertex-by-key :people "Morgan Freeman") => (contains {:name "Morgan Freeman" :age 78})
          (vertex-by-key :movie "Batman Begins") => (contains {:name "Batman Begins"  :year 2005})
          (vertex-by-key :movie "Oblivion") => (contains {:name "Oblivion" :year 2010}))
    (fact "Create Edges"
          (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")
                batman-begins  (vertex-by-key :movie "Batman Begins")
                dark-knight    (vertex-by-key :movie "The Dark Knight")
                oblivion       (vertex-by-key :movie "Oblivion")
                dark-knight-rises (vertex-by-key :name "The Dark Knight Rises")
                jeanette-adair-bradshaw (vertex-by-key :people "Jeanette Adair Bradshaw")]
            (link! morgan-freeman :acted-in batman-begins {:as "Lucius Fox"}) => anything
            (link! morgan-freeman :acted-in dark-knight {:as "Lucius Fox"}) => anything
            (link! morgan-freeman :acted-in oblivion {:as "Malcolm Beech"}) => anything
            (link! morgan-freeman :acted-in dark-knight-rises {:as "Lucius Fox"}) => anything
            (link! morgan-freeman :spouse jeanette-adair-bradshaw) => anything))
    (fact "Read Edges"
          (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")
                batman-begins  (vertex-by-key :movie "Batman Begins")
                oblivion       (vertex-by-key :movie "Oblivion")]
            (neighbours morgan-freeman) => (contains [(contains {:*ep* (contains {:name :spouse, :type :indirected}), :*direction* :*neighbours*})
                                                      (contains {:*ep* (contains {:name :acted-in, :type :directed}), :*direction* :*outbounds*})]
                                                     :gaps-ok :in-any-order)
            (neighbours morgan-freeman) => #(= 5 (count %))
            (degree morgan-freeman) => 5
            (neighbours morgan-freeman :directions :*outbounds*) => (just [(contains {:*ep* (contains {:name :acted-in, :type :directed}) :*direction* :*outbounds*})
                                                                           (contains {:*ep* (contains {:name :acted-in, :type :directed}),  :*direction* :*outbounds*})
                                                                           (contains {:*ep* (contains {:name :acted-in, :type :directed}),  :*direction* :*outbounds*})
                                                                           (contains {:*ep* (contains {:name :acted-in, :type :directed}),  :*direction* :*outbounds*})])
            (neighbours morgan-freeman {:types :spouse}) => (just [(contains {:*ep* (contains {:name :spouse, :type :indirected}),  :*direction* :*neighbours*})])
            (degree morgan-freeman {:types :spouse} {:types :acted-in}) => 5
            (neighbours batman-begins) => (just [(contains {:*ep* (contains {:name :acted-in :type :directed}) :*direction* :*inbounds*})])
            (relationships morgan-freeman batman-begins) => (just (contains {:*direction* :*outbounds* :as "Lucius Fox"}))
            (relationships batman-begins oblivion) => nil))
    (fact "Update Defined Vertex"
          (update-vertex! (vertex-by-key :movie "Oblivion")
                          'clojure.core/assoc :year 2013) => anything)
    (fact "Check Updated Defined Vertex"
          (vertex-by-key :movie "Oblivion") => (contains {:name "Oblivion" :year 2013}))
    (fact "Update Dynamic Vertex"
          (update-vertex! (vertex-by-key :people "Morgan Freeman")
                          'clojure.core/assoc :said "Every time I show up and explain something, I earn a freckle.") => anything)
    (fact "Check Updated Dynamic Vertex"
          (vertex-by-key :people "Morgan Freeman") => (contains {:said "Every time I show up and explain something, I earn a freckle."})
          (degree (vertex-by-key :people "Morgan Freeman")) => 5)
    (fact "Reset Vertex"
          (reset-vertex! (vertex-by-key :movie "Batman Begins") {:name "Batman Begins" :year 2005 :directed-by "Christopher Nolan"}) => anything
          (degree (vertex-by-key :movie "Batman Begins")) => 1
          (vertex-by-key :movie "Batman Begins") => (contains {:name "Batman Begins" :year 2005 :directed-by "Christopher Nolan"}))
    (fact "Update Edges"
          (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")
                mf-spouse-edge (first (neighbours morgan-freeman :types :spouse))
                rand-acted-movie (first (neighbours morgan-freeman :types :acted-in))]
            mf-spouse-edge => map?
            (update-edge! mf-spouse-edge :a :b) => (throws AssertionError)
            rand-acted-movie => map?
            (update-edge! rand-acted-movie 'clojure.core/assoc :actor-name "Morgan Freeman") => anything))
    (fact "Check Edge Updated"
          (first (neighbours (vertex-by-key :people "Morgan Freeman") :types :acted-in)) => (contains {:actor-name "Morgan Freeman"}))
    (fact "Delete Vertex"
          (delete-vertex! (vertex-by-key :people "Jeanette Adair Bradshaw")) => anything
          (delete-vertex! (vertex-by-key :movie "Oblivion")) => anything)
    (fact "Check Deleted Vertex"
          (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")]
            (vertex-by-key :people "Jeanette Adair Bradshaw") => nil?
            (vertex-by-key :movie "Oblivion") => nil?
            (degree morgan-freeman) => 3))
    (fact "Add Deleted Vertex For Following Tests"
          (new-vertex! :people {:name "Jeanette Adair Bradshaw"}) => anything
          (link!
            (vertex-by-key :people "Morgan Freeman") :spouse
            (vertex-by-key :people "Jeanette Adair Bradshaw")) => anything
          (degree (vertex-by-key :people "Morgan Freeman")) => 4)
    (fact "Delete Edge"
          (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")
                rand-acted-movie (first (neighbours morgan-freeman :types :acted-in))
                mf-spouse-edge (first (neighbours morgan-freeman :types :spouse))]
            (unlink! rand-acted-movie) => anything
            (unlink! mf-spouse-edge) => anything
            (degree (reload-vertex morgan-freeman)) => 2))))