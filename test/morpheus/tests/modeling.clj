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
          (new-vertex-group :movie {:body :defined :key-field :name
                                    :fields [[:name :text]
                                             [:year :short]
                                             [:directed-by :obj]]}) => anything
          (new-vertex-group :people {:body :dynamic :key-field :name}) => anything)
    (fact "Create Edge Schema"
          (new-edge-group :acted-in {:type :directed :body :dynamic}) => anything
          (new-edge-group :spouse {:type :indirected :body :simple}) => anything)
    (fact "New Veterxs"
          (new-vertex :people {:name "Morgan Freeman"        :age 78}) => anything
          (new-vertex :movie {:name "Batman Begins"         :year 2005}) => anything
          (new-vertex :movie {:name "The Dark Knight"       :year 2008}) => anything
          (new-vertex :movie {:name "The Dark Knight Rises" :year 2012}) => anything
          (new-vertex :movie {:name "Oblivion"              :year 2010}) => anything
          (new-vertex :people {:name "Jeanette Adair Bradshaw"}) => anything)
    (fact "Check Veterxs"
          (get-vertex-by-key :people "Morgan Freeman") => (contains {:name "Morgan Freeman" :age 78})
          (get-vertex-by-key :movie "Batman Begins") => (contains {:name "Batman Begins"  :year 2005})
          (get-vertex-by-key :movie "Oblivion") => (contains {:name "Oblivion" :year 2010}))
    (fact "Create Edges"
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")
                batman-begins  (get-vertex-by-key :movie "Batman Begins")
                dark-knight    (get-vertex-by-key :movie "The Dark Knight")
                oblivion       (get-vertex-by-key :movie "Oblivion")
                dark-knight-rises (get-vertex-by-key :name "The Dark Knight Rises")
                jeanette-adair-bradshaw (get-vertex-by-key :people "Jeanette Adair Bradshaw")]
            (create-edge morgan-freeman :acted-in batman-begins {:as "Lucius Fox"}) => anything
            (create-edge morgan-freeman :acted-in dark-knight {:as "Lucius Fox"}) => anything
            (create-edge morgan-freeman :acted-in oblivion {:as "Malcolm Beech"}) => anything
            (create-edge morgan-freeman :acted-in dark-knight-rises {:as "Lucius Fox"}) => anything
            (create-edge morgan-freeman :spouse jeanette-adair-bradshaw) => anything))
    (fact "Read Edges"
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")
                batman-begins  (get-vertex-by-key :movie "Batman Begins")]
            (neighbours morgan-freeman) => (contains [(contains {:*ep* (contains {:name :spouse, :type :indirected}), :*direction* :*neighbours*})
                                                      (contains {:*ep* (contains {:name :acted-in, :type :directed}), :*direction* :*outbounds*})]
                                                     :gaps-ok :in-any-order)
            (neighbours morgan-freeman) => #(= 5 (count %))
            (neighbours morgan-freeman :directions :*outbounds*) => (just [(contains {:*ep* (contains {:name :acted-in, :type :directed}) :*direction* :*outbounds*})
                                                                           (contains {:*ep* (contains {:name :acted-in, :type :directed}),  :*direction* :*outbounds*})
                                                                           (contains {:*ep* (contains {:name :acted-in, :type :directed}),  :*direction* :*outbounds*})
                                                                           (contains {:*ep* (contains {:name :acted-in, :type :directed}),  :*direction* :*outbounds*})])
            (neighbours morgan-freeman :relationships :spouse) => (just [(contains {:*ep* (contains {:name :spouse, :type :indirected}),  :*direction* :*neighbours*})])
            (neighbours batman-begins) => (just [(contains {:*ep* (contains {:name :acted-in :type :directed}) :*direction* :*inbounds*})])))
    (fact "Update Defined Vertex"
          (update-vertex (get-vertex-by-key :movie "Oblivion")
                         'clojure.core/assoc :year 2013) => anything)
    (fact "Check Updated Defined Vertex"
          (get-vertex-by-key :movie "Oblivion") => (contains {:name "Oblivion" :year 2013}))
    (fact "Update Dynamic Vertex"
          (update-vertex (get-vertex-by-key :people "Morgan Freeman")
                         'clojure.core/assoc :said "Every time I show up and explain something, I earn a freckle.") => anything)
    (fact "Check Updated Dynamic Vertex"
          (get-vertex-by-key :people "Morgan Freeman") => (contains {:said "Every time I show up and explain something, I earn a freckle."})
          (neighbours (get-vertex-by-key :people "Morgan Freeman")) => #(= 5 (count %)))
    (fact "Reset Vertex"
          (reset-vertex (get-vertex-by-key :movie "Batman Begins") {:name "Batman Begins" :year 2005 :directed-by "Christopher Nolan"}) => anything
          (neighbours (get-vertex-by-key :movie "Batman Begins")) => #(= 1 (count %))
          (get-vertex-by-key :movie "Batman Begins") => (contains {:name "Batman Begins" :year 2005 :directed-by "Christopher Nolan"}))
    (fact "Delete Vertex"
          (delete-vertex (get-vertex-by-key :people "Jeanette Adair Bradshaw")) => anything
          (delete-vertex (get-vertex-by-key :movie "Oblivion")) => anything)
    (fact "Check Deleted Vertex"
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")]
            (get-vertex-by-key :people "Jeanette Adair Bradshaw") => nil?
            (get-vertex-by-key :movie "Oblivion") => nil?
            (neighbours morgan-freeman) => #(= 3 (count %))))))