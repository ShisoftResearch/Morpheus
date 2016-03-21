(ns morpheus.tests.static-performance
  (:require [morpheus.tests.server :refer [with-server]]
            [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]))

(defmacro on-error-resume-next [& body]
  `(do ~@(map
           (fn [l]
             `(try ~l (catch Throwable ex#
                        (println ex#))))
           body)))

(defn start-test []
  (with-server
    (new-vertex-group! :movie {:body  :defined :key-field :name
                                :fields [[:name :text]
                                         [:year :short]
                                         [:directed-by :obj]]})
    (new-vertex-group! :people {:body :dynamic :key-field :name})
    (new-edge-group! :acted-in {:type :directed :body :dynamic})
    (new-edge-group! :spouse {:type :indirected :body :simple})
    (while true
      (on-error-resume-next
        (new-vertex! :people {:name "Morgan Freeman"        :age 78})
        (new-vertex! :movie {:name "Batman Begins"         :year 2005})
        (new-vertex! :movie {:name "The Dark Knight"       :year 2008})
        (new-vertex! :movie {:name "The Dark Knight Rises" :year 2012})
        (new-vertex! :movie {:name "Oblivion"              :year 2010})
        (new-vertex! :people {:name "Jeanette Adair Bradshaw"})
        (vertex-by-key :people "Morgan Freeman")
        (vertex-by-key :movie "Batman Begins")
        (vertex-by-key :movie "Oblivion")
        (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")
                batman-begins  (vertex-by-key :movie "Batman Begins")
                dark-knight    (vertex-by-key :movie "The Dark Knight")
                oblivion       (vertex-by-key :movie "Oblivion")
                dark-knight-rises (vertex-by-key :name "The Dark Knight Rises")
                jeanette-adair-bradshaw (vertex-by-key :people "Jeanette Adair Bradshaw")]
            (link! morgan-freeman :acted-in batman-begins {:as "Lucius Fox"})
            (link! morgan-freeman :acted-in dark-knight {:as "Lucius Fox"})
            (link! morgan-freeman :acted-in oblivion {:as "Malcolm Beech"})
            (link! morgan-freeman :acted-in dark-knight-rises {:as "Lucius Fox"})
            (link! morgan-freeman :spouse jeanette-adair-bradshaw))
        (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")
                batman-begins  (vertex-by-key :movie "Batman Begins")]
            (neighbours morgan-freeman)
            (neighbours morgan-freeman)
            (degree morgan-freeman)
            (neighbours morgan-freeman :directions :*outbounds*)
            (neighbours morgan-freeman :relationships :spouse)
            (degree morgan-freeman :relationships :spouse)
            (neighbours batman-begins))
        (update-vertex! (vertex-by-key :movie "Oblivion")
                          'clojure.core/assoc :year 2013)
        (vertex-by-key :movie "Oblivion")
        (update-vertex! (vertex-by-key :people "Morgan Freeman")
                          'clojure.core/assoc :said "Every time I show up and explain something, I earn a freckle.")
        (vertex-by-key :people "Morgan Freeman")
        (degree (vertex-by-key :people "Morgan Freeman"))
        (reset-vertex! (vertex-by-key :movie "Batman Begins") {:name "Batman Begins" :year 2005 :directed-by "Christopher Nolan"})
        (degree (vertex-by-key :movie "Batman Begins"))
        (vertex-by-key :movie "Batman Begins")
        (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")
                mf-spouse-edge (first (neighbours morgan-freeman :relationships :spouse))
                rand-acted-movie (first (neighbours morgan-freeman :relationships :acted-in))]
            (update-edge! rand-acted-movie 'clojure.core/assoc :actor-name "Morgan Freeman"))
        (neighbours (vertex-by-key :people "Morgan Freeman") :relationships :acted-in)
        (delete-vertex! (vertex-by-key :people "Jeanette Adair Bradshaw"))
        (delete-vertex! (vertex-by-key :movie "Oblivion"))
        (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")]
            (vertex-by-key :people "Jeanette Adair Bradshaw")
            (vertex-by-key :movie "Oblivion")
            (degree morgan-freeman))
        (new-vertex! :people {:name "Jeanette Adair Bradshaw"})
        (link!
            (vertex-by-key :people "Morgan Freeman") :spouse
            (vertex-by-key :people "Jeanette Adair Bradshaw"))
        (degree (vertex-by-key :people "Morgan Freeman"))
        (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")
                rand-acted-movie (first (neighbours morgan-freeman :relationships :acted-in))
                mf-spouse-edge (first (neighbours morgan-freeman :relationships :spouse))]
            (unlink! rand-acted-movie)
            (unlink! mf-spouse-edge)
            (degree (reload-vertex morgan-freeman)))
        (let [morgan-freeman (vertex-by-key :people "Morgan Freeman")
                batman-begins  (vertex-by-key :movie "Batman Begins")
                dark-knight    (vertex-by-key :movie "The Dark Knight")
                oblivion       (vertex-by-key :movie "Oblivion")
                dark-knight-rises (vertex-by-key :name "The Dark Knight Rises")
                jeanette-adair-bradshaw (vertex-by-key :people "Jeanette Adair Bradshaw")]
          (on-error-resume-next
            (when morgan-freeman (delete-vertex! morgan-freeman))
            (when batman-begins (delete-vertex! batman-begins))
            (when dark-knight (delete-vertex! dark-knight))
            (when oblivion (delete-vertex! oblivion))
            (when dark-knight-rises (delete-vertex! dark-knight-rises))
            (when jeanette-adair-bradshaw (delete-vertex! jeanette-adair-bradshaw))))))))
