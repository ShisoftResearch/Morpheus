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
      (new-vertex-group :movie {:body :defined :key-field :name
                                :fields [[:name :text]
                                         [:year :short]
                                         [:directed-by :obj]]})
      (new-vertex-group :people {:body :dynamic :key-field :name})
      (new-edge-group :acted-in {:type :directed :body :dynamic})
      (new-edge-group :spouse {:type :indirected :body :simple})
      (while true
        (on-error-resume-next
          (new-vertex :people {:name "Morgan Freeman"        :age 78})
          (new-vertex :movie {:name "Batman Begins"         :year 2005})
          (new-vertex :movie {:name "The Dark Knight"       :year 2008})
          (new-vertex :movie {:name "The Dark Knight Rises" :year 2012})
          (new-vertex :movie {:name "Oblivion"              :year 2010})
          (new-vertex :people {:name "Jeanette Adair Bradshaw"})
          (get-vertex-by-key :people "Morgan Freeman")
          (get-vertex-by-key :movie "Batman Begins")
          (get-vertex-by-key :movie "Oblivion")
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")
                batman-begins  (get-vertex-by-key :movie "Batman Begins")
                dark-knight    (get-vertex-by-key :movie "The Dark Knight")
                oblivion       (get-vertex-by-key :movie "Oblivion")
                dark-knight-rises (get-vertex-by-key :name "The Dark Knight Rises")
                jeanette-adair-bradshaw (get-vertex-by-key :people "Jeanette Adair Bradshaw")]
            (create-edge morgan-freeman :acted-in batman-begins {:as "Lucius Fox"})
            (create-edge morgan-freeman :acted-in dark-knight {:as "Lucius Fox"})
            (create-edge morgan-freeman :acted-in oblivion {:as "Malcolm Beech"})
            (create-edge morgan-freeman :acted-in dark-knight-rises {:as "Lucius Fox"})
            (create-edge morgan-freeman :spouse jeanette-adair-bradshaw))
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")
                batman-begins  (get-vertex-by-key :movie "Batman Begins")]
            (neighbours morgan-freeman)
            (neighbours morgan-freeman)
            (degree morgan-freeman)
            (neighbours morgan-freeman :directions :*outbounds*)
            (neighbours morgan-freeman :relationships :spouse)
            (degree morgan-freeman :relationships :spouse)
            (neighbours batman-begins))
          (update-vertex (get-vertex-by-key :movie "Oblivion")
                         'clojure.core/assoc :year 2013)
          (get-vertex-by-key :movie "Oblivion")
          (update-vertex (get-vertex-by-key :people "Morgan Freeman")
                         'clojure.core/assoc :said "Every time I show up and explain something, I earn a freckle.")
          (get-vertex-by-key :people "Morgan Freeman")
          (degree (get-vertex-by-key :people "Morgan Freeman"))
          (reset-vertex (get-vertex-by-key :movie "Batman Begins") {:name "Batman Begins" :year 2005 :directed-by "Christopher Nolan"})
          (degree (get-vertex-by-key :movie "Batman Begins"))
          (get-vertex-by-key :movie "Batman Begins")
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")
                mf-spouse-edge (first (neighbours morgan-freeman :relationships :spouse))
                rand-acted-movie (first (neighbours morgan-freeman :relationships :acted-in))]
            (update-edge rand-acted-movie 'clojure.core/assoc :actor-name "Morgan Freeman"))
          (neighbours (get-vertex-by-key :people "Morgan Freeman") :relationships :acted-in)
          (delete-vertex (get-vertex-by-key :people "Jeanette Adair Bradshaw"))
          (delete-vertex (get-vertex-by-key :movie "Oblivion"))
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")]
            (get-vertex-by-key :people "Jeanette Adair Bradshaw")
            (get-vertex-by-key :movie "Oblivion")
            (degree morgan-freeman))
          (new-vertex :people {:name "Jeanette Adair Bradshaw"})
          (create-edge
            (get-vertex-by-key :people "Morgan Freeman") :spouse
            (get-vertex-by-key :people "Jeanette Adair Bradshaw"))
          (degree (get-vertex-by-key :people "Morgan Freeman"))
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")
                rand-acted-movie (first (neighbours morgan-freeman :relationships :acted-in))
                mf-spouse-edge (first (neighbours morgan-freeman :relationships :spouse))]
            (delete-edge rand-acted-movie)
            (delete-edge mf-spouse-edge)
            (degree (reload-vertex morgan-freeman)))
          (let [morgan-freeman (get-vertex-by-key :people "Morgan Freeman")
                batman-begins  (get-vertex-by-key :movie "Batman Begins")
                dark-knight    (get-vertex-by-key :movie "The Dark Knight")
                oblivion       (get-vertex-by-key :movie "Oblivion")
                dark-knight-rises (get-vertex-by-key :name "The Dark Knight Rises")
                jeanette-adair-bradshaw (get-vertex-by-key :people "Jeanette Adair Bradshaw")]
            (on-error-resume-next
              (when morgan-freeman (delete-vertex morgan-freeman))
              (when batman-begins (delete-vertex batman-begins))
              (when dark-knight (delete-vertex dark-knight))
              (when oblivion (delete-vertex oblivion))
              (when dark-knight-rises (delete-vertex dark-knight-rises))
              (when jeanette-adair-bradshaw (delete-vertex jeanette-adair-bradshaw))))))))
