(ns morpheus.tests.traversal
  (:require [midje.sweet :refer :all]
            [morpheus.tests.server :refer [with-server]]
            [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]
            [morpheus.traversal.dfs :refer :all]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(facts
  "Traversal Tests"
  (with-server
    (let [get-vertex1 (fn [n] (vertex-by-key :item-1 (str n)))
          get-vertex2 (fn [n] (vertex-by-key :item-2 (str n)))]
      (fact "Schemas"
            (new-vertex-group! :item-1 {:body :dynamic :key-field :name}) => anything
            (new-vertex-group! :item-2 {:body :dynamic :key-field :name}) => anything
            (new-edge-group! :link {:type :directed :body :dynamic}) => anything)
      (fact "Create Edges"
            (new-vertex! :item-1 {:name "1"}) => anything
            (new-vertex! :item-1 {:name "2"}) => anything
            (new-vertex! :item-1 {:name "3"}) => anything
            (new-vertex! :item-1 {:name "4"}) => anything
            (new-vertex! :item-1 {:name "5"}) => anything
            (new-vertex! :item-1 {:name "6"}) => anything
            (new-vertex! :item-1 {:name "7"}) => anything
            (new-vertex! :item-1 {:name "8"}) => anything
            (new-vertex! :item-1 {:name "9"}) => anything
            (new-vertex! :item-1 {:name "10"}) => anything
            (new-vertex! :item-2 {:name "11"}) => anything
            (new-vertex! :item-2 {:name "12"}) => anything
            (new-vertex! :item-2 {:name "13"}) => anything
            (new-vertex! :item-2 {:name "14"}) => anything
            (new-vertex! :item-2 {:name "15"}) => anything
            (new-vertex! :item-2 {:name "16"}) => anything
            (new-vertex! :item-2 {:name "17"}) => anything
            (new-vertex! :item-2 {:name "18"}) => anything
            (new-vertex! :item-2 {:name "19"}) => anything
            (new-vertex! :item-2 {:name "20"}) => anything
            (new-vertex! :item-2 {:name "21"}) => anything
            (new-vertex! :item-2 {:name "22"}) => anything)
      (fact "Create Network"

            ;;   1  - 2  - 3  - 4  - 5
            ;;             |
            ;;   6  - 7  - 8  - 9  - 10
            ;;   |                    |
            ;;   11 - 12 - 13 - 14 - 15
            ;;
            ;;   16 - 17 - 18 - 19 - 20
            ;;   |                   |
            ;;   21                  22

            (fact "Sub Graph 1"
                  (link! (get-vertex1 1) :link (get-vertex1 2))  => anything
                  (link! (get-vertex1 2) :link (get-vertex1 3))  => anything
                  (link! (get-vertex1 3) :link (get-vertex1 4))  => anything
                  (link! (get-vertex1 4) :link (get-vertex1 5))  => anything
                  (link! (get-vertex1 5) :link (get-vertex1 6))  => anything
                  (link! (get-vertex1 6) :link (get-vertex1 7))  => anything
                  (link! (get-vertex1 7) :link (get-vertex1 8))  => anything
                  (link! (get-vertex1 8) :link (get-vertex1 9))  => anything
                  (link! (get-vertex1 9) :link (get-vertex1 10)) => anything
                  (link! (get-vertex1 3) :link (get-vertex1 8))  => anything

                  (link! (get-vertex2 11) :link (get-vertex2 12))  => anything
                  (link! (get-vertex2 12) :link (get-vertex2 13))  => anything
                  (link! (get-vertex2 13) :link (get-vertex2 14))  => anything
                  (link! (get-vertex2 14) :link (get-vertex2 15))  => anything

                  (link! (get-vertex1 6)  :link (get-vertex2 11))  => anything
                  (link! (get-vertex1 10) :link (get-vertex2 15))  => anything)

            (fact "Sub Graph 2"
                  (link! (get-vertex2 16) :link (get-vertex2 17))  => anything
                  (link! (get-vertex2 17) :link (get-vertex2 18))  => anything
                  (link! (get-vertex2 18) :link (get-vertex2 19))  => anything
                  (link! (get-vertex2 19) :link (get-vertex2 20))  => anything
                  (link! (get-vertex2 16) :link (get-vertex2 21))  => anything
                  (link! (get-vertex2 20) :link (get-vertex2 22))  => anything))
      (fact "Simple check"
            (degree (get-vertex1 1)) => 1
            (count (apply neighbours (get-vertex1 1) [])) => 1)
      (fact "DFS"
            (println "Starting DFS")
            ($ dfs (get-vertex1 1)) => anything
            (println "DFS Test End")))))