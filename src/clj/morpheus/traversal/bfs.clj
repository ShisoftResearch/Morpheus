(ns morpheus.traversal.bfs
  (:require [neb.core :as neb]
            [morpheus.computation.base :as compute]
            [morpheus.messaging.core :as msg]
            [morpheus.models.vertex.core :as vertex]
            [neb.utils :refer :all]
            [neb.base :as nb]
            [cluster-connector.distributed-store.core :as ds]
            [com.climate.claypoole :as cp]
            [morpheus.models.edge.core :as edges]
            [morpheus.models.edge.base :as eb]
            [morpheus.query.lang.evaluation :as eva]
            [morpheus.traversal.bfs.rebuild :refer :all]
            [clojure.core.async :as a]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [morpheus.computation.data-map :as data-map])
  (:import (java.util Map)
           (org.shisoft.hurricane.datastructure SeqableMap)))

; Parallel breadth-first-search divised by Aydın Buluç
; The algoithm was first introduced in Lawrence National Laboratory on BlueGene supercomputer
; I don't really sure my impelementation is 100% the same as theirs. But the ideas are similiar.
; I also took some idea from Charles E. Leiserson and Tao B. Schardl, for the underlying BSP data processing mechanism.
; Morpueus will use the task founder server as the only machine for join operations in each level.

(def tasks-vertices (atom {}))
(def tasks-level (atom {}))
(def superstep-tasks (atom {}))

(defn partation-vertices [vertex-ids]
  (group-by
    #(nb/locate-cell-by-id %)
    vertex-ids))

(defn proc-forward-msg [task-id data]
  (let [[superstep-id vertices-stack] data
        {:keys [founder filters with-edges? with-vertices?] :as extra-params} (compute/get-task task-id)]
    (msg/send-msg
      founder :BFS-RETURN
      [superstep-id
       (->> (cp/pfor
              compute/compution-threadpool
              [vertex-id vertices-stack]
              (let [vertex (vertex/vertex-by-id vertex-id)
                    vertex-criteria (get-in filters [:criteria :vertex])
                    vertex-vailed (if vertex-criteria (eva/eval-with-data vertex vertex-criteria) true)]
                (when vertex-vailed
                  (let [neighbours (apply edges/neighbours-edges vertex (if filters (mapcat identity filters) []))
                        vertex-res (if with-vertices? vertex (select-keys vertex [:*id*]))
                        edges-res (doall (map (fn [edge]
                                                (assoc (if with-edges? edge {})
                                                  :*opp* (eb/get-oppisite edge vertex-id)))
                                              neighbours))]
                    [vertex-res edges-res]))))
            (filter identity)
            (doall))]
      :task-id task-id)))

(defn proc-return-msg [task-id data]
  (let [[superstep-id vertices-stack] data
        deepth (get @tasks-level task-id)]
    (a/go
      (let [^Map vertices-map (get @tasks-vertices task-id)]
        (locking vertices-map
          (doseq [[vertex-res edges-res] vertices-stack]
            (let [vertex-id (:*id* vertex-res)]
              (.put vertices-map vertex-id
                    (merge (.get vertices-map vertex-id)
                           vertex-res
                           {:*visited* true}))
              (doseq [edge edges-res]
                (let [opp-id (:*opp* edge)]
                  (.put vertices-map opp-id
                        (let [oppv (get vertices-map opp-id)]
                          (if oppv (if-not (:*visited* oppv)
                                     (update oppv :*parents* conj vertex-id) oppv)
                                   {:*id* opp-id
                                    :*visited* false
                                    :*level* deepth
                                    :*parents* [vertex-id]})))))))))
      (a/>! (get @superstep-tasks superstep-id) true))))

(defn proc-stack [task-id stack]
  (let[server-vertices (partation-vertices stack)
       step-chains (doall (cp/pfor
                            compute/compution-threadpool
                            [[server-name vertex-ids] server-vertices]
                            (let [superstep-id (neb/rand-cell-id)
                                  superstep-chan (a/chan 1)]
                              (swap! superstep-tasks assoc superstep-id superstep-chan)
                              (msg/send-msg server-name :BFS-FORWARD [superstep-id vertex-ids] :task-id task-id)
                              [superstep-id superstep-chan])))]
    (doseq [[superstep-id superstep-chan] step-chains]
      (a/<!! superstep-chan)
      (swap! superstep-tasks dissoc superstep-id))))

(defn bfs ^SeqableMap [vertex & {:keys [filters max-deepth timeout level-stop-cond with-edges? with-vertices? on-disk?] :as extra-params
                                 :or {timeout 60000 max-deepth 10}}]
  "Perform parallel and distributed breadth first search"
  (let [task-id (neb/rand-cell-id)
        vertex-id (:*id* vertex)
        initial-stack [vertex-id]
        ^SeqableMap vertices-map (data-map/gen-map task-id on-disk?)]
    (compute/new-task task-id (assoc extra-params :founder @ds/this-server-name))
    (swap! tasks-level assoc task-id 0)
    (swap! tasks-vertices assoc task-id vertices-map)
    (proc-stack task-id initial-stack)
    (loop [level 1]
      (let [vertices-list (.keyColl vertices-map)
            unvisited-tr (atom (transient []))
            stop-required? (atom false)]
        (loop [vl   vertices-list]
          (let [vid  (first vl)
                {:keys [*id* *visited* *level*] :as vertex} (.get vertices-map vid)]
            (when (and *id* (not *visited*)) (swap! unvisited-tr conj! *id*))
            (when-not (= 1 (count vl))
              (if
                (or (not vertex)
                    (when (and level-stop-cond (= *level* (dec level)))
                       (eva/eval-with-data vertex level-stop-cond)))
                (reset! stop-required? true)
                (recur (rest vl))))))
        (let [unvisited (persistent! @unvisited-tr)]
          (if (or @stop-required? (empty? unvisited) (> level max-deepth))
            (doseq [unvisted-id unvisited]
              (.remove vertices-map unvisted-id))
            (do (proc-stack task-id unvisited)
                (swap! tasks-level update task-id inc)
                (recur (inc level)))))))
    (swap! tasks-vertices dissoc task-id)
    (swap! tasks-level dissoc task-id)
    vertices-map))

(defn path-to [vertex-a vertex-b & params]
  (let [vertex-a-id (:*id* vertex-a)
        vertex-b-id (:*id* vertex-b)
        vertices-map (apply bfs vertex-a params)
        res-chan (atom (transient []))]
    (next-parents vertex-a-id [] #{vertex-b-id} vertices-map res-chan vertex-b-id)
    (persistent! @res-chan)))

(defn shortest-path [vertex-a vertex-b & params]
  (apply path-to vertex-a vertex-b
         :level-stop-cond ['(= :*id* :.vid) {:.vid (:*id* vertex-b)}]
         params))

(defn has-path? [vertex-a vertex-b & params]
  (let [vertex-b-id (:*id* vertex-b)]
    (.containsKey
      (apply bfs vertex-a
             :level-stop-cond ['(= :*id* :.vid) {:.vid vertex-b-id}]
             params)
      vertex-b-id)))

(defn one-path-to [vertex-a vertex-b & params]
  (first (apply shortest-path vertex-a vertex-b params)))

(msg/register-action :BFS-FORWARD proc-forward-msg)
(msg/register-action :BFS-RETURN proc-return-msg)
