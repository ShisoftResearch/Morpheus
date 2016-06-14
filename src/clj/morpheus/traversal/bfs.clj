(ns morpheus.traversal.bfs
  (:require [neb.core :as neb]
            [morpheus.computation.base :as compute]
            [morpheus.messaging.core :as msg]
            [morpheus.models.vertex.core :as vertex]
            [neb.base :as nb]
            [cluster-connector.distributed-store.core :as ds]
            [com.climate.claypoole :as cp]
            [morpheus.models.edge.core :as edges]
            [morpheus.models.edge.base :as eb]
            [morpheus.query.lang.evaluation :as eva]))

; Parallel breadth-first-search divised by Aydın Buluç
; The algoithm was first introduced in Lawrence National Laboratory on BlueGene supercomputer
; I don't really sure my impelementation is 100% the same as theirs. But the ideas are similiar.
; I also took some idea from Charles E. Leiserson and Tao B. Schardl, for the underlying BSP data processing mechanism.
; Morpueus will use the task founder server as the only machine for join operations in each level.

(def superstep-tasks (atom {}))
(def searched-stacks (atom {}))

(defn fetch-local-edges [task-id vertex-ids]
  )

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
       (->> (cp/pmap
              (fn [vertex-id]
                (let [vertex (vertex/vertex-by-id vertex-id)
                      vertex-criteria (get-in filters [:criteria :vertex])
                      vertex-vailed (if vertex-criteria (eva/eval-with-data vertex vertex-criteria) true)]
                  (when vertex-vailed
                    (let [neighbours (apply edges/neighbours-edges vertex (if filters (mapcat identity filters) []))
                          vertex-res (if with-vertices? vertex (select-keys vertex [:*id*]))
                          edges-res (map (fn [edge]
                                           (assoc (if with-edges? edge {})
                                             :opp (eb/get-oppisite edge vertex-id)))
                                         neighbours)]
                      [vertex-res edges-res]))))
              vertices-stack)
            (filter identity))]
      :task-id task-id)))

(defn proc-return-msg [task-id data]
  (let [[superstep-id vertices-stack] data]
    ))

(defn proc-stack [task-id stack]
  (let[server-vertices (partation-vertices stack)
       superstep-id (neb/rand-cell-id)]
    (cp/pdoseq
      compute/compution-threadpool
      [[server-name vertices] server-vertices]
      (msg/send-msg server-name :BFS-FORWARD [superstep-id vertices] :task-id task-id))))

(defn bfs [vertex {:keys [filters max-deepth stop-cond timeout with-edges? with-vertices?] :as extra-params
                   :or {timeout 60000 max-deepth 8}}]
  "Perform parallel and distributed breadth first search"
  (let [task-id (neb/rand-cell-id)
        vertex-id (:*id* vertex)
        initial-stack [vertex-id]]
    (compute/new-task task-id (assoc extra-params :founder ds/this-server-name))
    (swap! searched-stacks assoc task-id initial-stack)
    (proc-stack task-id initial-stack)
    (doseq [level (range max-deepth)]
      )))

(msg/register-action :BFS-FORWARD proc-forward-msg)
(msg/register-action :BFS-RETURN proc-return-msg)
