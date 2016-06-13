(ns morpheus.traversal.bfs
  (:require [neb.core :as neb]
            [morpheus.computation.base :as compute]))

; Parallel breadth-first-search divised by Aydın Buluç
; The algoithm was first introduced in Lawrence National Laboratory on BlueGene supercomputer
; I don't really sure my impelementation is 100% the same as theirs. But the ideas are similiar.
; I also took some idea from Charles E. Leiserson and Tao B. Schardl, for the underlying BSP data processing mechanism.
; Morpueus will use the task founder server as the only machine for join operations in each level.

(def superstep-tasks (atom {}))

(defn fetch-local-edges [task-id vertex-ids]
  )

(defn partation-vertices [vertex-ids]
  )

(defn bfs [vertex {:keys [filters max-deepth stop-cond timeout with-edges? with-vertices?] :as extra-params
                   :or {:timeout 60000}}]
  "Perform parallel and distributed breadth first search"
  (let [task-id (neb/rand-cell-id)]
    (compute/new-task task-id extra-params)
    ))