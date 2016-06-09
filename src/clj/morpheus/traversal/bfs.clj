(ns morpheus.traversal.bfs)

(defn bfs [vertex {:keys [filters max-deepth stop-cond timeout with-edges? with-vertices?] :as extra-params
                   :or {:timeout 60000}}]
  "Perform parallel and distributed breadth first search"
  )