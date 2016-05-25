(ns morpheus.traversal.dfs.mutable)

(defn new-mutable-vertex [vertex]
  {:vertex vertex
   :edges (atom [])})

(defn push-adj [mutable-vertex target-mutable-vertex-id edge]
  (swap! (:edges mutable-vertex) conj {:edge edge :vertex target-mutable-vertex-id}))