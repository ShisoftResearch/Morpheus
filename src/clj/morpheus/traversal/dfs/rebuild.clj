(ns morpheus.traversal.dfs.rebuild
  (:require [cluster-connector.utils.for-debug :refer [$ spy]]
            [clojure.core.async :as a]
            [manifold.stream :as s]))

(defn new-mutable-vertex [vp]
  {:vertex-props vp
   :adjacents    (atom [])})

(defn push-adj [mutable-vertex target-mutable-vertex-props]
  (when (and mutable-vertex target-mutable-vertex-props)
    (swap! (:adjacents mutable-vertex) conj target-mutable-vertex-props)))

(defn preproc-stack-for-adj-list [stack]
  (into
    {}
    (map
      (fn [{:keys [id] :as vp}]
        [id (new-mutable-vertex vp)])
      stack)))

(defn adjacancy-list [stack]
  (let [vertices-map (preproc-stack-for-adj-list stack)]
    (doseq [[vid vm] vertices-map]
      (let [vp (:vertex-props vm)
            {:keys [parent edge]} vp]
        (push-adj (get vertices-map parent) vp)))
    (map (fn [[vid vm]]
           (update vm :adjacents deref))
         vertices-map)))

(defn- next-parents [a path visited vertices-map chan vertex-id]
  (doseq [vertex (get vertices-map vertex-id)]
    (let [{:keys [parent id] :as vp} vertex
          new-path (conj path vp)]
      (if (and parent (not (visited parent)))
        (next-parents a new-path (conj visited parent) vertices-map chan parent)
        (when (= id a)
          (swap! chan conj! (reverse new-path)))))))

(defn path-from-stack
  "Recursive (non-linear) path extraction"
  [stack a b]
  (let [vertices-map (group-by :id stack)
        res-chan (atom (transient []))]
    (next-parents a [] #{b} vertices-map res-chan b)
    (persistent! @res-chan)))

(defn one-path-from-stack
  "Linear path extraction"
  [stack b]
  (let [vertices-info (into {} (map (fn [{:keys [id] :as vp}] [id vp]) stack))]
    (loop [path [(get vertices-info b)]
           parent (:parent (get vertices-info b))]
      (if (nil? parent)
        (reverse path)
        (recur (conj path (get vertices-info parent))
               (:parent (get vertices-info parent)))))))