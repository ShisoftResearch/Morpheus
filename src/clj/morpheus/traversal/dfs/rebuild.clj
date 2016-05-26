(ns morpheus.traversal.dfs.rebuild
  (:require [morpheus.traversal.dfs.mutable :as mv]))

(defn new-mutable-vertex [vp]
  {:vertex-props vp
   :adjacents    (atom [])})

(defn push-adj [mutable-vertex target-mutable-vertex-props]
  (swap! (:adjacents mutable-vertex) conj target-mutable-vertex-props))

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

(defn complete-adjacancy-list [stack]
  (throw (UnsupportedOperationException.)))

(defn paths-from-stack [stack a b]
  )