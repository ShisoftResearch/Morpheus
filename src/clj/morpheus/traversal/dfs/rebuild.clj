(ns morpheus.traversal.dfs.rebuild
  (:require [cluster-connector.utils.for-debug :refer [$ spy]]))

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

(defn complete-adjacancy-list [stack]
  (throw (UnsupportedOperationException.)))

(defn path-from-stack
  "Recursive (non-linear) path extraction"
  [stack b]
  (let [vertices-map (group-by :id stack)
        next-parent (fn next-parent [path]
                      (let [last-vertex (last path)]
                        (map
                          (fn [vp]
                            (if vp
                              (next-parent (conj path vp))
                              path))
                          (get vertices-map (:id last-vertex)))))]
    ($ mapcat (fn [b-parent]
           (next-parent [b-parent]))
         (get vertices-map b))))

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