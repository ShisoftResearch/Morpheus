(ns morpheus.traversal.bfs.rebuild
  (:require [cluster-connector.utils.for-debug :refer [$ spy]])
  (:import (org.shisoft.hurricane.datastructure SeqableMap)))

(defn next-parents [a path visited ^SeqableMap vertices-map chan vertex-id]
  (let [vertex (.get vertices-map vertex-id)
        {:keys [*parents* *id*] :as vp} vertex
        new-path (conj path vp)]
    (if (seq *parents*)
      (doseq [parent *parents*]
        (when (not (visited parent))
          (next-parents a new-path (conj visited parent) vertices-map chan parent)))
      (when (= *id* a)
        (swap! chan conj! (reverse new-path))))))