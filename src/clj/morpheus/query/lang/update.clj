(ns morpheus.query.lang.update
  (:require [morpheus.query.lang.AST :as AST]
            [morpheus.models.vertex.core :as vertex]
            [morpheus.models.edge.core :as edge]
            [neb.utils :refer [map-on-keys]]))

(defn update-vertex* [vertex field-exp-map]
  (loop [exp-pairs field-exp-map
         vertex-to-update vertex]
    (if (empty? exp-pairs)
      vertex-to-update
      (let [[k exp] (first exp-pairs)]
        (recur (rest exp-pairs)
               (assoc-in vertex-to-update k
                         (AST/eval-with-data vertex exp)))))))

(defn update-vertex [vertex field-exp-map]
  (vertex/update-vertex!
    vertex
    'morpheus.query.lang.update/update-vertex*
    (map-on-keys AST/parse-map-path field-exp-map)))