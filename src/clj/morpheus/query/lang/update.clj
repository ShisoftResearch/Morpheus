(ns morpheus.query.lang.update
  (:require [morpheus.query.lang.evaluation :as eva]
            [morpheus.models.vertex.core :as vertex]
            [morpheus.models.edge.core :as edge]
            [neb.utils :refer [map-on-keys]]))

(defn update-object* [vertex field-exp-map]
  (loop [exp-pairs field-exp-map
         vertex-to-update vertex]
    (if (empty? exp-pairs)
      vertex-to-update
      (let [[k exp] (first exp-pairs)]
        (recur (rest exp-pairs)
               (assoc-in vertex-to-update k
                         (eva/eval-with-data vertex exp)))))))

(defn update-object [update-func obj field-exp-map]
  (update-func
    obj 'morpheus.query.lang.update/update-object*
    (map-on-keys eva/parse-map-path field-exp-map)))

(defn update-vertex [vertex field-exp-map]
  (update-object vertex/update-vertex! vertex field-exp-map))

(defn update-edge [edge field-exp-map]
  (update-object edge/update-edge! edge field-exp-map))