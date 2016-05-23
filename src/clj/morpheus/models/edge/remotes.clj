(ns morpheus.models.edge.remotes
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.query.lang.AST :as AST]
            [morpheus.models.vertex.core :as vertex]
            [neb.core :as neb]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(defn neighbours-edges*
  "Neighbouthoods edges from Cid Lists"
  [direction sid vertex-id filters]
  (with-cid-list
    (let [edge-filter (:edge filters)
          cid-lists (extract-cid-lists direction sid vertex-id filters)]
      (->> (map
             (fn [{:keys [*group-props* *direction*] :as cid-list}]
               (map
                 (fn [x] (when x (format-edge-cells *group-props* *direction* x)))
                 (edges-from-cid-array *group-props* cid-list vertex-id)))
             cid-lists)
           (flatten)
           (map
             (fn [edge]
               (when
                 (and edge
                      (if edge-filter
                        (AST/eval-with-data
                          edge edge-filter)
                        true))
                 edge)))
           (filter identity)))))

(defn neighbours*
  [direction sid vertex-id filters]
  (with-cid-list
    (let [vertex-filter (:vertex filters)
          edges (neighbours-edges* direction sid vertex-id filters)]
      (->> edges
           (map
             (fn [edge]
               (let [oppisite-vertex-id (get-oppisite edge vertex-id)
                     oppisite-vertex (vertex/get-veterx-by-id oppisite-vertex-id)]
                 (when (or (not vertex-filter) (AST/eval-with-data oppisite-vertex vertex-filter))
                   (assoc oppisite-vertex
                     :*edge* edge)))))
           (filter identity)))))

(defn count-edges [direction sid vertex-id filters]
  (let [[vertex-filter edge-filter] ((juxt :vertex :edge) filters)]
    (cond
      (and (not edge-filter) (not vertex-filter))
      (with-cid-list
        (+ (read-cid-list-len)
           (if next-cid
             (neb/read-lock-exec*
               next-cid
               'morpheus.models.edge.remotes/count-edges
               direction sid vertex-id filters)
             0)))
      (and edge-filter (not vertex-filter))
      (count (neighbours-edges* direction sid vertex-id filters))
      vertex-filter
      (count (neighbours direction sid vertex-id filters)))))