(ns morpheus.models.edge.simple
  (:require [morpheus.models.edge.base :refer :all]
            [morpheus.utils :refer :all]
            [cluster-connector.utils.for-debug :refer [spy $]]))

(def vertex-direction-mapper
  {:*neighbours* {:start-kw :*start*
                  :end-kw   :*end*}
   :*outbounds*  {:start-kw :*start*
                  :end-kw   :*end*}
   :*inbounds*  {:start-kw :*end*
                 :end-kw   :*start*}})

(defmethods
  :simple ep
  (require-edge-cell? [] false)
  (edge-base-schema [] nil)
  (edge-schema [base-schema fields] nil)
  (edges-from-cid-array
    [{:keys [cid-array *direction*] :as cid-list} & [start-vertex]]
    (let [{:keys [start-kw end-kw]} (get vertex-direction-mapper *direction*)]
      (map
        (fn [vertex-cid]
          (hash-map
            start-kw start-vertex
            end-kw   vertex-cid))
        cid-array)))
  (update-edge [id func-sym params] (throw (UnsupportedOperationException. "Simple edges cannot been updated"))))