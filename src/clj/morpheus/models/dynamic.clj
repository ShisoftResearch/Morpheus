(ns morpheus.models.dynamic
  (:require [neb.core :as neb]
            [neb.cell :as neb-cell]
            [morpheus.models.base :as mb]
            [cluster-connector.remote-function-invocation.core :refer [compiled-cache]]))

(defn preproc-for-dynamic-write [neb-sid data]
  (let [defined-fields (map first (:f (neb/get-schema-by-id neb-sid)))
        defined-map (select-keys data defined-fields)
        dynamic-map (apply dissoc data defined-fields)]
    (assoc defined-map :*data* dynamic-map)))

(defn assemble-dynamic-outcome [neb-cell]
  (merge (dissoc neb-cell :*data*) (:*data* neb-cell)))

(defn update-dynamic-cell [schema-field neb-cell func-sym params]
  (let [neb-sid (:*schema* neb-cell)
        sp (mb/schema-by-neb-id neb-sid)
        vertex (assoc (assemble-dynamic-outcome neb-cell) schema-field sp)]
    (->> (apply (compiled-cache func-sym) vertex params)
         (#(apply dissoc % schema-field neb-cell/internal-cell-fields))
         (preproc-for-dynamic-write neb-sid))))