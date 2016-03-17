(ns morpheus.models.vertex.dynamic
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]
            [morpheus.models.base :as mb]
            [neb.core :as neb]
            [neb.cell :as neb-cell]
            [cluster-connector.remote-function-invocation.core :refer [compiled-cache]]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(def dynamic-veterx-schema-fields
  [[:*data* :obj]])

(defn preproc-for-write [neb-sid data]
  (let [defined-fields (map first (:f (neb/get-schema-by-id neb-sid)))
        defined-map (select-keys data defined-fields)
        dynamic-map (apply dissoc data defined-fields)]
    (assoc defined-map :*data* dynamic-map)))

(defn update-vertex* [neb-cell func-sym params]
  (let [neb-sid (:*schema* neb-cell)
        vp (mb/schema-by-neb-id neb-sid)
        vertex (assemble-vertex vp neb-cell)]
    (->> (apply (compiled-cache func-sym) vertex params)
         (#(apply dissoc % :*vp* neb-cell/internal-cell-fields))
         (preproc-for-write neb-sid))))

(defmethods
  :dynamic vp
  (assemble-vertex
    [neb-cell]
    (merge {:*vp* vp} (dissoc neb-cell :*data*) (:*data* neb-cell)))
  (new-vertex
    [data]
    (let [{:keys [neb-sid]} vp]
      (neb/new-cell-by-ids
        (mb/cell-id-by-data :v vp data) neb-sid
        (preproc-for-write neb-sid data))))
  (update-vertex
    [id func-sym params]
    (assemble-vertex
      vp
      (neb/update-cell*
        id 'morpheus.models.vertex.dynamic/update-vertex*
        func-sym params)))
  (cell-fields
    [fields]
    (concat
      vertex-relation-fields fields
      dynamic-veterx-schema-fields)))