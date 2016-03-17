(ns morpheus.models.vertex.dynamic
  (:require [morpheus.utils :refer :all]
            [morpheus.models.vertex.base :refer :all]
            [morpheus.models.base :as mb]
            [neb.core :as neb]
            [cluster-connector.remote-function-invocation.core :refer [compiled-cache]]))

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
        vertex (assumble-vertex vp neb-cell)]
    (->> (apply (compiled-cache func-sym) vertex params)
         (preproc-for-write neb-sid))))

(defmethods
  :dynamic vp
  (assumble-vertex
    [neb-cell]
    (merge {:*vp* vp} (dissoc neb-cell :*data*) (:*data* neb-cell)))
  (reset-vertex
    [id val]
    )
  (new-vertex
    [data]
    (let [{:keys [neb-sid]} vp]
      (neb/new-cell-by-ids
        (mb/cell-id-by-data :v vp data) neb-sid
        (preproc-for-write neb-sid data))))
  (update-vertex
    [id func-sym params]
    (neb/update-cell*
      id 'morpheus.models.vertex.dynamic/update-vertex*
      func-sym params))
  (cell-fields
    [fields]
    (concat
      vertex-relation-fields fields
      dynamic-veterx-schema-fields)))