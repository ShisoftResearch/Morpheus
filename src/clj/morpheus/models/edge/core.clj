(ns morpheus.models.edge.core
  (:require [morpheus.utils :refer :all]
            [morpheus.models.edge.directed]
            [morpheus.models.edge.indirected]
            [morpheus.models.edge.hyper]
            [morpheus.models.edge.simple]
            [morpheus.models.edge.defined]
            [morpheus.models.edge.dynamic]
            [morpheus.models.edge.base :as eb]
            [neb.core :as neb]
            [morpheus.models.core :as core]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [neb.utils :refer [map-on-vals]]
            [morpheus.models.base :as mb]))

(defn new-edge-group! [group-name group-props]
  (let [{:keys [fields]} group-props
        require-edge-cell?  (eb/require-edge-cell? group-props)
        base-schema      (eb/edge-base-schema group-props)
        fields (when require-edge-cell? (eb/edge-schema group-props base-schema fields))]
    (core/add-model-schema :e group-name fields group-props)))

(defn edge-group-props [group] (core/get-schema :e group))

(defn link! [v1 group v2 & args]
  (let [v1-id (:*id* v1)
        v2-id (:*id* v2)
        edge-schema (edge-group-props group)
        edge-schema-id (:id edge-schema)
        v1-e-field (eb/v1-vertex-field edge-schema)
        v2-e-field (eb/v2-vertex-field edge-schema)
        type-body-sticker (eb/type-stick-body edge-schema)
        require-edge-cell? (eb/require-edge-cell? edge-schema)
        edge-cell-vertex-fields (eb/edge-cell-vertex-fields v1-id v2-id)]
    (when type-body-sticker (assert (= type-body-sticker (:body edge-schema))
                                    (str type-body-sticker " cannot with body type " (:body edge-schema))))
    (let [edge-cell-id (when require-edge-cell?
                         (apply eb/create-edge-cell
                                edge-schema
                                edge-cell-vertex-fields args))
          v1-remote (or edge-cell-id v2-id)
          v2-remote (or edge-cell-id v1-id)
          v1-cell (neb/update-cell* v1-id 'morpheus.models.edge.base/record-edge-on-vertex
                                    edge-schema-id v1-e-field)
          v2-cell (neb/update-cell* v2-id 'morpheus.models.edge.base/record-edge-on-vertex
                                    edge-schema-id v2-e-field)
          v1-list-cell-row-id (eb/extract-cell-list-id v1-cell v1-e-field edge-schema-id)
          v2-list-cell-row-id (eb/extract-cell-list-id v2-cell v2-e-field edge-schema-id)]
      (neb/update-cell* v1-list-cell-row-id 'morpheus.models.edge.base/conj-into-list-cell v1-remote)
      (neb/update-cell* v2-list-cell-row-id 'morpheus.models.edge.base/conj-into-list-cell v2-remote)
      (merge edge-cell-vertex-fields
             {:*id* edge-cell-id
              :*ep* edge-schema}))))

(defn- vertex-cid-lists [vertex & params]
  (let [seqed-params (seq params)
        params (cond
                 (or (nil? seqed-params) (coll? (first params)))
                 params
                 seqed-params
                 [(apply hash-map params)]
                 :else params)
        all-dir-fields #{:*inbounds* :*outbounds* :*neighbours*}
        regular-directions (fn [directions]
                             (or (when directions
                                   (if (vector? directions)
                                     directions [directions]))
                                 all-dir-fields))
        regular-types (fn [types]
                        (when types
                          (into #{} (map (fn [x] (core/get-schema-id :e x))
                                         (if (vector? types)
                                           types [types])))))
        params (if-not (seq params)
                 (map (fn [d] {:directions [d]}) all-dir-fields)
                 (map (fn [{:keys [type types direction directions]}]
                        {:types (regular-types (or type types))
                         :directions (regular-directions (or direction directions))})
                      params))
        expand-params (flatten (map (fn [{:keys [directions types]}]
                                        (map
                                          (fn [d]
                                            (if (seq types)
                                              (map (fn [t] {:d d :t (or t :Nil)}) types)
                                              {:d d :t :Nil}))
                                          directions))
                                      params))
        params-grouped (group-by :d expand-params)
        direction-fields (->> params-grouped (keys) (set))
        direction-types (map-on-vals
                          (fn [ps]
                            (let [t (set (map :t ps))]
                              (when-not (t :Nil) t)))
                          params-grouped)
        cid-lists (select-keys vertex direction-fields)]
    (->> (map
           (fn [[direction dir-cid-list]]
             (let [types (get direction-types direction)]
               (map
                 (fn [{:keys [sid list-cid]}]
                   (when (or (nil? types)
                             (types sid))
                     {:cid-array (:cid-array (neb/read-cell* list-cid))
                      :*direction* direction
                      :*group-props* (mb/schema-by-id sid)}))
                 dir-cid-list)))
           cid-lists)
         (flatten)
         (filter identity))))

(defn neighbours [vertex & params]
  (let [vertex-id (:*id* vertex)
        cid-lists (apply vertex-cid-lists vertex params)]
    (->> (map
           (fn [{:keys [*group-props* *direction*] :as cid-list}]
             (map
               (fn [x] (when x (eb/format-edge-cells *group-props* *direction* x)))
               (eb/edges-from-cid-array *group-props* cid-list vertex-id)))
           cid-lists)
         (flatten)
         (filter identity))))

(defn degree [vertex & params]
  (let [cid-lists (apply vertex-cid-lists vertex params)]
    (reduce + (map (comp count :cid-array) cid-lists))))

(def opposite-direction-mapper
  {:*outbounds* #{:*end*}
   :*inbounds* #{:*start*}
   :*neighbours* #{:*start* :*end*}})

(defn relationships [vertex-1 vertex-2 & params]
  (let [neighbours (apply neighbours vertex-1 params)
        v2-id (:*id* vertex-2)]
    (seq (filter (fn [{:keys [*direction*] :as edge}]
                   ((set (vals (select-keys edge (get opposite-direction-mapper *direction*)))) v2-id)) neighbours))))

(defn linked? [vertex-1 vertex-2 & params]
  (boolean (apply relationships vertex-1 vertex-2 params)))

(defn linked-degree [vertex-1 vertex-2 & params]
  (count (apply relationships vertex-1 vertex-2 params)))



(defn update-edge! [edge func-sym & params]
  (let [{:keys [*id* *ep*]} edge]
    (assert *id* "Cannot update this edge because there is no cell id for it.")
    (eb/update-edge *ep* *id* func-sym params)))

(defn unlink! [edge]
  (let [{:keys [*ep* *start* *end* *id*]} edge
        es-id (:id *ep*)
        v1-field (eb/v1-vertex-field *ep*)
        v2-field (eb/v2-vertex-field *ep*)]
    (assert (and *start* *end*) (do (spy edge) "Edge missing important info to delete"))
    (neb/update-cell* *start* 'morpheus.models.edge.base/rm-ve-relation
                      v1-field es-id (or *id* *end*))
    (neb/update-cell* *end* 'morpheus.models.edge.base/rm-ve-relation
                      v2-field es-id (or *id* *start*))
    (when *id* (eb/delete-edge-cell *ep* edge *start* *end*))))