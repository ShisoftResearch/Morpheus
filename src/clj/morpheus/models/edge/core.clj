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

(defn new-edge-group [group-name group-props]
  (let [{:keys [fields]} group-props
        require-edge-cell?  (eb/require-edge-cell? group-props)
        base-schema      (eb/edge-base-schema group-props)
        fields (when require-edge-cell? (eb/edge-schema group-props base-schema fields))]
    (core/add-schema :e group-name fields group-props)))

(defn edge-group-props [group] (core/get-schema :e group))

(defn create-edge [v1 group v2 & args]
  (let [v1-id (:*id* v1)
        v2-id (:*id* v2)
        edge-schema (edge-group-props group)
        edge-schema-id (:id edge-schema)
        v1-v-field (eb/v1-vertex-field edge-schema)
        v2-v-field (eb/v2-vertex-field edge-schema)
        type-body-sticker (eb/type-stick-body edge-schema)
        require-edge-cell? (eb/require-edge-cell? edge-schema)
        edge-cell-vertex-fields (eb/edge-cell-vertex-fields v1-id v2-id)]
    (when type-body-sticker (assert (= type-body-sticker (:body edge-schema))
                                    (str type-body-sticker " cannot with body type " (:body edge-schema))))
    (let [edge-cell-id (when require-edge-cell?
                         (apply eb/create-edge-cell
                                edge-schema
                                edge-cell-vertex-fields args))]
      (neb/update-cell* v1-id 'morpheus.models.edge.base/record-edge-on-vertex
                        edge-schema-id v1-v-field (or edge-cell-id v2-id))
      (neb/update-cell* v2-id 'morpheus.models.edge.base/record-edge-on-vertex
                        edge-schema-id v2-v-field (or edge-cell-id v1-id))
      (merge edge-cell-vertex-fields
             {:*id* edge-cell-id
              :*ep* edge-schema}))))

(defn vertex-cid-lists [vertex & {:keys [directions relationships]}]
  (let [direction-fields (set (or (when directions
                                    (if (vector? directions)
                                      directions [directions]))
                                  [:*inbounds* :*outbounds* :*neighbours*]))
        edge-groups (when relationships
                      (into #{}
                            (map
                              (fn [x] (core/get-schema-id :e x))
                              (if (vector? relationships)
                                relationships [relationships]))))
        cid-lists (select-keys vertex direction-fields)]
    (->> (map
           (fn [[direction dir-cid-list]]
             (when (direction-fields direction)
               (map
                 (fn [{:keys [sid list-cid]}]
                   (when (or (nil? edge-groups)
                             (edge-groups sid))
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
               (fn [x] (eb/format-edge-cells *group-props* *direction* x))
               (eb/edges-from-cid-array *group-props* cid-list vertex-id)))
           cid-lists)
         (flatten))))

(defn degree [vertex & params]
  (let [cid-lists (apply vertex-cid-lists vertex params)]
    (reduce + (map (comp count :cid-array) cid-lists))))

(defn update-edge [edge func-sym & params]
  (let [{:keys [*id* *ep*]} edge]
    (assert *id* "Cannot update this edge because there is no cell id for it.")
    (eb/update-edge *ep* *id* func-sym params)))

(defn delete-edge [edge]
  (let [{:keys [*ep* *start* *end* *id*]} edge
        es-id (:id *ep*)
        [v1-field v2-field] ((juxt eb/v1-vertex-field eb/v2-vertex-field) *ep*)]
    (assert (and *start* *end*) (do (spy edge) "Edge missing important info to delete"))
    (neb/update-cell* *start* 'morpheus.models.edge.base/rm-ve-relation
                      v1-field es-id (or *id* *end*))
    (neb/update-cell* *end* 'morpheus.models.edge.base/rm-ve-relation
                      v2-field es-id (or *id* *start*))
    (when *id* (eb/delete-edge-cell *ep* edge *start* *end*))))