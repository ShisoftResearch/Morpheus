(ns morpheus.models.edge.base
  (:require [morpheus.utils :refer :all]
            [morpheus.models.base :as mb]
            [morpheus.models.core :as mc]
            [neb.core :as neb]
            [neb.cell :as neb-cell]
            [neb.header :as neb-header]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [cluster-connector.native-cache.core :refer [defcache evict-cache-key]]
            [neb.utils :refer [map-on-vals]])
  (:import (java.util UUID)
           (org.shisoft.neb Trunk)
           (org.shisoft.neb.io type_lengths CellMeta Reader)))

(def schema-fields
  [[:*start*  :cid]
   [:*end*  :cid]])

(defmulties
  :type
  (edge-base-schema [])
  (v1-vertex-field [])
  (v2-vertex-field [])
  (type-stick-body [])
  (vertex-fields [])
  (delete-edge-cell [edge start end]))

(defmulties
  :body
  (update-edge [id func-sym params])
  (delete-edge [])
  (base-schema [])
  (require-edge-cell? [])
  (edge-schema [base-schema fields])
  (create-edge-cell [vertex-fields & args])
  (edges-from-cid-array [cid-list & [start-vertex]]))

(defn edge-cell-vertex-fields [v1 v2]
  {:*start* v1
   :*end* v2})

(def vertex-fields #{:*start* :*end*})

(def max-list-size (-> (Trunk/getMaxObjSize)
                       (- neb-header/cell-head-len)
                       (/ type_lengths/cidLen)
                       (Math/floor) (int)
                       (dec)))

(defn extract-edge-cid-list [cid-lists edge-schema-id]
  (first (filter
           (fn [item]
             (= edge-schema-id (:sid item)))
           cid-lists)))

(defn extract-cell-list-id [vertex field edge-schema-id]
  (let [cid-lists (get vertex field)]
    (:list-cid (extract-edge-cid-list cid-lists edge-schema-id))))

(defn cid-list-id-by-vertex [v-id field edge-schema-id]
  (neb/cell-id-by-key (str v-id "-" field "-" edge-schema-id)))

(def empty-cid (UUID. 0 0))

(defn format-edge-cells [group-props direction edge]
  (let [pure-edge (dissoc edge :*schema* :*hash*)]
    (merge pure-edge
           {:*ep* group-props
            :*direction* direction})))

(defmacro with-cid-list [& body]
  `(let [^Trunk ~'trunk neb-cell/*cell-trunk*
         ^Integer ~'hash neb-cell/*cell-hash*
         ^CellMeta ~'meta neb-cell/*cell-meta*
         ~'next-cid (neb-cell/get-in-cell ~'trunk ~'hash [:next-list])
         ~'next-cid (when-not (= ~'next-cid empty-cid) ~'next-cid)]
     ~@body))

(defn read-cid-list-len []
  (Reader/readInt
    (+ (.getLocation neb-cell/*cell-meta*)
       type_lengths/cidLen
       neb-header/cell-head-len)))

(defmacro def-cid-append-op [func-name params & body]
  `(defn ~func-name ~params
     (with-cid-list
       (let [~'move-to-list-with-params (fn [cid-id# params#]
                                          (apply neb/write-lock-exec* cid-id#
                                                 (quote ~(symbol (str "morpheus.models.edge.base/"
                                                                      (name func-name))))
                                                 params#))
             ~'move-to-list (fn [next-cid#] (~'move-to-list-with-params next-cid# ~params))
             ~'list-length (read-cid-list-len)]
         ~@body))))

(defn new-list-cell []
  (let [cell-id (neb/new-cell-by-ids (neb/rand-cell-id) @mb/cid-list-schema-id
                                     {:next-list empty-cid :cid-array []})]
    (neb-cell/update-cell*
      neb-cell/*cell-trunk* neb-cell/*cell-hash*
      (fn [cid-list]
        (assoc cid-list :next-list cell-id)))
    cell-id))

(def-cid-append-op
  append-cid-to-list* [target-cid]
  (if (< list-length max-list-size)
    (neb-cell/update-cell*
      trunk hash
      (fn [list-cell]
        (update list-cell :cid-array conj target-cid)))
    (move-to-list (or next-cid (new-list-cell)))))

(defn append-cid-to-list [head-cid target-cid]
  (neb/write-lock-exec* head-cid 'morpheus.models.edge.base/append-cid-to-list* target-cid))

(def-cid-append-op
  append-cids-to-list* [target-cids]
  (when (not (empty? target-cids))
    (if (< list-length max-list-size)
      (let [cids-num-to-go (- max-list-size list-length)
            cids-to-go (take cids-num-to-go target-cids)]
        (neb-cell/update-cell*
          trunk hash
          (fn [list-cell]
            (update list-cell :cid-array concat cids-to-go)))
        (when-not (= cids-to-go target-cids)
          (move-to-list-with-params (or next-cid (new-list-cell)) [(subvec (vec target-cids) cids-num-to-go)])))
      (move-to-list (or next-cid (new-list-cell))))))

(defn append-cids-to-list [head-cid target-cids]
  (neb/write-lock-exec* head-cid 'morpheus.models.edge.base/append-cids-to-list* target-cids))

(defn remove-cid-from-list* [target-cid]
  (with-cid-list
    (let [{:keys [cid-array] :as list-cell} (neb-cell/read-cell trunk hash)
          removed-list (remove-first #(= % target-cid) cid-array)]
      (if (= removed-list cid-array)
        (neb/write-lock-exec* next-cid 'morpheus.models.edge.base/remove-cid-from-list* target-cid)
        (neb-cell/replace-cell* trunk hash (assoc list-cell :cid-array removed-list))))))

(defn remove-cid-from-list [head-cid target-cid]
  (neb/write-lock-exec* head-cid 'morpheus.models.edge.base/remove-cid-from-list* target-cid))

(defn record-edge-on-vertex [vertex edge-schema-id field & ]
  (let [cid-list-row-id (extract-cell-list-id vertex field edge-schema-id)
        list-cell-id (or cid-list-row-id
                         (neb/new-cell-by-ids
                           (cid-list-id-by-vertex (:*id* vertex) field edge-schema-id)
                           @mb/cid-list-schema-id
                           {:next-list empty-cid :cid-array []}))]
    (if-not cid-list-row-id
      (update vertex field conj {:sid edge-schema-id :list-cid list-cell-id})
      vertex)))

(defn vertex-edge-list [[vertex-id direction schema-id]]
  (let [id (cid-list-id-by-vertex vertex-id direction schema-id)]
    (when-not (neb/cell-exists?* id)
      (neb/update-cell* vertex-id 'morpheus.models.edge.base/record-edge-on-vertex
                        schema-id direction))
    id))

(defn rm-ve-relation [vertex direction es-id target-cid]
  (let [cid-list-cell-id (->> (get vertex direction)
                              (filter (fn [m] (= es-id (:sid m))))
                              (first) (:list-cid))]
    (remove-cid-from-list cid-list-cell-id target-cid)
    vertex))

(defn remove-list-chain* []
  (with-cid-list
    (when next-cid (neb/write-lock-exec* next-cid 'morpheus.models.edge.base/remove-list-chain*))
    (neb-cell/delete-cell trunk hash)))

(defn remove-list-chain [head-cid]
  (neb/write-lock-exec* head-cid 'morpheus.models.edge.base/remove-list-chain*))

(defn remove-vertex-edge-list-chains [vertex]
  (let [lists (mapcat #(get vertex %) [:*inbounds* :*outbounds* :*neighbours*])
        list-ids (map :list-cid lists)]
    (dorun (map remove-list-chain list-ids))))

(defn extract-cid-lists [direction sid vertex-id filters]
  (with-cid-list
    (let [{:keys [cid-array] :as list-cell} (neb-cell/read-cell trunk hash)]
      (concat [{:cid-array cid-array
                :*direction* direction
                :*group-props* (mb/schema-by-id sid)}]
              (when next-cid
                (neb/read-lock-exec*
                  next-cid
                  'morpheus.models.edge.base/extract-cid-lists
                  direction sid vertex-id filters))))))

(defn get-oppisite [edge vertex-id]
  (let [{:keys [*start* *end*]} edge]
    (cond (and (= vertex-id *start*) (not= vertex-id *end*))   *end*
          (and (= vertex-id *end*)   (not= vertex-id *start*)) *start*)))

(defn- vertex-cid-lists [vertex read-list-sym & params]
  (let [vertex-id (:*id* vertex)
        seqed-params (seq params)
        params (cond
                 (or (nil? seqed-params) (map? (first params)))
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
                          (into #{} (map (fn [x] (mc/get-schema-id :e x))
                                         (if (vector? types)
                                           types [types])))))
        params (if-not (seq params)
                 (map (fn [d] {:directions [d]}) all-dir-fields)
                 (map (fn [{:keys [type types direction directions filters]}]
                        {:types (regular-types (or type types))
                         :directions (regular-directions (or direction directions))
                         :filters filters})
                      params))
        expand-params (flatten (map (fn [{:keys [directions types filters]}]
                                      (map
                                        (fn [d]
                                          (if (seq types)
                                            (map (fn [t] {:d d :t (or t :Nil) :f filters}) types)
                                            {:d d :t nil :f filters}))
                                        directions))
                                    params))
        params-grouped (group-by :d expand-params)
        direction-fields (->> params-grouped (keys) (set))
        direction-items (map-on-vals
                          (fn [ps]
                            (let [item (set (map (fn [{:keys [t f]}]
                                                   (if t [t f] :Nil))
                                                 ps))]
                              (when-not (item :Nil) item)))
                          params-grouped)
        direction-types (map-on-vals
                          (fn [items] (set (map first items)))
                          direction-items)
        cid-lists (select-keys vertex direction-fields)]
    (->> (map
           (fn [[direction dir-cid-list]]
             (let [items (get direction-items direction)
                   types (get direction-types direction)]
               (map
                 (fn [{:keys [sid list-cid]}]
                   (when (or (nil? items)
                             (types sid))
                     ;{:cid-array (:cid-array (neb/read-cell* list-cid))
                     ; :*direction* direction
                     ; :*group-props* (mb/schema-by-id sid)}
                     (neb/read-lock-exec*
                       list-cid read-list-sym
                       direction sid vertex-id
                       (map second
                            (filter (fn [[t f]] (and (= t sid) (identity t) (identity f)))
                                    items)))))
                 dir-cid-list)))
           cid-lists)
         (flatten)
         (filter identity))))

(defn neighbours [vertex & params]
  (apply vertex-cid-lists vertex 'morpheus.models.edge.remotes/neighbours* params))

(defn neighbours-edges [vertex & params]
  (apply vertex-cid-lists vertex 'morpheus.models.edge.remotes/neighbours-edges* params))

(defn degree [vertex & params]
  (reduce + (apply vertex-cid-lists vertex 'morpheus.models.edge.remotes/count-edges params)))