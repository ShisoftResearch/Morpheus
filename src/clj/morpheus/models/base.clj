(ns morpheus.models.base
  (:require [cluster-connector.distributed-store.atom :as da]
            [neb.core :as neb]
            [cluster-connector.distributed-store.core :as ds]
            [cluster-connector.distributed-store.lock :as dl])
  (:import (org.shisoft.morpheus schemaStore)
           (org.shisoft.neb.exceptions SchemaAlreadyExistsException)))

(def schema-store (schemaStore.))
(def cid-list-schema-id (atom nil))

(defn add-schema [sname neb-id id meta]
  (.put schema-store id neb-id sname (assoc meta :id id)))

(defn schema-by-id [^Integer id]
  (.getById schema-store id))

(defn schema-by-neb-id [^Integer id]
  (.nebId2schema schema-store id))

(defn schema-id-by-sname [sname]
  (.sname2Id schema-store sname))

(defn schema-by-sname [sname]
  (schema-by-id (schema-id-by-sname sname)))

(defn schema-sname-exists? [sname]
  (.snameExists schema-store sname))

(defn clear-schema []
  (.clear schema-store))

(defn gen-schema-id []
  (locking schema-store
    (let [existed-ids (sort (keys (.getSchemaIdMap schema-store)))
          ids-range   (range)]
      (loop [e-ids existed-ids
             r-ids ids-range]
        (if(not= (first e-ids) (first r-ids))
          (first r-ids)
          (recur (rest e-ids)
                 (rest r-ids)))))))

(defn cell-id-by-key [st vp key]
  (let [{:keys [neb-sid name]} vp]
    (neb/cell-id-by-key (str (name st) "-" (str neb-sid) "-" (str name) "-" (str key)))))

(defn cell-id-by-data [st vp data]
  (let [{:keys [key-field]} vp]
    (if key-field
      (cell-id-by-key st vp (get data key-field))
      (neb/rand-cell-id))))

(dl/deflock models-init)

(defn init-base-models []
  (dl/locking
    models-init
    (when (ds/is-first-node?)
      (try
        (neb/add-schema :relations [[:sid :int] [:list-cid :cid]])
        (neb/add-schema :cid-list  [[:cid-array :cid-array]])
        (catch SchemaAlreadyExistsException _))))
  (reset! cid-list-schema-id (neb/schema-id-by-sname :cid-list)))