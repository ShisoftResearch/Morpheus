(ns morpheus.models.base
  (:require [cluster-connector.distributed-store.atom :as da])
  (:import (org.shisoft.morpheus schemaStore)))

(def schema-store (schemaStore.))

(defn add-schema [sname neb-id id meta]
  (.put schema-store id neb-id sname meta))

(defn schema-by-id [^Integer id]
  (.getById schema-store id))

(defn schema-id-by-sname [sname]
  (.sname2Id schema-store sname))

(defn schema-by-sname [sname]
  (schema-by-id (schema-id-by-sname sname)))

(defn schema-sname-exists? [sname]
  (.snameExists schema-store sname))

(defn clear-schema []
  (.clear schema-store))

(defn gen-id []
  (locking schema-store
    (let [existed-ids (sort (keys (.getSchemaIdMap schema-store)))
          ids-range   (range)]
      (loop [e-ids existed-ids
             r-ids ids-range]
        (if(not= (first e-ids) (first r-ids))
          (first r-ids)
          (recur (rest e-ids)
                 (rest r-ids)))))))