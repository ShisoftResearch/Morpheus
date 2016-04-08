(ns morpheus.models.core
  (:require [morpheus.models.base :refer [schema-id-by-sname schema-sname-exists? schema-by-sname init-base-models]]
            [cluster-connector.distributed-store.lock :as dl]
            [cluster-connector.distributed-store.core :as ds]
            [cluster-connector.remote-function-invocation.core :as rfi]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [neb.core :as neb]
            [neb.schema :as neb-schema])
  (:import (org.shisoft.neb.exceptions SchemaAlreadyExistsException)))

(def schema-file "configures/neb-schemas.edn")

(defn init-models []
  (init-base-models))

(dl/deflock morph-schemas-lock)

(defn get-schema-name [stype group-name]
  (str (name stype) "-" (name group-name)))

(defn add-model-schema [stype group-name fields meta]
  (dl/locking
    morph-schemas-lock
    (let [schema-name (get-schema-name stype group-name)]
      (if (schema-sname-exists? schema-name)
        (throw (SchemaAlreadyExistsException. (str stype " - " group-name)))
        (let [neb-schema-id (when fields (neb/add-schema schema-name (vec fields)))
              meta (merge meta {:name group-name
                                :neb-sid neb-schema-id
                                :stype stype})]
          (rfi/condinated-invoke-with-selection
            ['morpheus.models.base/gen-schema-id nil]
            ['morpheus.models.base/add-schema [schema-name neb-schema-id '<> meta]] max))))))

(defn add-schema [schema-name fields]
  (neb/add-schema schema-name fields))

(defn get-schema [stype group-name]
  (let [schema-name (get-schema-name stype group-name)]
    (schema-by-sname schema-name)))

(defn get-schema-id [stype group-name]
  (schema-id-by-sname (get-schema-name stype group-name)))