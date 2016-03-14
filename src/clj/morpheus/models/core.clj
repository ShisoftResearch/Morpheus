(ns morpheus.models.core
  (:require [morpheus.models.base :refer [schema-sname-exists? schema-by-sname]]
            [cluster-connector.distributed-store.lock :as dl]
            [cluster-connector.distributed-store.core :as ds]
            [cluster-connector.remote-function-invocation.core :as rfi]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [neb.core :as neb]
            [neb.schema :as neb-schema])
  (:import (org.shisoft.neb.io reader writer type_lengths)
           (org.shisoft.neb.exceptions SchemaAlreadyExistsException)))

(dl/deflock models-init)
(def schema-file "configures/neb-schemas.edn")

(defn init-models []
  (dl/locking
    models-init
    (when (ds/is-first-node?)
      (try
        (neb/add-schema :relations [[:sid :int] [:list-cid :cid]])
        (neb/add-schema :cid-list  [[:cid-array]])
        (catch SchemaAlreadyExistsException _)))))

(dl/deflock morph-schemas-lock)

(defn get-schema-name [stype group-name]
  (str (name stype) "-" (name group-name)))

(defn add-schema [stype group-name fields meta]
  (dl/locking
    morph-schemas-lock
    (let [schema-name (get-schema-name stype group-name)]
      (if (schema-sname-exists? schema-name)
        (throw (SchemaAlreadyExistsException.))
        (let [neb-schema-id (when fields (neb/add-schema schema-name (vec fields)))
              meta (merge meta {:name group-name
                                :neb-sid neb-schema-id
                                :stype stype})]
          (rfi/condinated-invoke-with-selection
            ['morpheus.models.base/gen-id nil]
            ['morpheus.models.base/add-schema [schema-name neb-schema-id '<> meta]] max))))))

(defn get-schema [stype group-name]
  (let [schema-name (get-schema-name stype group-name)]
    (schema-by-sname schema-name)))