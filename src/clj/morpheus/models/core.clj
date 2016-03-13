(ns morpheus.models.core
  (:require [morpheus.models.base :refer [schema-sname-exists?]]
            [cluster-connector.distributed-store.lock :as dl]
            [cluster-connector.distributed-store.core :as ds]
            [cluster-connector.remote-function-invocation.core :as rfi]
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

(dl/deflock schemas-lock)

(defn add-schema [stype group-name fields meta]
  (dl/locking
    schema-lock
    (let [schema-name (str (name stype) "-" (name group-name))]
      (if (schema-sname-exists? schema-name)
        (throw (SchemaAlreadyExistsException.))
        (let [neb-schema-id (when fields (neb/add-schema schema-name fields))
              meta (merge meta {:name group-name
                                :neb-id neb-schema-id
                                :type stype})]
          (rfi/condinated-invoke-with-selection
            ['morpheus.models.base/gen-id nil]
            ['morpheus.models.base/add-schema [schema-name neb-schema-id '<> meta]] max))))))