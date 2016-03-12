(ns morpheus.models.core
  (:require [morpheus.models.vertex.core  :as v]
            [morpheus.models.edge.core    :as e]
            [morpheus.models.base :refer [schemas]]
            [cluster-connector.distributed-store.lock :as dl]
            [cluster-connector.distributed-store.core :as ds]
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