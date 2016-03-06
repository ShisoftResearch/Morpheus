(ns morpheus.models.core
  (:require [morpheus.models.vertex.core  :as v]
            [morpheus.models.edge.core    :as e]
            [cluster-connector.distributed-store.lock :as dl]
            [cluster-connector.distributed-store.atom :as da]
            [cluster-connector.distributed-store.core :as ds]
            [neb.core :as neb]
            [neb.types :as neb-types])
  (:import (org.shisoft.neb.io reader writer type_lengths)))

(dl/deflock models-init)
(def schemas (da/atom :schamas))

(defn init-models []
  (dl/locking
    models-init
    (when (ds/is-first-node?)
      (neb/add-schema :relations [[:sid :int] [:list-cid :cid]])
      (neb/add-schema :cid-list  [[:cid-array]]))))

(defn new-vertex-schema [vname fields]
  (let [neb-schema-name (str "veterx-" (name vname))
        neb-fields (concat v/vertex-schema-fields fields)
        neb-schema-id (neb/add-schema neb-schema-name neb-fields)]
    (patom/swap
      schemas 'clojure.core/assoc neb-schema-id
      {:id neb-schema-id
       :name vname})))