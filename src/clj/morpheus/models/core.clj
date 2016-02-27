(ns morpheus.models.core
  (:require [morpheus.models.vertex.core  :as v]
            [morpheus.models.edge.core    :as e]
            [cluster-connector.paxos.atom :as patom]
            [neb.core :as neb]
            [neb.types :as neb-types])
  (:import (org.shisoft.neb.io reader writer type_lengths)))

(patom/defatom schemas {})

(neb-types/new-custom-data-type
  :edge-list-pointer
  {:id (int 1000) :dynamic? true
   :reader (fn [trunk offset]
             (let [edge-schema   (reader/readInt trunk offset)
                   edge-ids-pos  (+ offset# type_lengths/intLen)]
               {:id edge-schema
                :list ((get-in data-types [:cid-array :reader]) trunk edge-ids-pos)}))
   :writer (fn [trunk value offset]
             )})

(defn new-vertex-schema [vname fields]
  (let [neb-schema-name (str "veterx-" (name vname))
        neb-fields (concat v/vertex-schema-fields fields)
        neb-schema-id (neb/add-schema neb-schema-name neb-fields)]
    (patom/swap
      schemas 'clojure.core/assoc
      neb-schema-id
      {:id neb-schema-id
       :name vname})))