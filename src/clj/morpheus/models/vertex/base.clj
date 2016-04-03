(ns morpheus.models.vertex.base
  (:require [morpheus.utils :refer :all]
            [neb.core :as neb]
            [morpheus.models.core :as core]
            [morpheus.models.edge.core :as e]
            [morpheus.models.edge.base :as eb]
            [cluster-connector.utils.for-debug :refer [spy $]]
            [neb.cell :as neb-cell]
            [neb.trunk-store :as neb-ts]
            [morpheus.models.base :as mb])
  (:import (org.shisoft.morpheus.exceptions GroupNotFoundException)))

(def vertex-relation-fields
  [[:*inbounds*     [:ARRAY :relations]]
   [:*outbounds*    [:ARRAY :relations]]
   [:*neighbours*   [:ARRAY :relations]]])

(def vertex-relation-field-keys
  (map first vertex-relation-fields))

(defmulties
  :body
  (assemble-vertex [neb-cell])
  (new-vertex [data])
  (update-vertex [id func-sym params])
  (cell-fields [fields]))

(defn veterx-group-props [group] (or (core/get-schema :v group) (throw (GroupNotFoundException. (str group)))))

(defn reset-vertex-cell-map [vertex value]
  (merge value (select-keys vertex vertex-relation-field-keys)))

(defn delete-vertex* [vertex]
  "It should been called from write-lock-exec in neb"
  (let [v-id (:*id* vertex)]
    (doseq [{:keys [*ep* *direction* *start* *end* *id*] :as neighbour} (e/neighbours vertex)]
      (let [es-id (:id *ep*)
            target-id (or *id* v-id)
            remote-direction (case *direction*
                               :*inbounds* :*outbounds*
                               :*outbounds* :*inbounds*
                               :*neighbours* :*neighbours*)
            remote-vertex-id (if (or (= *start* target-id)
                                     (= *start* v-id)) *end* *start*)]
        (neb/update-cell* remote-vertex-id 'morpheus.models.edge.base/rm-ve-relation
                          remote-direction es-id target-id)
        (when *id* (eb/delete-edge-cell *ep* neighbour *start* *end*))))
    (mb/try-invoke-local-neb-cell
      neb-cell/delete-cell neb-ts/delete-cell vertex)))