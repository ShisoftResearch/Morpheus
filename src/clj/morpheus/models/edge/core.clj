(ns morpheus.models.edge.core
  (:require [morpheus.utils :refer :all]
            [morpheus.models.edge.directed]
            [morpheus.models.edge.indirected]
            [morpheus.models.edge.hyper]
            [morpheus.models.edge.simple]
            [morpheus.models.edge.defined]
            [morpheus.models.edge.dynamic]
            [morpheus.models.base :refer [schemas]]
            [neb.core :as neb]))

(defmulties
  :type
  (neighbours [])
  (inboundds [])
  (outbounds [])
  (neighbours [relationship])
  (inboundds [relationship])
  (outbounds [relationship]))

(defmulties
  :body
  (get-edge [])
  (update-edge [new-edge])
  (delete-edge [])
  (base-schema []))

#_(defn new-edge-schema [edge-name edge-props relationship & [fields]]
  (let [neb-schema-name (str "edge-" (name edge-name))
        edge-props (merge edge-props
                          {:rel relationship})
        neb-schema-id (neb/add-schema neb-schema-name neb-fields)]
    (patom/swap
      schemas 'clojure.core/assoc neb-schema-id
      {:id neb-schema-id
       :name vname})))
