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

(defn new-edge-group [group-name group-props]
  (let [{:keys [fields dynamic-fields?]} group-props]
    ))
