(ns morpheus.models.edge.core
  (:require [morpheus.utils :refer :all]
            [morpheus.models.edge.directed]
            [morpheus.models.edge.indirected]
            [morpheus.models.edge.hyper]
            [morpheus.models.edge.simple]
            [morpheus.models.edge.defined]
            [morpheus.models.edge.dynamic]
            [morpheus.models.edge.base :as eb]
            [neb.core :as neb]
            [morpheus.models.core :as core]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(defn new-edge-group [group-name group-props]
  (let [{:keys [fields]} group-props
        require-schema?  (eb/require-schema? group-props)
        base-schema      (eb/edge-base-schema group-props)
        fields (when require-schema? (eb/edge-schema group-props base-schema fields))]
    (core/add-schema :e group-name fields group-props)))

(defn edge-group-props [group] (core/get-schema :e group))
