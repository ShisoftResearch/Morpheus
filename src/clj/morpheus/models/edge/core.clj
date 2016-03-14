(ns morpheus.models.edge.core
  (:require [morpheus.utils :refer :all]
            [morpheus.models.edge.directed]
            [morpheus.models.edge.indirected]
            [morpheus.models.edge.hyper]
            [morpheus.models.edge.simple]
            [morpheus.models.edge.defined]
            [morpheus.models.edge.dynamic]
            [morpheus.models.edge.base :as vb]
            [neb.core :as neb]
            [morpheus.models.core :as core]))

(defn new-edge-group [group-name group-props]
  (let [{:keys [fields]} group-props
        require-schema?  (vb/require-schema? group-props)
        base-schema      (vb/edge-base-schema group-props)
        fields (when require-schema? (vb/edge-schema base-schema fields))]
    (core/add-schema :e group-name fields group-props)))
