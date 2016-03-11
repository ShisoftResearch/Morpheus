(ns morpheus.models.vertex.core
  (:require [morpheus.utils :refer :all]
            [morpheus.models.base :refer [schemas]]
            [morpheus.models.vertex.defined]
            [morpheus.models.vertex.dynamic]
            [morpheus.models.vertex.base :as vb]
            [neb.core :as neb]
            [cluster-connector.distributed-store.atom :as dsatom]))

(defn new-vertex-group [group group-props]
  (let [{:keys [fields dynamic-fields?]} group-props
        fields (if dynamic-fields? vb/dynamic-veterx-schema-fields (or fields []))
        neb-schema-name (str "veterx-" (name group))
        neb-fields (concat vb/vertex-schema-fields fields)
        neb-schema-id (neb/add-schema neb-schema-name neb-fields)]
    (dsatom/swap
      schemas assoc neb-schema-id
      (merge group-props
             {:id neb-schema-id
              :name group}))))

(defn fetch-group-props [group] (get @schemas group))

(defmacro wrap-base-ops [op]
  ;TODO This can be better for performance by avoid using apply
  `(defn ~op [group# & args#]
     (let [props# (fetch-group-props group#)]
       (apply ~(symbol "morpheus.models.vertex.base" (name op)) props# args#))))

(wrap-base-ops get-veterx)
(wrap-base-ops reset-veterx)
(wrap-base-ops new-veterx)
(wrap-base-ops update-in-veterx)