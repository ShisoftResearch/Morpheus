(ns morpheus.models.defined
  (:require [morpheus.models.base :as mb]
            [cluster-connector.remote-function-invocation.core :refer [compiled-cache]]
            [neb.cell :as neb-cell]))

(defn update-modeled-cell [schema-field neb-cell func-sym params]
  (let [neb-sid (:*schema* neb-cell)
        sp (mb/schema-by-neb-id neb-sid)
        cell (assoc neb-cell schema-field sp)]
    (-> (apply (compiled-cache func-sym) cell params)
        (dissoc schema-field neb-cell/internal-cell-fields))))
