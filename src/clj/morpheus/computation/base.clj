(ns morpheus.computation.base
  (:require [cluster-connector.distributed-store.synced-atom :as da]
            [com.climate.claypoole :as cp]))

(da/defatom tasks {})

(defn new-task [id data]
  (da/swap tasks assoc id data))

(defn remove-task [id]
  (da/swap tasks dissoc id))

(defn get-task [id]
  (get @tasks id))

(def compution-threadpool (cp/threadpool (cp/ncpus) :name "graph-compute"))