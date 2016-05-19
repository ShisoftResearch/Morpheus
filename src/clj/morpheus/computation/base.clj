(ns morpheus.computation.base
  (:require [cluster-connector.distributed-store.synced-atom :as da]))

(da/defatom tasks {})

(defn new-task [id data]
  (da/swap tasks assoc id data))

(defn remove-task [id]
  (da/swap tasks dissoc id))

(defn get-task [id]
  (get @tasks id))