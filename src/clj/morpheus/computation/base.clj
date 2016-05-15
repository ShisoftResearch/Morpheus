(ns morpheus.computation.base
  (:require [cluster-connector.distributed-store.atom :as da]))

(def tasks (da/atom :tasks {}))

(defn new-task [id data]
  (da/swap tasks assoc id data))

(defn remove-task [id]
  (da/swap tasks id))

(defn get-task [id]
  (get @tasks id))