(ns morpheus.computation.base
  (:require [cluster-connector.distributed-store.atom :as da]))

(def tasks (da/atom :tasks {}))

(defn new-taks [id data]
  (da/swap tasks assoc id data))

(defn remove-task [id]
  (da/swap tasks id))