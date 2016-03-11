(ns morpheus.models.base
  (:require [cluster-connector.distributed-store.atom :as da]))

(def schemas (da/atom :schemas))