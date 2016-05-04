(ns morpheus.traversal.dfs
  (:require [morpheus.messaging.core :as msg]
            [neb.base :as nb]
            [cluster-connector.sharding.DHT :as dht]))

;; Distributed deepeth first search divised by S.A.M. Makki and George Havas

;; Message schama [stack root-id filter]

(defn send-stack [root-id vertex-id action stack filter]
  (let [server-name (nb/locate-cell-by-id vertex-id)]
    (msg/send-msg server-name action [stack root-id filter])))

(defn proc-forward-msg [task-id data]
  )

(defn proc-return-msg [task-id data]
  )

(defn dfs [])