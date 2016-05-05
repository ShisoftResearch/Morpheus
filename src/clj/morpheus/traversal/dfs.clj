(ns morpheus.traversal.dfs
  (:require [morpheus.messaging.core :as msg]
            [morpheus.models.edge.core :as edges]
            [neb.base :as nb]
            [cluster-connector.sharding.DHT :as dht]))

;; Distributed deepeth first search divised by S.A.M. Makki and George Havas

;; Message schama [vertex-id stack filter max-deepth]

(defn send-stack [task-id action vertex-id stack {:keys [filter max-deepeth]}]
  (let [server-name (nb/locate-cell-by-id vertex-id)]
    (case action
      :DFS-FORWARD (msg/send-msg server-name action [vertex-id stack filter max-deepeth] :task-id task-id)
      :DFS-RETURN  (msg/send-msg server-name action stack :task-id task-id))))

(defn proc-forward-msg [task-id data]
  (let [[vertex-id stack filter max-deepth] data
        neighbours (apply edges/neighbours vertex-id filter)
        current-vertex-stat (atom nil)
        proced-stack (map (fn [v]
                            (let [[svid] v]
                              (if (= svid vertex-id)
                                (reset! current-vertex-stat v)
                                (assoc v 1 1) ;; reset flag to visited
                                v)))
                          stack)
        deepth (@current-vertex-stat 2)
        ]
    (if (> deepth (or max-deepth Long/MAX_VALUE))
      (let [root-id (first (last stack))]
        #_(send-stack :DFS-RETURN root-id (map first stack) deepth {}))
      (if (or (empty? neighbours) )
        #_(send-stack )
        ()))))

(defn proc-return-msg [task-id data]
  )

(defn dfs [vertex-id & {:keys [filter max-deepeth] :as extra-params}]
  ;;                                   [svid      flag depth parent]
  (send-stack nil :DFS-FORWARD vertex-id `([vertex-id 0    0     nil]) extra-params))

(msg/register-action :DFS-FORWARD proc-forward-msg)
(msg/register-action :DFS-RETURN  proc-return-msg)
