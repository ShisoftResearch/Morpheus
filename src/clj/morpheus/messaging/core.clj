(ns morpheus.messaging.core
  (:require [neb.core :as neb]
            [cluster-connector.networking.core :as nw]
            [cluster-connector.native-cache.core :refer [evict-cache-key defcache]]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [cluster-connector.distributed-store.core :as ds]))

(defonce server (atom nil))
(def ws-port 7177)

(def action->id-map (atom {}))
(def id->act-func-map (atom {}))

(defn act->id [act] (get @action->id-map act))
(defn id->act-func [id]  (get @id->act-func-map id))

(defn register-action [act-name func]
  (let [id (.hashCode (name act-name))]
    (swap! action->id-map assoc act-name id)
    (swap! id->act-func-map assoc id func)))

(defn server-handler [obj]
  (when-let [[task-id act data] obj]
    ((id->act-func act) task-id data)))

(defn stop-server []
  (when @server
    (.close @server)
    (reset! server nil)))

(defn start-server []
  (reset! server (nw/start-server (nw/slow-echo-handler server-handler) ws-port)))

(declare get-client)
(declare entity-get-client)

(defn get-client* [host]
  (let [c @(nw/client host ws-port)]
    (s/on-closed c (fn [] (evict-cache-key get-client host)))
    (d/let-flow
      [msg (s/take! c ::none)]
      (when (and msg (not (= ::none msg)))
        (server-handler msg)))
    c))

(defcache get-client {} get-client*)

(defn send-msg [server-name act data & {:keys [task-id]}]
  (let [task-id (or task-id (neb/rand-cell-id))
        act-id (act->id act)]
    (if (= server-name @ds/this-server-name)
      (when-let [rmsg (server-handler [task-id act-id data])]
        (server-handler rmsg))
      (s/put!
        (get-client server-name)
        [task-id act-id data]))
    task-id))