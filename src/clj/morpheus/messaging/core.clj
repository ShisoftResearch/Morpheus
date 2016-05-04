(ns morpheus.messaging.core
  (:require [morpheus.traversal.dfs :as dfs]
            [cluster-connector.networking.core :as nw]
            [cluster-connector.remote-function-invocation.core :as rfi]
            [cluster-connector.native-cache.core :refer [evict-cache-key defcache]]
            [taoensso.nippy :as nippy]
            [manifold.stream :as s]
            [manifold.deferred :as d]
            [clojure.core.async :as a]
            [cluster-connector.distributed-store.core :as ds]))

(defonce server (atom nil))
(def ws-port 71771)
(def tasks (atom {}))

(def act-mapper
  [0 :DFS-FORWARD dfs/proc-forward-msg nil]
  [1 :DFS-RETURN  dfs/proc-return-msg nil])

(defmacro gen-act-mapper []
  `(do (def id-act-map []
         ~(into {} (map (fn [[id kw]] [id kw]) act-mapper)))
       (def act-id-map []
         ~(into {} (map (fn [[id kw]] [id kw]) act-mapper)))
       (def id-act-func-map []
         ~(into {} (map (fn [[id _ func]] [id func]) act-mapper)))
       (def id-act-return-func-map []
         ~(into {} (map (fn [[id _ _ rfunc]] [id rfunc]) act-mapper)))))

(defn act->id [act] (get act-id-map act))
(defn id->act [id]  (get id-act-map id))
(defn id->act-func [id]  (get id-act-func-map id))
(defn id->act-rfunc [id]  (get id-act-return-func-map id))

(defn server-handler [obj]
  (when-let [[task-id act data] obj]
    ((id->act-func act) task-id data)))

(defn stop-server []
  (when @server
    (.close @server)
    (reset! server nil)))

(defn start-server []
  (reset! server (nw/start-server (nw/slow-echo-handler server-handler) ws-port))
  (.addShutdownHook
    (Runtime/getRuntime)
    (Thread. (fn [] (try (stop-server) (catch Exception _))))))

(declare get-client)

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
  (let [task-id (or task-id (- (System/currentTimeMillis) (rand)))
        act-id (act->id act)]
    (if (= server-name @ds/this-server-name)
      (when-let [rmsg (server-handler [task-id act-id data])]
        (server-handler rmsg))
      (s/put!
        (get-client server-name)
        [task-id act-id data]))
    task-id))