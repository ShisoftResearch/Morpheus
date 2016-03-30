(ns morpheus.core
  (:require [morpheus.models.vertex.core :as veterx]
            [morpheus.models.edge.core :as edge]
            [morpheus.models.core :as models]
            [neb.server :as nserver]
            [cluster-connector.utils.for-debug :refer [$]])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn shutdown-server []
  (println "Shuting down...")
  (nserver/stop-server))

(defn start-server []
  (nserver/start-server (read-string (slurp "configures/neb.edn")))
  (println "Initialize Models...")
  (models/init-models))

(defn -main
  "Main Entrance"
  [& args]
  (println "Morpueus, General Purpose Graph Engine")
  (println "(C) 2016 Shisoft Research")
  (start-server)
  (.addShutdownHook
    (Runtime/getRuntime)
    (Thread. shutdown-server))
  (println "Server started"))