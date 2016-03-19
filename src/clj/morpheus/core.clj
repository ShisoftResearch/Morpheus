(ns morpheus.core
  (:require [morpheus.models.vertex.core :as veterx]
            [morpheus.models.edge.core :as edge]
            [morpheus.models.core :as models]
            [neb.core :as neb]
            [cluster-connector.utils.for-debug :refer [$]])
  (:gen-class))

(set! *warn-on-reflection* true)

(defn shutdown-server []
  (println "Shuting down...")
  (neb/stop-server))

(defn start-server []
  (neb/start-server (read-string (slurp "configures/neb.edn")))
  (println "Initialize Models...")
  (models/init-models))

(defn -main
  "Main Entrance"
  [& args]
  (==)
  (println "Morpueus, General Purpose Graph Engine")
  (println "(C) 2016 Shisoft Research")
  (start-server)
  (.addShutdownHook
    (Runtime/getRuntime)
    (Thread. shutdown-server))
  (println "Server started"))