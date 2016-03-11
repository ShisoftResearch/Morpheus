(ns morpheus.core
  (:require [morpheus.models.vertex.core :as veterx]
            [morpheus.models.edge.core :as edge]
            [morpheus.models.core :as models]
            [neb.core :as neb]
            [cluster-connector.utils.for-debug :refer [$]])
  (:gen-class))

(defn shutdown []
  (println "Shuting down...")
  (models/save-models))

(defn -main
  "Main Entrance"
  [& args]
  (println "Morpueus, General Purpose Graph Engine")
  (println "(C) 2016 Shisoft Research")
  (neb/start-server (read-string (slurp "configures/neb.edn")))
  (println "Initialize Models...")
  (models/init-models)
  (.addShutdownHook
    (Runtime/getRuntime)
    (Thread. shutdown))
  (println "Server started"))
