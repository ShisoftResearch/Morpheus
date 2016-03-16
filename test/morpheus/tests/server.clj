(ns morpheus.tests.server
  (:require [midje.sweet :refer :all]
            [morpheus.models.base :refer [clear-schema]]
            [morpheus.core :refer [start-server shutdown-server]]
            )
  (:import (java.io File)))

(defn remove-server-files []
  (.delete (File. "configures/neb-schemas.edn"))
  (clear-schema))

(defmacro with-server [& body]
  `(do (remove-server-files)
       (fact "Start Server"
             (start-server) => anything)
       (try
         ~@body
         (catch Exception ex#
           (clojure.stacktrace/print-cause-trace ex#))
         (finally
           (fact "Stop Server"
                 (shutdown-server) => anything)
           (remove-server-files)))))
