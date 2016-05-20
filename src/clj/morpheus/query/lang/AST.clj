(ns morpheus.query.lang.AST
  (:require [cluster-connector.utils.for-debug :refer [$ spy]]))

(defn parse-map-path [path]
  (let [path (cond
               (string? path) path
               (keyword? path) (apply str (rest (str path)))
               (symbol? path) (str path))]
    (map keyword (clojure.string/split path #"/"))))

(defn eval [s-exp data]
  )