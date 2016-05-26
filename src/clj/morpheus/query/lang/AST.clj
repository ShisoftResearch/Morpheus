(ns morpheus.query.lang.AST
  (:require [morpheus.query.lang.base :as base]
            [cluster-connector.utils.for-debug :refer [$ spy]]))

(defn parse-map-path [path]
  (let [path (cond
               (string? path) path
               (keyword? path) (apply str (rest (str path)))
               (symbol? path) (str path))]
    (map
      (fn [token]
        (if (re-matches #"[0-9]" token)
          (read-string token)
          (keyword token)))
      (clojure.string/split path #"\|"))))

(defn eval-with-data* [data s-exp]
  (if (list? s-exp)
    (let [func-sym (peek s-exp)
          params (rest s-exp)]
      (apply
        (get base/op-mapper func-sym)
        (map (partial eval-with-data* data) params)))
    (cond
      (symbol? s-exp)
      (get base/op-mapper s-exp)
      (keyword? s-exp)
      (get-in data (parse-map-path s-exp))
      :else
      s-exp)))

(defn eval-with-data [data s-exp-or-with-params]
  (let [s-exp (if (vector? s-exp-or-with-params)
                (first s-exp-or-with-params) s-exp-or-with-params)
        params (if (vector? s-exp-or-with-params)
                 (second s-exp-or-with-params) {})]
    (eval-with-data*
      (merge data params) s-exp)))