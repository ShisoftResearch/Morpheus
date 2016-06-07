(ns morpheus.query.lang.evaluation
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

(defn eval-with-data* [s-exp]
  (let [data base/*data*]
    (if (list? s-exp)
      (let [func-sym (peek s-exp)
            params (rest s-exp)
            function (get base/function-mapper func-sym)
            interpreter (get base/interpreter-mapper func-sym)]
        (cond
          function
          (apply function (map eval-with-data* params))
          interpreter
          (apply interpreter params)))
      (cond
        (symbol? s-exp)
        (get base/function-mapper s-exp)
        (keyword? s-exp)
        (get-in data (parse-map-path s-exp))
        :else
        s-exp))))

(defn eval-with-data [data s-exp-or-with-params]
  (let [s-exp (if (vector? s-exp-or-with-params)
                (first s-exp-or-with-params) s-exp-or-with-params)
        params (if (vector? s-exp-or-with-params)
                 (second s-exp-or-with-params) {})]
    (with-bindings {#'base/*data* (merge data params)}
      (eval-with-data* s-exp))))

(defn let*- [bindings & body]
  (with-bindings
    {#'base/*data*
     (merge
       base/*data*
       (into
         {}
         (map
           (fn [[k exp]]
             [(keyword (name k))
              (eval-with-data* exp)])
           (partition 2 bindings))))}
    (last (map eval-with-data* body))))

(defn let- [bindings & body]
  (let [data (atom base/*data*)]
    (doseq [[k exp] (partition 2 bindings)]
      (with-bindings
        {#'base/*data* @data}
        (swap! data assoc (keyword (name k)) (eval-with-data* exp))))
    (with-bindings
      {#'base/*data* @data}
      (last (map eval-with-data* body)))))