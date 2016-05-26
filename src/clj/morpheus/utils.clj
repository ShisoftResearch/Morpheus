(ns morpheus.utils
  (:require [cluster-connector.utils.for-debug :refer [$ spy]]))

(defmacro defmulties [dispatch-fn & body]
  (concat
    '(do)
    (mapcat
      (fn [[name# args#]]
        (let [dispatcher-pam# (gensym "m")]
          [`(defmulti
              ~name#
              (fn ~(vec (concat [dispatcher-pam#] args#))
                (~dispatch-fn ~dispatcher-pam#))
              :default nil)
           `(defmethod ~name# nil ~(vec (concat [dispatcher-pam#] args#))
              (println "WARNING: method" ~(pr-str name#)
                       "for"
                       (pr-str (~dispatch-fn ~dispatcher-pam#))
                       "does not have implementation. Will return nil.")
              #_(.dumpStack (Thread/currentThread))
              nil)
           `(defmethod ~name# :none ~(vec (concat [dispatcher-pam#] args#)))]))
      body)))

(defmacro defmethods [dispatch-val head-symbol & body]
  (concat
    '(do)
    (map
      (fn [[name# args# & mbody#]]
        `(defmethod ~name# ~dispatch-val ~(vec (concat [head-symbol] args#))
           ~@mbody#))
      body)))

(defn remove-first [pred coll]
  (loop [checked []
         remains coll]
    (if (or (pred (first remains))
            (empty? remains))
      (concat (rest remains) checked)
      (recur (conj checked (first remains))
             (rest remains)))))

(defn and* [cond-funcs]
  (loop [ccond ((first cond-funcs))
         conds (rest cond-funcs)]
    (if (or (not ccond) (empty? conds))
      ccond
      (recur ((first conds))
             (rest conds)))))

(defn or* [cond-funcs]
  (loop [ccond ((first cond-funcs))
         conds (rest cond-funcs)]
    (if (or ccond (empty? conds))
      ccond
      (recur ((first conds))
             (rest conds)))))