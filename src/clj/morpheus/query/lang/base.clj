(ns morpheus.query.lang.base
  (:require [cluster-connector.utils.for-debug :refer [$ spy]]
            [morpheus.models.vertex.core :as v]
            [cluster-connector.remote-function-invocation.core :as rfi])
  (:import (java.util UUID)))

(def ^:dynamic *data* nil)

(defn has?- [a b]
  (cond
    (string? a)
    (identity (clojure.string/index-of a b))
    (coll? a)
    (clojure.set/subset?
      (if (coll? b) (set b) #{b}) (set a))))

(defn concat- [a & colls]
  (cond
    (string? a)
    (apply str a colls)
    (coll? a)
    (apply concat a colls)))

(defn and- [& conds]
  (loop [ccond (first conds)
         conds (rest conds)]
    (if (or (not ccond) (empty? conds))
      ccond
      (recur (first conds)
             (rest conds)))))

(defn or- [& conds]
  (loop [ccond (first conds)
         conds (rest conds)]
    (if (or ccond (empty? conds))
      ccond
      (recur (first conds)
             (rest conds)))))

(defn and-coll- [coll]
  (apply and- coll))

(defn or-coll- [coll]
  (apply or- coll))

(defn if- [clause a & [b]]
  (if clause a b))

(defn power-of-2?- [n]
  (= 0 (bit-and n (- n 1))))

(defn round- ^double [x]
  (Math/floor (+ 0.5 (double x))))

(defn round?- [x]
  (= x (Math/floor (+ 0.5 (double x)))))

(defn floor- ^double [x]
  (Math/floor (double x)))

(defn ceil- ^double [x]
  (Math/ceil (double x)))

(defn pow- ^double [x y]
  (Math/pow (double x) (double y)))

(defn exp- ^double [x]
  (Math/exp (double x)))

(defn log- ^double [x]
  (Math/log (double x)))

(defn log10- ^double [x]
  (Math/log10 (double x)))

(defn log1p- ^double [x]
  (Math/log1p (double x)))

(defn sqrt- ^double [x]
  (Math/sqrt (double x)))

(defn abs- ^double [x]
  (Math/abs (double x)))

(defn vertex- [& params]
  (let [[arg1 arg2] params]
    (cond
      (instance? UUID arg1)
      (v/vertex-by-id arg1)
      (keyword? arg1)
      (v/vertex-by-key arg1 arg2))))

(defn uuid- [& params]
  (let [[arg1 arg2] params]
    (cond
      (empty? params)
      (UUID/randomUUID)
      (string? arg1)
      (UUID/fromString arg1)
      (and (number? arg1) (number? arg2))
      (UUID. arg1 arg2))))

(defn soft-link [sym]
  (fn [& args]
    (apply (rfi/compiled-cache sym) args)))

(def function-mapper
  {'= =
   '< <
   '> >
   '>= >=
   '<= <=
   '!= not=
   'has? has?-
   '+ +
   '- -
   '* *
   '/ /
   'num? number?
   'str? string?
   'coll? coll?
   'set? set?
   'str str
   'num read-string
   'double double
   'int int
   'float float
   'uuid uuid-
   'cid uuid-
   'concat concat-
   'append conj
   'or or-
   'and and-
   'or-coll or-coll-
   'and-coll and-coll-
   '|| or-
   '&& and-
   'if if-
   'power-of-2? power-of-2?-
   'round round-
   'round? round?-
   'floor floor-
   'ceil ceil-
   'pow pow-
   'exp exp-
   'log log-
   'log10 log10-
   'log1p log1p-
   'sqrt sqrt-
   'abs abs-
   'assoc assoc
   'assoc-in assoc-in
   'dissoc dissoc
   'keyword? keyword?
   'keyword keyword
   'get get
   'get-in get-in
   'lower-case clojure.string/lower-case
   'upper-case clojure.string/upper-case
   '$ vertex-
   '-> (soft-link 'morpheus.models.edge.core/neighbours)
   '->- (soft-link 'morpheus.models.edge.core/neighbours-edges)
   '->n (soft-link 'morpheus.models.edge.core/degree)})

(def interpreter-mapper
  {'let (soft-link 'morpheus.query.lang.evaluation/let-)
   'let* (soft-link 'morpheus.query.lang.evaluation/let*-) ;let* will be slightly faster than let, but is not instant binding
   })
