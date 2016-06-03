(ns morpheus.query.lang.base
  (:require [cluster-connector.utils.for-debug :refer [$ spy]]))

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

(def op-mapper
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
   'concat concat-
   'append conj
   'or or-
   'and and-
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
   'abs abs-})

