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

(defn round- ^double [^double x]
  (Math/floor (+ 0.5 x)))

(defn round?- [^double x]
  (= x (Math/floor (+ 0.5 x))))

(defn floor- ^double [^double x]
  (Math/floor x))

(defn ceil- ^double [^double x]
  (Math/ceil x))

(defn pow- ^double [^double x ^double y]
  (Math/pow x y))

(defn exp- ^double [^double x]
  (Math/exp x))

(defn log- ^double [^double x]
  (Math/log x))

(defn log10- ^double [^double x]
  (Math/log10 x))

(defn log1p- ^double [^double x]
  (Math/log1p x))

(defn sqrt- ^double [^double x]
  (Math/sqrt x))

(defn abs- ^double [^double x]
  (Math/abs x))

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

