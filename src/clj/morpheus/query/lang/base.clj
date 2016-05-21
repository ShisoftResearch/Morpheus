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
   'if if-})

