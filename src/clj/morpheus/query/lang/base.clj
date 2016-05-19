(ns morpheus.query.lang.base)

(defn has?- [a b]
  (cond
    (string? a)
    (identity (clojure.string/index-of a b))
    (coll? a)
    ((set a) b)))

(defn sub?- [a & b]
  )

(def op-mapper
  {'= =
   '< <
   '> >
   '>= >=
   '<= <=
   '!= not=
   'has? has?-
   'sub? sub?-})