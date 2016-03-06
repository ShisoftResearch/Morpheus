(ns morpheus.utils)

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