(ns morpheus.computation.package-compiler
  (:require [cemerick.pomegranate :as pomegranate]
            [leiningen.core.classpath :as classpath]))

(defn eval-in [project form]
  (doseq [path (classpath/get-classpath project)]
    (pomegranate/add-classpath path))
  (eval form))

(defn compile-path [wd]
  (let [project (read-string (slurp (str wd "/project.clj")))
        project (-> (apply hash-map (subvec (vec project) 3))
                    (assoc :eval-in :classloader
                           :source-paths [(str wd "/src")]))
        core-namespace (:main project)]
    (eval-in
      project `(require (symbol ~(str core-namespace))))))