(ns morpheus.computation.zip-utils
  (:require [clojure.java.io :as io])
  (:import (java.util.zip ZipEntry ZipOutputStream)
           (java.io FileInputStream File)
           (org.shisoft.morpheus UnzipUtil)))

(defn zip [src out]
  (with-open [zip (ZipOutputStream. (io/output-stream out))]
    (doseq [f (file-seq (io/file src)) :when (.isFile f)]
      (.putNextEntry zip (ZipEntry. (.getPath f)))
      (io/copy f zip)
      (.closeEntry zip))))

(defn unzip [src out]
  (UnzipUtil/unzip src out))