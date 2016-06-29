(ns morpheus.computation.data-map
  (:require [taoensso.nippy :as nippy])
  (:import (org.shisoft.hurricane DiskMappingTable)
           (java.io File)
           (net.openhft.koloboke.collect.map.hash HashObjObjMaps)
           (java.util Map)))

(defn ^Map gen-map [uuid on-disk?]
  (if on-disk?
    (DiskMappingTable.
      (.getName (File/createTempFile (str uuid) ".bin"))
      nippy/freeze nippy/thaw)
    (HashObjObjMaps/newMutableMap)))