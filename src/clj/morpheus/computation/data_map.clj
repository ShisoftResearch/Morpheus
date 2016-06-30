(ns morpheus.computation.data-map
  (:require [taoensso.nippy :as nippy]
            [cluster-connector.utils.for-debug :refer [$ spy]])
  (:import (org.shisoft.hurricane.datastructure DiskMappingTable SeqableMap InMemoryMap)
           (java.io File)
           (net.openhft.koloboke.collect.map.hash HashObjObjMaps)))

(defn ^SeqableMap gen-map [uuid on-disk?]
  (if on-disk?
    (DiskMappingTable.
      (spy (.getAbsolutePath (File/createTempFile (str uuid) ".bin")))
      nippy/freeze nippy/thaw true)
    (InMemoryMap.)))