(ns morpheus.models.edge.base
  (:require [morpheus.utils :refer :all]))

(defmulties
  :type
  (neighbours [])
  (inboundds [])
  (outbounds [])
  (neighbours [relationship])
  (inboundds [relationship])
  (outbounds [relationship])
  (edge-base-schema []))

(defmulties
  :body
  (get-edge [])
  (update-edge [new-edge])
  (delete-edge [])
  (base-schema [])
  (require-schema? [])
  (edge-schema [base-schema fields]))