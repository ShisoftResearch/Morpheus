(ns morpheus.integration.sources.wikidata
  (:require [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]
            [morpheus.models.core :refer [add-schema]]
            [cheshire.core :as json]
            [clj-time :as time]
            [clj-time.format :as time-format])
  (:import (org.shisoft.neb.exceptions SchemaAlreadyExistsException)))

(defn from-calendar-code [c]
  (case c
    "Q1985727" 0
    "Q1985786" 1
    c))

(defn parse-entity-url [url]
  (when (and url (.startsWith url "http://www.wikidata.org/entity/"))
    (last (.split url "/"))))

(defn read-data [data-type data-value]
  (case (clojure.string/lower-case data-type)
    "string" data-type
    "wikibase-entityid" (let [{:keys [entity-type numeric-id]} data-value]
                          (str (case entity-type
                                 "item" "Q"
                                 "property" "P")
                               numeric-id))
    "globecoordinate" (let [{:keys [latitude longitude precision globe]} data-value
                            parsed-globe (parse-entity-url globe)]
                        [latitude longitude precision parsed-globe])
    "time" (let [{:keys [time timezone precision calendarmodel]} data-value
                 calendar-code (parse-entity-url calendarmodel)
                 calendar (from-calendar-code calendar-code)]
             [time timezone precision calendar])
    (throw (Exception. (str "unknown type" data-type)))))

(defn encode-type [t]
  (get
    {"commonsMedia" 0
     "globe-coordinate" 1
     "globecoordinate" 1
     "monolingualtext" 2
     "quantity" 3
     "string" 4
     "time" 5
     "url" 6
     "external-id" 7
     "wikibase-item" 8
     "wikibase-property" 9
     "math" 10}
    t))

(defn from-rank [r]
  (byte (case r
          "normal" 0
          "preferred" 1)))

(defn from-snak [s]
  (let [{:keys [snaktype datatype datavalue]} s
        {:keys [value type]} datavalue
        value-data (read-data type value)]
    (assert (= snaktype "value") (str "Unknown snaktype: " s))
    [(encode-type datatype) value-data]))

(defn import-entities [dump-path lang]
  (try
    (add-schema :wikidata-reference [[:prop :text] [:type :byte] [:value :edn]])
    (add-schema :wikidata-qualifier [[:prop :text] [:type :byte] [:value :edn]])
    (new-vertex-group!
      :wikidata-record
      {:body  :defined :key-field :id
       :fields [[:id :text] [:label :text] [:description :text] [:type :byte]
                [:alias [:ARRAY :text]]
                [:props [:ARRAY [[:prop :text] [:data-type :byte] [:rank :byte] [:value :edn]
                                 [:qualifiers [:ARRAY :wikidata-qualifier]]
                                 [:references [:ARRAY :wikidata-reference]]]]]]})
    (new-edge-group!
      :wikidata-link
      {:body :defined :key-field :id
       :fields [[:prop :text]
                [:rank :byte]
                [:qualifiers [:ARRAY :wikidata-qualifier]]
                [:references [:ARRAY :wikidata-reference]]]})
    (catch SchemaAlreadyExistsException _))
  (let [lang (keyword lang)]
    (with-open [rdr (clojure.java.io/reader dump-path)]
      (doseq [line (line-seq rdr)]
        (try
          (let [{:keys [labels descriptions aliases claims type id]} (json/parse-string line true)
                [label desc alias] (map lang [labels descriptions aliases])
                props (->> claims
                           (map (fn [[prop-id {:keys [mainsnak rank] :as claim}]]
                                  (assert (= (:type claim) "statement") (str "claim type not a statement: " claim))
                                  (let [data-type (get mainsnak :datatype)]
                                    (when (and (not= "deprecated" rank)
                                               (not (or (= "wikibase-item" data-type)
                                                        (= "wikibase-property" data-type))))
                                      {:prop prop-id
                                       :data-type data-type
                                       :rank (from-rank rank)
                                       :value (from-snak mainsnak)}))))
                           (filter identity))]
            )
          (catch Exception ex
            (clojure.stacktrace/print-cause-trace ex)))))))