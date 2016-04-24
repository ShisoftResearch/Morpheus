(ns morpheus.integration.sources.wikidata
  (:require [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]
            [morpheus.models.core :refer [add-schema]]
            [morpheus.core :refer [start-server* shutdown-server]]
            [neb.core :as neb]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [cheshire.core :as json]
            [com.climate.claypoole :as cp])
  (:import (org.shisoft.neb.exceptions SchemaAlreadyExistsException)
           (javax.xml.bind DatatypeConverter)
           (com.fasterxml.jackson.core JsonParseException)))

(set! *warn-on-reflection* true)

(defn from-calendar-code [c]
  (byte (case c
          "Q1985727" 0
          "Q1985786" 1
          c)))

(defn parse-entity-url [^String url]
  (when (and url (.startsWith url "http://www.wikidata.org/entity/"))
    (last (.split url "/"))))

(defn read-data [data-value data-type]
  (case (clojure.string/lower-case data-type)
    "string" data-value
    "wikibase-entityid" (let [{:keys [entity-type numeric-id]} data-value]
                          (str (case entity-type
                                 "item" "Q"
                                 "property" "P")
                               numeric-id))
    "globecoordinate" (let [{:keys [latitude longitude precision globe]} data-value
                            parsed-globe (parse-entity-url globe)]
                        [latitude longitude precision parsed-globe])
    "time" (let [{:keys [^String time timezone precision calendarmodel]} data-value
                 calendar-code (parse-entity-url calendarmodel)
                 calendar (from-calendar-code calendar-code)
                 time (try (-> (DatatypeConverter/parseDateTime
                                 (-> time
                                     (.replace "+" "")
                                     (.replace "-00" "-01")))
                               (.getTimeInMillis))
                           (catch Exception e))]
             [time timezone precision calendar])
    "monolingualtext" (let [{:keys [text language]} data-value]
                        text)
    "quantity" (let [{:keys [amount lowerBound upperBound unit]} data-value
                     [amount lowerBound upperBound] (map #(when % (read-string %))
                                                         [amount lowerBound upperBound])
                     unit (parse-entity-url unit)]
                 (cond
                   (and (not unit) (= amount lowerBound upperBound))
                   amount
                   (and unit (= amount lowerBound upperBound))
                   [amount unit]
                   :else
                   [amount lowerBound upperBound unit]))
    (throw (Exception. (str "unknown type: " data-type " with value: " data-value)))))

(defn encode-type [t]
  (let [encoded-num (get
                      {nil -1
                       "commonsMedia" 0
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
                      t)]
    (if-not encoded-num
      (throw (Exception. "unknown type for encode: " t))
      (byte encoded-num))))

(defn from-rank [r]
  (byte (case r
          "normal" 0
          "preferred" 1)))

(defn from-snak [s]
  (let [{:keys [snaktype datatype datavalue]} s
        {:keys [value type]} datavalue]
    (case snaktype
      "value" {:datatype (encode-type datatype)
               :value (read-data value type)}
      "novalue" {:datatype (byte -1)
                 :value nil}
      "somevalue" {:datatype (encode-type datatype)
                   :value nil}
      (throw (Exception. (str "Unknown snaktype: " snaktype " " s))))))

(defn from-entity-id [id]
  (when id (vertex-id-by-key :wikidata-record (name id))))

(defn from-qualifier [[id qarr]]
  {:prop (from-entity-id (name id))
   :values (map from-snak qarr)})

(defn from-reference [])

(defn from-entity-type [t]
  (byte (case t
          nil -1
          "item" 0
          "property" 1
          (throw (Exception. (str "Unknown entity type: " t))))))

(defn prepare-schemas []
  (try
    (println (vec (neb/get-schemas)))
    ;(add-schema :wikidata-reference [[:prop :cid] [:values [:ARRAY [[:type :byte] [:value :obj]]]]])
    (add-schema :wikidata-qualifier [[:prop :cid] [:values [:ARRAY [[:datatype :byte] [:value :obj]]]]])
    (new-vertex-group!
      :wikidata-record
      {:body  :defined :key-field :id
       :fields [[:id :text] [:label :text] [:description :text] [:type :byte] [:data-type :byte]
                [:alias [:ARRAY :text]]
                [:props [:ARRAY [[:prop :cid] [:datatype :byte] [:rank :byte] [:value :obj]
                                 [:qualifiers [:ARRAY :wikidata-qualifier]]
                                 ;[:references [:ARRAY :wikidata-reference]]
                                 ]]]]})
    (new-edge-group!
      :wikidata-link
      {:body :defined :type :directed
       :fields [[:prop :cid]
                [:rank :byte]
                [:qualifiers [:ARRAY :wikidata-qualifier]]
                ;[:references [:ARRAY :wikidata-reference]]
                ]})
    (catch SchemaAlreadyExistsException ex
      (println "Schema Already Existed " (.getMessage ex)))))

(defn import-entities [dump-path lang]
  (println "Import Vertices")
  (let [lang (keyword lang)
        th-pool (cp/threadpool 32)]
    (with-open [rdr (clojure.java.io/reader dump-path)]
      (cp/pdoseq
        th-pool [line (line-seq rdr)]
        (try
          (let [{:keys [labels descriptions aliases claims type datatype id]} (json/parse-string line true)]
            (let [[label desc alias] (map (fn [lang-strs-map]
                                            (let [ls (get lang-strs-map lang)
                                                  rand-s (-> lang-strs-map first second)
                                                  ls (if ls ls rand-s)]
                                              (cond
                                                (string? ls) ls
                                                (string? (:value ls)) (:value ls)
                                                (not (nil? ls)) ls
                                                (nil? ls) "")))
                                          [labels descriptions aliases])
                  alias (map :value alias)
                  props (->> claims
                             (map
                               (fn [[prop-id claim-arr]]
                                 (map
                                   (fn [{:keys [mainsnak qualifiers rank references] :as claim}]
                                     (assert (= (:type claim) "statement") (str "claim type not a statement: " claim))
                                     (let [data-type (get mainsnak :datatype)]
                                       (when (and (not= "deprecated" rank)
                                                  (not (or (= "wikibase-item" data-type)
                                                           (= "wikibase-property" data-type))))
                                         (merge (from-snak mainsnak)
                                                {:prop (from-entity-id (name prop-id))
                                                 :rank (from-rank rank)
                                                 :qualifiers (map from-qualifier qualifiers)
                                                 ;:references (map from-reference references)
                                                 }))))
                                   claim-arr)))
                             (flatten)
                             (filter identity)
                             (doall))
                  type (from-entity-type type)
                  datatype (encode-type datatype)]
              (new-vertex! :wikidata-record {:id id :label label :description desc :type type :data-type datatype :alias alias :props props})))
          (catch JsonParseException ex)
          (catch Exception ex
            (clojure.stacktrace/print-cause-trace ex)))))
    (cp/shutdown th-pool)))

(defn import-links [dump-path]
  (println "Import Edges")
  (with-open [rdr (clojure.java.io/reader dump-path)]
    (let [th-parse-pool (cp/threadpool 32)]
      (cp/pdoseq
        th-parse-pool [line (line-seq rdr)]
        (try
          (let [{:keys [id claims]} (json/parse-string line true)
                local-digest  (digest-vertex (from-entity-id id))
                edges (->> claims
                           (mapcat
                             (fn [[prop-id claim-arr]]
                               (map
                                 (fn [{:keys [mainsnak qualifiers rank references] :as claim}]
                                   (let [data-type (get mainsnak :datatype)]
                                     (when (and (= (:type claim) "statement")
                                                (not= "deprecated" rank)
                                                (or (= "wikibase-item" data-type)
                                                    (= "wikibase-property" data-type)))
                                       (let [snak (from-snak mainsnak)
                                             {:keys [datatype value]} snak]
                                         (when (and value (or (= 8 datatype) (= 9 datatype)))
                                           (let [remote-digest (digest-vertex (from-entity-id value))]
                                             (if remote-digest
                                               (try
                                                 [remote-digest
                                                  {:prop (from-entity-id (name prop-id))
                                                   :rank (from-rank rank)
                                                   :qualifiers (map from-qualifier qualifiers)}]
                                                 (catch Throwable tr
                                                   (clojure.stacktrace/print-cause-trace tr)))
                                               (println "Missing Vertex" local-digest remote-digest id value))))))))
                                 claim-arr)))
                           (filter identity))]
            (when local-digest (apply link-group! local-digest :wikidata-link edges)))
          (catch JsonParseException _)
          (catch Exception ex
            (clojure.stacktrace/print-cause-trace ex))))
      (cp/shutdown th-parse-pool))))

(defn import-to-this-cluster []
  (start-server* {:server-name :morpheus
                  :port 5124
                  :zk  "10.0.1.104:2181"
                  :trunks-size "25gb"
                  :memory-size "50gb"
                  ;:schema-file "configures/neb-schemas.edn"
                  :data-path   "wikidata"
                  :durability true
                  :auto-backsync true
                  ;:recover-backup-at-startup true
                  :replication 2})
  (let [wikidata-path "wikidata-latest-all.json"]
    (prepare-schemas)
    (import-entities wikidata-path :en)
    (import-links wikidata-path)))