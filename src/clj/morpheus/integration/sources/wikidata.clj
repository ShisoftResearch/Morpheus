(ns morpheus.integration.sources.wikidata
  (:require [cheshire.core :as json]
            [morpheus.models.vertex.core :refer :all]
            [morpheus.models.edge.core :refer :all]
            [morpheus.models.core :refer [add-schema]])
  (:import (org.shisoft.neb.exceptions SchemaAlreadyExistsException)))

(defn import-entities [dump-path lang]
  (try
    (add-schema :wikidata-reference [[:prop :text] [:type :byte] [:value :string]])
    (add-schema :wikidata-qualifier [[:prop :text] [:type :byte] [:value :string]])
    (new-vertex-group!
      :wikidata-record
      {:body  :defined :key-field :id
       :fields [[:id :text] [:label :text] [:description :text] [:type :byte]
                [:alias [:ARRAY :text]]
                [:props [:ARRAY [[:prop :text] [:data-type :byte] [:rank :byte] [:value :string]
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
                [label desc alias] (map lang [labels descriptions aliases])]
            )
          (catch Exception ex
            (clojure.stacktrace/print-cause-trace ex)))))))