(ns gregor.details.deserializer
  (:require [cheshire.core :as json]
            [clojure.edn :as edn])
  (:import (org.apache.kafka.common.serialization Deserializer
                                                  StringDeserializer)))

(defn reify-deserializer
  "Creates an implementation of the `Deserializer` protocol provided by Kafka, only implementing `deserialize`"
  [deserialization-fn]
  (reify
    Deserializer
    (close [_])
    (configure [_ config is-key?])
    (deserialize [_ topic payload]
      (deserialization-fn topic payload))))

(defn edn-deserializer
  "Create EDN deserializer"
  []
  (reify-deserializer (fn [_ #^"[B" payload]
                  (when payload
                    (edn/read-string (String. payload "UTF-8"))))))

(defn json-deserializer
  "Create JSON deserializer"
  []
  (reify-deserializer (fn [_ #^"[B" payload]
                  (when payload
                    (json/parse-string (String. payload "UTF-8") true)))))

(defn keyword-deserializer
  "Creates a String deserializer which turns the result into a keyword"
  []
  (reify-deserializer (fn [_ #^"[B" payload]
                  (when payload
                    (keyword (String. payload "UTF-8"))))))

(defn string-deserializer
  "Create an instance of Kafka's own StringDeserializer object"
  []
  (StringDeserializer.))

(def deserializers {:edn edn-deserializer
                    :json json-deserializer
                    :keyword keyword-deserializer
                    :string string-deserializer})

(defn ^Deserializer ->deserializer
  "Map symbolic keyword to an actual deserializer"
  [deserializer-id]
  (if-let [deserializer-creator (deserializers deserializer-id)]
    (deserializer-creator)
    (throw (ex-info "unknown deserializer" {:deserializer deserializer-id}))))
