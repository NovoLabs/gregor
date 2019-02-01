(ns gregor.details.serializer
  (:require [cheshire.core :as json])
  (:import (org.apache.kafka.common.serialization Serializer
                                                  StringSerializer)))

(defn reify-serializer
  "Creates an implementation of the `Serializer` protocol provided by Kafka, only implementing `serialize`"
  [serialization-fn]
  (reify
    Serializer
    (close [_])
    (configure [_ config is-key?])
    (serialize [_ topic payload]
      (serialization-fn topic payload))))

(defn edn-serializer
  "Serializer for EDN"
  []
  (reify-serializer
   (fn [_ payload] (some-> payload pr-str .getBytes))))

(defn json-serializer
  "Serializer for JSON"
  []
  (reify-serializer
   (fn [_ payload] (some-> payload json/generate-string .getBytes))))

(defn keyword-serializer
  "Serializer for Keywords"
  []
  (reify-serializer
   (fn [_ k] (some-> k name .getBytes))))

(defn string-serializer
  "Serializer for Strings"
  []
  (StringSerializer.))

(def serializers {:edn edn-serializer
                  :json json-serializer
                  :keyword keyword-serializer
                  :string string-serializer})

(defn ^Serializer ->serializer
  "Map symbolic keyword to an actual serializer"
  [serializer-id]
  (if-let [serializer-creator (serializers serializer-id)]
    (serializer-creator)
    (throw (ex-info "unknown serializer" {:serializer serializer-id}))))
