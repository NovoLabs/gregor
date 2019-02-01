(ns gregor.details.producer
  (:require [gregor.details.protocols.producer :refer [ProducerProtocol]]
            [gregor.details.protocols.shared :refer [SharedProtocol]]
            [gregor.details.shared :refer [opts->props partition-info->data]]
            [gregor.details.serializer :refer [->serializer]])
  (:import java.util.concurrent.TimeUnit
           java.lang.Exception
           (org.apache.kafka.clients.producer KafkaProducer
                                              ProducerRecord
                                              RecordMetadata)
           org.apache.kafka.common.KafkaException
           (org.apache.kafka.common.errors InterruptException
                                           SerializationException
                                           TimeoutException)))

(defn record-metadata->map
  "Convert an instance of `RecordMetadata` to a map"
  [^RecordMetadata record-metadata]
  {:type :record-metadata
   :checksum (.checksum record-metadata)
   :offset (.offset record-metadata)
   :partition (.partition record-metadata)
   :serialized-key-size (.serializedKeySize record-metadata)
   :timestamp (.timestamp record-metadata)
   :topic (.topic record-metadata)})

(defn exception->map
  "Convert a known exception to a map"
  [^Exception e]
  {:type :exception
   :name (cond
           (instance? InterruptException e) :interrupt
           (instance? SerializationException e) :serialization
           (instance? TimeoutException e) :timeout
           (instance? KafkaException e) :kafka
           :else :unknown)
   :stack-trace (->> (.getStackTrace e) seq (into []))
   :message (.getMessage e)})

(defn ->record
  "Build a `ProducerRecord` object from a clojure map.  No-Op if `payload` is already a `ProducerRecord`"
  [{:keys [partition key value topic]}]
  (let [topic (some-> topic name str)]
    (cond
      (nil? topic)
        (throw (ex-info "`:topic` is a required parameter" {:key key}))

      (or (nil? value) (empty? value))
        (throw (ex-info "`:value` is required and must not be empty" {:key key :topic topic}))
      
      (and key partition)
        (ProducerRecord. str (int partition) key value)

      key
        (ProducerRecord. topic key value)

      :else
        (ProducerRecord. topic value))))

(defn reify-producer-protocol
  "Creates a reified implementation of a producer, which includes ProducerProtocol and CommonFunctionsProtocol"
  [^KafkaProducer producer]
  ;; Since we are capturing the `producer` object in `reify`, we us the `_` place holder for the `this` pointer
  ;; as it is unnecessary to perform the work
  (reify
    SharedProtocol
    (close! [_]
      (.close producer))
    (close! [_ timeout]
      (if-not (int? timeout)
        (.close producer)
        (.close producer (long timeout) TimeUnit/MILLISECONDS)))
    (partitions-for [_ topic]
      (mapv partition-info->data (.partitionsFor producer topic)))

    ProducerProtocol
    (send! [_ record]
      (.send producer (->record record)))
    (flush! [_]
      (.flush producer))
    (init-transactions! [_]
      (.initTransactions producer))
    (begin-transaction! [_]
      (.beginTransaction producer))
    (commit-transaction! [_]
      (.commitTransaction producer))))

(defn make-producer
  "Create a producer from a configuration"
  [{:keys [gregor.producer/key-serializer
           gregor.producer/value-serializer
           gregor.producer/kafka-configuration]
    :or {key-serializer (->serializer :edn) value-serializer (->serializer :edn)}
    :as config}]
  (let [config (dissoc config :gregor/key-serializer :gregor/value-serializer)]
    (reify-producer-protocol (KafkaProducer. (opts->props kafka-configuration)
                                             (->serializer key-serializer)
                                             (->serializer value-serializer)))))
