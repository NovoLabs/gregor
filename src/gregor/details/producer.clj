(ns gregor.details.producer
  (:require [gregor.details.protocols.producer :refer [ProducerProtocol]]
            [gregor.details.protocols.shared :refer [SharedProtocol]]
            [gregor.details.transform :refer [opts->props partition-info->data data->record]]
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
      (.send producer (data->record record)))
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
