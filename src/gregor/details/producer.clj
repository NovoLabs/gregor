(ns gregor.details.producer
  (:require [gregor.details.protocols.producer :refer [ProducerProtocol]]
            [gregor.details.transform :refer [opts->props partition-info->data data->producer-record]]
            [gregor.details.serializer :refer [->serializer]])
  (:import java.util.concurrent.TimeUnit
           org.apache.kafka.clients.producer.KafkaProducer))

(defn reify-producer-protocol
  "Creates a reified implementation of a producer, which includes ProducerProtocol and SharedProtocol functions"
  [^KafkaProducer producer]
  ;; Since we are capturing the `producer` object in `reify`, we us the `_` place holder for the `this` pointer
  ;; as it is unnecessary to perform the work
  (reify
    ProducerProtocol

    (close! [_ timeout]
      (if-not (int? timeout)
        (.close producer)
        (.close producer (long timeout) TimeUnit/MILLISECONDS)))

    (partitions-for [_ topic]
      (->> (.partitionsFor producer topic)
           (mapv partition-info->data)
           (assoc {} :partitions)))

    (send! [_ record]
      (.send producer (data->producer-record record)))

    (flush! [_]
      (.flush producer))))

(defn make-producer
  "Create a producer from a configuration"
  [{:keys [key-serializer value-serializer kafka-configuration] :or {key-serializer :edn value-serializer :edn}}]
  (reify-producer-protocol (KafkaProducer. (opts->props kafka-configuration)
                                           (->serializer key-serializer)
                                           (->serializer value-serializer))))
