(ns gregor.details.consumer
  (:require [gregor.details.protocols.consumer :refer [ConsumerProtocol] :as consumer]
            [gregor.details.transform :as xform]
            [gregor.details.deserializer :refer [->deserializer]])
  (:import org.apache.kafka.clients.consumer.KafkaConsumer
           java.util.concurrent.TimeUnit
           org.apache.kafka.common.errors.WakeupException))

(defn reify-consumer-protocol
  "Create a reified implementation of a consumer, which includes ConsumerProtocol and SharedProtocol functions"
  [^KafkaConsumer consumer]
  (reify
    ConsumerProtocol

    (close! [_ timeout]
      (if-not (int? timeout)
        (.close consumer)
        (.close consumer (long timeout) TimeUnit/MILLISECONDS)))

    ;; Hopefully we can remove this try ... catch in the future.  There is currently a synchronization bug
    ;; that arises if the user spams control events that for functions that can throw WakeupException.  Since
    ;; the WakeupException is used to exit `poll!` to handle control events
    (partitions-for [_ topic]
      (mapv xform/partition-info->data (.partitionsFor consumer topic)))

    (poll! [_ timeout]
      (xform/consumer-records->data (.poll consumer timeout)))

    (subscribe! [_ topics]
      (.subscribe consumer (xform/data->topics topics)))

    (unsubscribe! [_]
      (.unsubscribe consumer))

    (subscription [_]
      (.subscription consumer))
    
    (commit! [_]
      #(.commitSync consumer))

    (wakeup! [_]
      (.wakeup consumer))))

(defn make-consumer
  "Create a consumer from a configuration"
  [{:keys [key-deserializer value-deserializer kafka-configuration topics]
    :or {key-deserializer :edn value-deserializer :edn}}]
  (let [driver (reify-consumer-protocol (KafkaConsumer. (xform/opts->props kafka-configuration)
                                                        (->deserializer key-deserializer)
                                                        (->deserializer value-deserializer)))]
    (consumer/subscribe! driver topics)
    driver))
