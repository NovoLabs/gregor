(ns gregor.details.consumer
  (:require [gregor.details.protocols.consumer :refer [ConsumerProtocol]]
            [gregor.details.protocols.shared :refer [SharedProtocol]]
            [gregor.details.transform :as xform]
            [gregor.details.deserializer :refer [->deserializer]])
  (:import org.apache.kafka.clients.consumer.KafkaConsumer
           java.util.concurrent.TimeUnit))

(defn reify-consumer-protocol
  "Create a reified implementation of a consumer, which includes ConsumerProtocol and SharedProtocol functions"
  [^KafkaConsumer consumer]
  (reify
    SharedProtocol
    (close! [_]
      (.close consumer))

    (close! [_ timeout]
      (if-not (int? timeout)
        (.close consumer)
        (.close consumer (long timeout) TimeUnit/MILLISECONDS)))

    (partitions-for [_ topic]
      (mapv xform/partition-info->data (.partitionsFor consumer topic)))

    ConsumerProtocol
    (poll! [_ timeout]
      (xform/consumer-records->data (.poll consumer timeout)))

    (subscribe! [_ topics]
      (.subscribe consumer (xform/data->topics topics)))

    (unsubscribe! [_]
      (.unsubscribe consumer))

    (subscription [_]
      (.subscription consumer))
    
    (commit! [_]
      (.commitSync consumer))

    (commit! [_ topic-offsets]
      (.commitSync consumer (->> topic-offsets
                                 (map (juxt xform/data->topic-partition xform/data->offset-metadata))
                                 (reduce merge {}))))

    (seek! [_ topic-partition offset]
      (.seek consumer (xform/data->topic-partition topic-partition) offset))

    (position! [_ topic-partition]
      (.position consumer (xform/data->topic-partition topic-partition)))

    (wake-up! [_]
      (.wakeup consumer))))
