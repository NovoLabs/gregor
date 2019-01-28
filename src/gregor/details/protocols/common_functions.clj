(ns gregor.details.protocols.shared)

(defprotocol SharedProtocol
  "Protocal for shared functionality between consumer and producer"
  (partitions-for [this topic]
    "Protocol to retrive partition information for a given topic
     The resulting data structure looks like the following, sourced
     from kafka's PartitionInfo class:

       {:topic \"the-topic\"
        :partition 0
        :isr [{:host \"replica1.my-kafka.com\" :id 0 :port 9092}]
        :leader {:host \"replica1.my-kafka.com\" :id 0 :port 9092}
        :replicas [{:host \"replica1.my-kafka.com\" :id 0 :port 9092}]}")

  (close! [this] [this timeout]
    "Close this client"))
