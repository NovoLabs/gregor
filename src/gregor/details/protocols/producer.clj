(ns gregor.details.protocols.producer)

(defprotocol ProducerProtocol
  "Protocol defining the required functions of a producer implementation"
  (partitions-for [this topic]
    "Protocol to retrive partition information for a given topic
     The resulting data structure looks like the following, sourced
     from kafka's PartitionInfo class:

       {:topic \"the-topic\"
        :partition 0
        :isr [{:host \"replica1.my-kafka.com\" :id 0 :port 9092}]
        :leader {:host \"replica1.my-kafka.com\" :id 0 :port 9092}
        :replicas [{:host \"replica1.my-kafka.com\" :id 0 :port 9092}]}")

  (close! [this timeout]
    "Close this client")

  (send! [this record]
    "Produce a record on a topic.
     When using the single arity version, a map
     with the following keys is expected:
     `:key`, `:topic`, `:partition`, and `:value`.

     `:partition` is not required")

  (flush! [this]
    "Ensure that produced messages are flushed to their specified topics."))
