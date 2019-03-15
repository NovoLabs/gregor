(ns gregor.details.protocols.consumer)

(defprotocol ConsumerProtocol
  "Driver interface for consumers"
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

  (poll! [this timeout]
    "Poll for new messages. Timeout in ms.")

  (subscribe! [this topics]
    "Subscribe to a topic or list of topics.")

  (unsubscribe! [this]
    "Unsubscribe from currently subscribed topics.")

  (subscription [this]
    "Currently assigned topics")

  (commit! [this] [this topic-offsets]
    "Commit offsets for a consumer.
     The topic-offsets argument must be a list of maps of the form:
     ```
     {:topic     topic
      :partition partition
      :offset    offset
      :metadata  metadata}
     ```
     The topic and partition tuple must be unique across the whole list.")

  (wakeup! [this]
    "Safely wake-up a consumer which may be blocking during polling."))
