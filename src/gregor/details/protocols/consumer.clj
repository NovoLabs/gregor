(ns gregor.details.protocols.consumer)

(defprotocol ConsumerDriver
  "Driver interface for consumers"
  (poll!          [this timeout]
    "Poll for new messages. Timeout in ms.
     The result is a data representation of a ConsumerRecords instance.
         {:partitions [[\"t\" 0] [\"t\" 1]]
          :topics     [\"t\"]
          :count      2
          :by-partition {[\"t\" 0] [{:key       \"k0\"
                                     :offset    1
                                     :partition 0
                                     :topic     \"t\"
                                     :value     \"v0\"}]
                         [\"t\" 1] [{:key       \"k1\"
                                     :offset    1
                                     :partition 1
                                     :topic     \"t\"
                                     :value     \"v1\"}]}
          :by-topic      {\"t\" [{:key       \"k0\"
                                  :offset    1
                                  :partition 0
                                  :topic     \"t\"
                                  :value     \"v0\"}
                                 {:key       \"k1\"
                                  :offset    1
                                  :partition 1
                                  :topic     \"t\"
                                  :value     \"v1\"}]}}")
  (pause!         [this] [this topic-partitions]
    "Pause consumption.")
  (resume!        [this topic-partitions]
    "Resume consumption.")
  (unsubscribe!   [this]
    "Unsubscribe from currently subscribed topics.")
  (subscribe!     [this topics] [this topics listener]
    "Subscribe to a topic or list of topics.
     The topics argument can be:
     - A simple string when subscribing to a single topic
     - A regex pattern to subscribe to matching topics
     - A sequence of strings
     The optional listener argument is either a callback
     function or an implementation of
     [ConsumerRebalanceListener](http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html).
     When a function is supplied, it will be called on relance
     events with a map representing the event, see
     [kinsky.client/rebalance-listener](#var-rebalance-listener)
     for details on the map format.")
  (commit!         [this] [this topic-offsets]
    "Commit offsets for a consumer.
     The topic-offsets argument must be a list of maps of the form:
     ```
     {:topic     topic
      :partition partition
      :offset    offset
      :metadata  metadata}
     ```
     The topic and partition tuple must be unique across the whole list.")
  (wake-up!        [this]
    "Safely wake-up a consumer which may be blocking during polling.")
  (seek!           [this] [this topic-partition offset]
    "Overrides the fetch offsets that the consumer will use on the next poll")
  (position!      [this] [this topic-partition]
    "Get the offset of the next record that will be fetched (if a record with that offset exists).")
  (subscription   [this]
    "Currently assigned topics"))
