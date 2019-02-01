(ns gregor.details.protocols.consumer)

(defprotocol ConsumerDriver
  "Driver interface for consumers"
  (poll! [this timeout]
    "Poll for new messages. Timeout in ms.")
  (pause! [this] [this topic-partitions]
    "Pause consumption.")
  (resume! [this topic-partitions]
    "Resume consumption.")
  (unsubscribe! [this]
    "Unsubscribe from currently subscribed topics.")
  (subscribe! [this topics]
    "Subscribe to a topic or list of topics.")
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
  (wake-up! [this]
    "Safely wake-up a consumer which may be blocking during polling.")
  (seek! [this] [this topic-partition offset]
    "Overrides the fetch offsets that the consumer will use on the next poll")
  (position! [this] [this topic-partition]
    "Get the offset of the next record that will be fetched (if a record with that offset exists).")
  (subscription [this]
    "Currently assigned topics"))
