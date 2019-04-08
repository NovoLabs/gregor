(ns gregor.details.transform
  (:import (java.lang IllegalStateException
                      IllegalArgumentException)
           (java.util Map
                      )
           java.util.regex.Pattern
           org.apache.kafka.clients.consumer.OffsetAndMetadata
           (org.apache.kafka.common PartitionInfo
                                    Node)
           (org.apache.kafka.common KafkaException
                                    TopicPartition)
           (org.apache.kafka.clients.producer RecordMetadata
                                              ProducerRecord)
           (org.apache.kafka.clients.consumer ConsumerRecord
                                              ConsumerRecords)
           (org.apache.kafka.common.errors InterruptException
                                           SerializationException
                                           TimeoutException)
           (org.apache.kafka.common.record TimestampType)))

(defn ^Map opts->props
  "Kafka configuration requres a map of string to string.
   This function converts a Clojure map into a Java Map, String to String"
  [opts]
  (into {} (for [[k v] opts] [(name k) (str v)])))

(defn node->data
  "Convert an instance of `Node` into a map"
  [^Node n]
  (merge   {:type-name :node
            :host (.host n)
            :id   (.id n)
            :port (long (.port n))}
           (when (.hasRack n)
             {:rack (.rack n)})))

(defn partition-info->data
  "Convert an instance of `PartitionInfo` into a map"
  [^PartitionInfo pi]
  {:type-name :partition-info
   :isr (mapv node->data (.inSyncReplicas pi))
   :offline (mapv node->data (.offlineReplicas pi))
   :leader (node->data (.leader pi))
   :partition (long (.partition pi))
   :replicas (mapv node->data (.replicas pi))
   :topic (.topic pi)})

(defn topic-partition->data
  "Convert an instance of `TopicPartition` into a map"
  [^TopicPartition tp]
  {:type-name :topic-partition
   :topic (.topic tp)
   :partition (.partition tp)})

(defn record-metadata->data
  "Convert an instance of `RecordMetadata` to a map"
  [^RecordMetadata record-metadata]
  {:type-name :record-metadata
   :offset (.offset record-metadata)
   :partition (.partition record-metadata)
   :serialized-key-size (.serializedKeySize record-metadata)
   :serialized-value-size (.serializedValueSize record-metadata)
   :timestamp (.timestamp record-metadata)
   :topic (.topic record-metadata)})

(defn exception->data
  "Convert a known exception to a map"
  [^Exception e]
  {:type-name (cond
           (instance? InterruptException e) :interrupt-exception
           (instance? SerializationException e) :serialization-exception
           (instance? TimeoutException e) :timeout-exception
           (instance? KafkaException e) :kafka-exception
           (instance? IllegalStateException e) :illegal-state-exception
           (instance? IllegalArgumentException e) :illegal-argument-exception
           :else :unknown-exception)
   :stack-trace (->> (.getStackTrace e) seq (into []))
   :message (.getMessage e)})

(defn timestamp-type->data
  "Convert an instance of `TimestampType` to a map"
  [^TimestampType tt]
  {:type-name :timestamp-type
   :name (.name tt)
   :id (.id tt)})

;; TODO: Add :leader-epoch
;; TODO: Add support for ConsumerRecord Headers
(defn consumer-record->data
  "Yield a clojure representation of a consumer record"
  [^ConsumerRecord cr]
  {:type-name :consumer-record
   :message-key (.key cr)
   :message-key-size (.serializedKeySize cr)
   :offset (.offset cr)
   :partition (.partition cr)
   :timestamp (.timestamp cr)
   :topic (.topic cr)
   :message-value (.value cr)
   :message-value-size (.serializedValueSize cr)})

(defn consumer-records->data
  "Yield the clojure representation of topic"
  [^ConsumerRecords crs]
  (let [partitions (.partitions crs)]
    (-> (for [^TopicPartition p partitions] (mapv consumer-record->data (.records crs p)))
        flatten)))

(defn data->producer-record
  "Build a `ProducerRecord` object from a clojure map.  No-Op if `payload` is already a `ProducerRecord`"
  [{:keys [partition message-key message-value topic] :as data}]
  (let [topic (some-> topic name)]
    (cond
      (nil? topic)
        (throw (ex-info "`:topic` is a required parameter" {}))

      (or (nil? message-value) (empty? message-value))
        (throw (ex-info "`:value` is required and must not be empty" {:topic topic}))
      
      (and message-key partition)
        (ProducerRecord. topic (int partition) message-key message-value)

      message-key
        (ProducerRecord. topic message-key message-value)

      :else
        (ProducerRecord. topic message-value))))

(defn data->topics
  "Converts a topic or list of topics into a Collection of topics that KafkaConsumer understands"
  [topics]
  (cond
    (keyword? topics)
      [(name topics)]

    (string? topics)
      [topics]

    (instance? Pattern topics)
      topics

    (and (sequential? topics) (every? (some-fn string? keyword?) topics))
      (mapv name topics)

    :else
      (throw (ex-info "topics argument must be a string, keyword, regex or list of strings and/or keywords"
                      {:topics topics}))))

(def valid-events? #{:data :control :error :eof})

(defn ->event
  "Adds event information to the specified data (map)"
  ([event]
   (->event event {}))
  ([event data]
   (if (valid-events? event)
     (assoc data :event event)
     (throw (ex-info "invalid event" {:event event :valid-events valid-events?})))))
