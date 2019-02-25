(ns gregor.details.transform
  (:import (java.util Map
                      Collection)
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
                                           TimeoutException)))

(defn ^Map opts->props
  "Kafka configuration requres a map of string to string.
   This function converts a Clojure map into a Java Map, String to String"
  [opts]
  (into {} (for [[k v] opts] [(name k) (str v)])))

(defn node->data
  "Convert an instance of `Node` into a map"
  [^Node n]
  {:host (.host n)
   :id   (.id n)
   :port (long (.port n))})

(defn partition-info->data
  "Convert an instance of `PartitionInfo` into a map"
  [^PartitionInfo pi]
  {:name :partition-info
   :isr (mapv node->data (.inSyncReplicas pi))
   :leader (node->data (.leader pi))
   :partition (long (.partition pi))
   :replicas (mapv node->data (.replicas pi))
   :topic (.topic pi)})

(defn record-metadata->data
  "Convert an instance of `RecordMetadata` to a map"
  [^RecordMetadata record-metadata]
  {:name :record-metadata
   :checksum (.checksum record-metadata)
   :offset (.offset record-metadata)
   :partition (.partition record-metadata)
   :serialized-key-size (.serializedKeySize record-metadata)
   :timestamp (.timestamp record-metadata)
   :topic (.topic record-metadata)})

(defn exception->data
  "Convert a known exception to a map"
  [^Exception e]
  {:name (cond
           (instance? InterruptException e) :interrupt
           (instance? SerializationException e) :serialization
           (instance? TimeoutException e) :timeout
           (instance? KafkaException e) :kafka
           :else :unknown)
   :stack-trace (->> (.getStackTrace e) seq (into []))
   :message (.getMessage e)})

(defn data->producer-record
  "Build a `ProducerRecord` object from a clojure map.  No-Op if `payload` is already a `ProducerRecord`"
  [{:keys [partition key value topic]}]
  (let [topic (some-> topic name str)]
    (cond
      (nil? topic)
        (throw (ex-info "`:topic` is a required parameter" {:key key}))

      (or (nil? value) (empty? value))
        (throw (ex-info "`:value` is required and must not be empty" {:key key :topic topic}))
      
      (and key partition)
        (ProducerRecord. str (int partition) key value)

      key
        (ProducerRecord. topic key value)

      :else
        (ProducerRecord. topic value))))

(defn consumer-record->data
  "Yield a clojure representation of a consumer record"
  [^ConsumerRecord cr]
  {:key       (.key cr)
   :offset    (.offset cr)
   :partition (.partition cr)
   :timestamp (.timestamp cr)
   :topic     (.topic cr)
   :value     (.value cr)})

(defn consumer-records->data
  "Yield the clojure representation of topic"
  [^ConsumerRecords crs]
  (let [->d  (fn [^TopicPartition p] [(.topic p) (.partition p)])
        ps   (.partitions crs)
        by-p (into {} (for [^TopicPartition p ps] [(->d p) (mapv consumer-record->data (.records crs p))]))]
    {:by-partition by-p}))

(defn ^Collection data->topics
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

(defn ^TopicPartition data->topic-partition
  "Yield a TopicPartition object from a clojure map."
  [{:keys [topic partition]}]
  (TopicPartition. (name topic) (int partition)))

(defn ^OffsetAndMetadata data->offset-metadata
  "Yield a OffsetAndMetadata object from a clojure map."
  [{:keys [offset metadata]}]
  (OffsetAndMetadata. offset metadata))

(defn data->event
  "Adds event information to the specified data (map)"
  [event-type data]
  (assoc data :event-type event-type))
