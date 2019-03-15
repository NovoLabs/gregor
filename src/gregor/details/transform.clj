(ns gregor.details.transform
  (:import (java.lang IllegalStateException
                      IllegalArgumentException)
           (java.util Map
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
  {:type :node
   :host (.host n)
   :id   (.id n)
   :port (long (.port n))})

(defn partition-info->data
  "Convert an instance of `PartitionInfo` into a map"
  [^PartitionInfo pi]
  {:type :partition-info
   :isr (mapv node->data (.inSyncReplicas pi))
   :leader (node->data (.leader pi))
   :partition (long (.partition pi))
   :replicas (mapv node->data (.replicas pi))
   :topic (.topic pi)})

(defn record-metadata->data
  "Convert an instance of `RecordMetadata` to a map"
  [^RecordMetadata record-metadata]
  {:type :record-metadata
   :checksum (.checksum record-metadata)
   :offset (.offset record-metadata)
   :partition (.partition record-metadata)
   :serialized-key-size (.serializedKeySize record-metadata)
   :timestamp (.timestamp record-metadata)
   :topic (.topic record-metadata)})

(defn exception->data
  "Convert a known exception to a map"
  [^Exception e]
  {:type (cond
           (instance? InterruptException e) :interrupt-exception
           (instance? SerializationException e) :serialization-exception
           (instance? TimeoutException e) :timeout-exception
           (instance? KafkaException e) :kafka-exception
           (instance? IllegalStateException e) :illegal-state-exception
           (instance? IllegalArgumentException e) :illegal-argument-exception
           :else :unknown-exception)
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
  {:type      :consumer-record
   :key       (.key cr)
   :offset    (.offset cr)
   :partition (.partition cr)
   :timestamp (.timestamp cr)
   :topic     (.topic cr)
   :value     (.value cr)})

(defn consumer-records->data
  "Yield the clojure representation of topic"
  [^ConsumerRecords crs]
  (let [partitions (.partitions crs)]
    (-> (for [^TopicPartition p partitions] (mapv consumer-record->data (.records crs p)))
        flatten)))

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

(defn ->event
  "Adds event information to the specified data (map)"
  ([event]
   (->event event {}))
  ([event data]
   (assoc data :event event)))
