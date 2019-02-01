(ns gregor.details.transform
  (:import java.util.Map
           (org.apache.kafka.common PartitionInfo
                                    Node)))

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
  {:isr       (mapv node->data (.inSyncReplicas pi))
   :leader    (node->data (.leader pi))
   :partition (long (.partition pi))
   :replicas  (mapv node->data (.replicas pi))
   :topic     (.topic pi)})

(defn record-metadata->map
  "Convert an instance of `RecordMetadata` to a map"
  [^RecordMetadata record-metadata]
  {:type :record-metadata
   :checksum (.checksum record-metadata)
   :offset (.offset record-metadata)
   :partition (.partition record-metadata)
   :serialized-key-size (.serializedKeySize record-metadata)
   :timestamp (.timestamp record-metadata)
   :topic (.topic record-metadata)})

(defn exception->map
  "Convert a known exception to a map"
  [^Exception e]
  {:type :exception
   :name (cond
           (instance? InterruptException e) :interrupt
           (instance? SerializationException e) :serialization
           (instance? TimeoutException e) :timeout
           (instance? KafkaException e) :kafka
           :else :unknown)
   :stack-trace (->> (.getStackTrace e) seq (into []))
   :message (.getMessage e)})

(defn data->record
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

