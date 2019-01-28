(ns gregor.details.producer
  (:require [gregor.details.protocols.producer :as producer-protocol]
            [gregor.details.protocols.common-functions :as common-functions-protocol]
            [gregor.details.shared :refer [opts->props]]
            [gregor.details.serializer :refer [->serializer]])
  (:import java.util.concurrent.TimeUnit
           java.lang.Exception
           (org.apache.kafka.clients.producer KafkaProducer
                                              ProducerRecord
                                              RecordsMetadata)
           (org.apache.kafka.common Node
                                    PartitionInfo)
           (org.apache.kafka.common.errors InterruptException
                                           SerializationException
                                           TimeoutException
                                           KafkaException)))

(defn node->data
  "Convert an instance of `Node` into a map"
  [^Node n]
  {:host (.host n)
   :id   (.id n)
   :port (long (.port n))})

(defn record-metadata->map
  "Convert an instance of `RecordMetadata` to a map"
  [^RecordMetadata record-matadata]
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

(defn partition-info->data
  "Convert an instance of `PartitionInfo` into a map"
  [^PartitionInfo pi]
  {:isr       (mapv node->data (.inSyncReplicas pi))
   :leader    (node->data (.leader pi))
   :partition (long (.partition pi))
   :replicas  (mapv node->data (.replicas pi))
   :topic     (.topic pi)})

(defn ->record
  "Build a `ProducerRecord` object from a clojure map.  No-Op if `payload` is already a `ProducerRecord`"
  [{:keys [partition key value]}]
  (let [topic (some-> payload :topic name str)]
    (cond
      (nil? topic)
        (throw (ex-info "`:topic` is a required parameter" {:partition partition :key key :topic topic}))

      (or (nil? value) (empty? value))
        (throw (ex-info "`:value` is required and must not be empty"
                        {:partition partition :key key :topic topic}))
      
      (and key partition)
        (ProducerRecord. str (int partition) key value)

      key
        (ProducerRecord. str key value)

      :else
        (ProducerRecord. topic value))))

(defn reify-producer-protocol
  "Creates a reified implementation of a producer, which includes ProducerProtocol and CommonFunctionsProtocol"
  [^KafkaProducer producer]
  ;; Since we are capturing the `producer` object in `reify`, we us the `_` place holder for the `this` pointer
  ;; as it is unnecessary to perform the work
  (reify
    CommonFunctionsProtocol
    (close! [_]
      (.close producer))
    (close! [_ timeout]
      (if-not (int? timeout)
        (.close producer)
        (.close producer (long timeout) TimeUnit/MILLISECONDS)))
    (partitions-for [_ topic]
      (mapv partition-info->data (.partitionsFor producer topic)))

    ProducerProtocol
    (send! [_ record]
      (.send producer (->record record)))
    (send! [_ record k v]
      (.send producer (-> (assoc record :key k :value v) ->record)))
    (flush! [_]
      (.flush producer))
    (init-transactions! [_]
      (.initTransactions producer))
    (begin-transaction! [_]
      (.beginTransaction producer))
    (commit-transaction! [_]
      (.commitTransaction producer))))

(defn make-producer
  "Create a producer from a configuration.  The configuration options are as follows:

   `:gregor/key-serializer`: One of the following, indicating which serializer to use for keys:
                             `:edn`, `string`, `json`, `keyword`
   `:gregor/value-serializer`: Same as `:gregor/key-serializer` except it is used to serialize values

   All other configuration options should be key-value pairs, which will be converted to a
   property map and passed into the Kafka Java client as configuration options.  For example:

   `:bootstrap.servers` \"localhost:9092\"
   `:max.poll.records` 1000"
  [{:keys [gregor/key-serializer gregor/value-serializer] :as config}]
  (let [config (dissoc config :gregor/key-serializer :gregor/value-serializer)]
    (reify-producer-protocol (KafkaProducer. (opts->props config)
                                             (->serializer key-serializer)
                                             (->serializer value-serializer)))))
