(ns gregor.details.test-transfrom
  (:require [gregor.details.transform :as t]
            [clojure.test :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (java.lang IllegalStateException
                      IllegalArgumentException)
           (org.apache.kafka.common PartitionInfo
                                    Node)
           (org.apache.kafka.common TopicPartition
                                    KafkaException)
           (org.apache.kafka.clients.producer RecordMetadata)
           (org.apache.kafka.clients.consumer ConsumerRecord
                                              ConsumerRecords)
           (org.apache.kafka.common.errors InterruptException
                                           SerializationException
                                           TimeoutException)
           (org.apache.kafka.common.record TimestampType)))

(defn valid-exception?
  [e type]
  (and (contains? e :type)
       (keyword? (:type e))
       (= (:type e) type)
       (contains? e :stack-trace)
       (vector? (:stack-trace e))
       (contains? e :message)
       (string? (:message e))
       (not-empty (:message e))))

(deftest object-to-data-converters
  (testing "opts->props"
    (let [opts {:bootstrap.servers "localhost:9092"
                :group.id "gregor.consumer.test"}
          props {"bootstrap.servers" "localhost:9092"
                 "group.id" "gregor.consumer.test"}]
      (is (= props (t/opts->props opts)))))

  (testing "node->data"
    (let [node (Node. 1 "localhost" 9092 "some-rack")
          data {:type :node :host "localhost" :id 1 :port 9092 :rack "some-rack"}]
      (is (= data (t/node->data node))))

    (let [node (Node. 1 "localhost" 9092)
          data {:type :node :host "localhost" :id 1 :port 9092}]
      (is (= data (t/node->data node)))))

  (testing "partition-info->data"
    (let [leader-node (Node. 1 "localhost" 9092)
          replica-node-1 (Node. 2 "replica-1" 9092)
          replica-node-2 (Node. 3 "replica-2" 9092)
          partition-info (PartitionInfo. "gregor.test"
                                         1
                                         leader-node
                                         (into-array [replica-node-1 replica-node-2])
                                         (into-array [replica-node-1])
                                         (into-array [replica-node-2]))
          data {:type :partition-info
                :isr [(t/node->data replica-node-1)]
                :offline [(t/node->data replica-node-2)]
                :leader (t/node->data leader-node)
                :partition 1
                :replicas [(t/node->data replica-node-1) (t/node->data replica-node-2)]
                :topic "gregor.test"}]
      (is (= data (t/partition-info->data partition-info)))))

  (testing "topic-partition->data"
    (let [topic-partition (TopicPartition. "gregor.test" 1)
          data {:type :topic-partition :topic "gregor.test" :partition 1}]
      (is (= data (t/topic-partition->data topic-partition)))))

  (testing "record-metadata->data"
    (let [topic-partition (TopicPartition. "gregor.test" 1)
          timestamp (System/currentTimeMillis)
          record-metadata (RecordMetadata. topic-partition 100 4 timestamp 123456 123 456)
          data {:type :record-metadata :offset 104 :partition 1 :topic "gregor.test"
                :serialized-key-size 123 :serialized-value-size 456 :timestamp timestamp}]
      (is (= data (t/record-metadata->data record-metadata)))))

  ;; These tests do not test the contents of `:stack-trace`, only that it exists.  We
  ;; are making an assumption here that it will be set properly when the exception is received.
  (testing "exception->data"
    (let [illegal-state (-> (IllegalStateException. "illegal state") t/exception->data)
          illegal-argument (-> (IllegalArgumentException. "illegal argument") t/exception->data)
          kafka (-> (KafkaException. "kafka") t/exception->data)
          timeout (-> (TimeoutException. "timeout") t/exception->data)
          serialization (-> (SerializationException. "serialization") t/exception->data)
          interrupt (-> (InterruptException. "interrupt") t/exception->data)
          unknown (-> (Exception. "unknown") t/exception->data)]
      (is (valid-exception? illegal-state :illegal-state-exception))
      (is (valid-exception? illegal-argument :illegal-argument-exception))
      (is (valid-exception? kafka :kafka-exception))
      (is (valid-exception? timeout :timeout-exception))
      (is (valid-exception? serialization :serialization-exception))
      (is (valid-exception? interrupt :interrupt-exception))
      (is (valid-exception? unknown :unknown-exception))))

  (testing "timestamp-type->data"
    (let [timestamp-type TimestampType/LOG_APPEND_TIME
          data {:type :timestamp-type :name "LOG_APPEND_TIME" :id 1}]
      (is (= data (t/timestamp-type->data timestamp-type)))))

  (testing "consumer-record->data"
    (let [timestamp-type TimestampType/LOG_APPEND_TIME
          timestamp (System/currentTimeMillis)
          leader-epoch (System/currentTimeMillis)
          consumer-record (ConsumerRecord. "gregor.test" 1 104 timestamp timestamp-type
                                           123456 123 456 "key" "value")
          data {:type :consumer-record :key "key" :value "value" :offset 104
                :partition 1 :topic "gregor.test" :timestamp timestamp}]
      (is (= data (t/consumer-record->data consumer-record)))))

  (testing "consumer-records->data"
    (let [timestamp-type TimestampType/LOG_APPEND_TIME
          timestamp (System/currentTimeMillis)
          leader-epoch (System/currentTimeMillis)
          topic-partition-1 (TopicPartition. "gregor.test" 1)
          topic-partition-2 (TopicPartition. "gregor.test" 2)
          consumer-record-1 (->> (ConsumerRecord. "gregor.test" 1 104 timestamp timestamp-type 123456 123 456 "key" "value")
                                 (conj [])
                                 (java.util.ArrayList.))
          consumer-record-2 (->> (ConsumerRecord. "gregor.test" 2 99 timestamp timestamp-type 123456 123 456 "key" "value")
                                 (conj [])
                                 (java.util.ArrayList.))
          consumer-records (ConsumerRecords. {topic-partition-1 consumer-record-1 topic-partition-2 consumer-record-2})
          data [{:type :consumer-record :key "key" :value "value" :offset 104
                 :partition 1 :timestamp timestamp :topic "gregor.test"}
                {:type :consumer-record :key "key" :value "value" :offset 99
                 :partition 2 :timestamp timestamp :topic "gregor.test"}]]
      (is (= data (t/consumer-records->data consumer-records))))))

(deftest data-to-object-converters
  (testing "data->producer-record"
    (let [producer-record (t/data->producer-record {:topic "gregor.test" :value "value"})]
      (is (= (.topic producer-record) "gregor.test"))
      (is (= (.value producer-record) "value")))

    (let [producer-record (t/data->producer-record {:topic "gregor.test" :key "key" :value "value"})]
      (is (= (.topic producer-record) "gregor.test"))
      (is (= (.key producer-record) "key"))
      (is (= (.value producer-record) "value")))

    (let [producer-record (t/data->producer-record {:topic "gregor.test" :partition 1 :key "key" :value "value"})]
      (is (= (.partition producer-record) 1))
      (is (= (.topic producer-record) "gregor.test"))
      (is (= (.key producer-record) "key"))
      (is (= (.value producer-record) "value")))

    (is (thrown? ExceptionInfo (t/data->producer-record {:topic "gregor.test"})))
    (is (thrown? ExceptionInfo (t/data->producer-record {:value "value"})))))

(deftest data-to-data
  (testing "data->topics"
    (is (= ["gregor.test"] (t/data->topics :gregor.test)))
    (is (= ["gregor.test"] (t/data->topics "gregor.test")))
    (is (= ["gregor.test" "gregor.test-2"] (t/data->topics [:gregor.test "gregor.test-2"])))
    (is (= java.util.regex.Pattern (class (t/data->topics #"^gregor\..*"))))
    (is (= (str #"^gregor\..*") (str (t/data->topics #"^gregor\..*"))))
    (is (thrown? ExceptionInfo (t/data->topics {:topic "gregor.test"}))))

  (testing "->event"
    (is (= {:a 1 :b 2 :event :data} (t/->event :data {:a 1 :b 2})))
    (is (= {:a 1 :b 2 :event :control} (t/->event :control {:a 1 :b 2})))
    (is (= {:a 1 :b 2 :event :error}) (t/->event :error {:a 1 :b 2}))
    (is (thrown? ExceptionInfo (t/->event :foobar {:a 1 :b 2})))))
