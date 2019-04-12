(ns gregor.integration.test-producer-control-operations
  (:require [gregor.producer :as p]
            [clojure.core.async :as a]
            [clojure.test :refer :all]))

(defn create-producer
  []
  (p/create {:output-policy #{:data :control :error}
             :kafka-configuration {:bootstrap.servers "localhost:9092"}}))

(deftest ^:integration producer-partitions-for-operation
  (let [{:keys [ctl-ch out-ch]} (create-producer)]
    (a/>!! ctl-ch {:op :partitions-for :topic :gregor.test})
    (let [{:keys [partitions op event]} (a/<!! out-ch)]
      (is (= event :control))
      (is (= op :partitions-for))
      (is (vector? partitions))
      (is (not-empty partitions))
      (is (every? #(= (:type-name %) :partition-info) partitions))
      (is (every? #(contains? % :isr) partitions))
      (is (every? #(contains? % :offline) partitions))
      (is (every? #(contains? % :leader) partitions))
      (is (every? #(contains? % :partition) partitions))
      (is (every? #(contains? % :replicas) partitions))
      (is (every? #(contains? % :topic) partitions)))
    (a/>!! ctl-ch {:op :close})))

(deftest ^:integration producer-flush-operation
  (let [{:keys [ctl-ch out-ch]} (create-producer)]
    (a/>!! ctl-ch {:op :flush})
    (let [{:keys [op event]} (a/<!! out-ch)]
      (is (= event :control))
      (is (= op :flush)))
    (a/>!! ctl-ch {:op :close})))

(deftest ^:integration producer-close-operation
  (let [{:keys [ctl-ch out-ch]} (create-producer)]
    (a/>!! ctl-ch {:op :close})
    (is (= (a/<!! out-ch) {:op :close :event :control}))
    (is (= (a/<!! out-ch) {:event :eof}))
    (is (not (a/<!! out-ch)))
    (is (not (a/>!! ctl-ch {:op :noop})))))
