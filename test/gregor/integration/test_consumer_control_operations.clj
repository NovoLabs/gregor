(ns gregor.integration.test-consumer-control-operations
  (:require [gregor.consumer :as c]
            [clojure.core.async :as a]
            [clojure.test :refer :all]))

(defn create-consumer
  []
  (c/create {:output-policy #{:data :control :error}
             :kafka-configuration {:bootstrap.servers "localhost:9092"
                                   :group.id "gregor.consumer.test"}
             :topics :gregor.test}))

(deftest ^:integration consumer-subscription-operations
  (let [{:keys [out-ch ctl-ch]} (create-consumer)]
    ;; Should have a single subscription from the creation of the consumer
    (a/>!! ctl-ch {:op :subscriptions})
    (is (= (a/<!! out-ch) {:op :subscriptions :subscriptions ["gregor.test"] :event :control}))

    ;; Should have 2 subscriptions now, since we called `:subscribe` again
    (a/>!! ctl-ch {:op :subscribe :topics [:gregor.test :gregor.test.2]})
    (a/>!! ctl-ch {:op :subscriptions})
    (is (= (a/<!! out-ch) {:op :subscribe :topics [:gregor.test :gregor.test.2] :event :control}))
    (is (= (a/<!! out-ch) {:op :subscriptions :subscriptions ["gregor.test.2" "gregor.test"] :event :control}))

    ;; Should get an `:error` event as you can not switch subscription types (i.e. named to regex-based)
    ;; without first unsubscribing.  This is a limitation of the KafkaConsumer object
    (a/>!! ctl-ch {:op :subscribe :topics #"gregor\..*"})
    (let [{:keys [type-name message event]} (a/<!! out-ch)]
      (is (= event :error))
      (is (= type-name :illegal-state-exception))
      (is (= message "Subscription to topics, partitions and pattern are mutually exclusive")))

    ;; If we want to re-subscribe using a regular expression, we first need to unsubscribe
    (a/>!! ctl-ch {:op :unsubscribe})
    (is (= (a/<!! out-ch) {:op :unsubscribe :event :control}))
    (a/>!! ctl-ch {:op :subscriptions})

    ;; Subscriptions should be empty if we are unsubscribed
    (let [{:keys [event op subscriptions]} (a/<!! out-ch)]
      (is (= op :subscriptions))
      (is (= event :control))
      (is (empty? subscriptions)))

    ;; Now we can successfully subscribe using a regular expression
    (a/>!! ctl-ch {:op :subscribe :topics #"gregor\..*"})
    (let [{:keys [op topics event]} (a/<!! out-ch)]
      (is (= event :control))
      (is (= op :subscribe))
      (is (= "gregor\\..*" (str topics))))

    (a/>!! ctl-ch {:op :close})))

(deftest ^:integration consumer-close-operation
  (let [{:keys [out-ch ctl-ch]} (create-consumer)]
    (a/>!! ctl-ch {:op :close})
    (is (= (a/<!! out-ch) {:op :close :event :control}))
    (is (= (a/<!! out-ch) {:event :eof}))
    (is (not (a/<!! out-ch)))
    (is (not (a/>!! ctl-ch {:op :noop})))))

(deftest ^:integration consumer-partitions-for-operation
  (let [{:keys [ctl-ch out-ch]} (create-consumer)]
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
      (is (every? #(contains? % :topic) partitions)))))
