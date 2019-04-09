(ns gregor.integration.test-message-passing
  (:require [gregor.consumer :as c]
            [gregor.producer :as p]
            [clojure.core.async :as a]
            [clojure.test :refer :all]))

(def consumer-control-ops #{:noop :subscribe :unsubscribe :subscriptions :close :commit :partitions-for})

(defn validate-consumer-control
  [{:keys [event op] :as payload}]
  (is (consumer-control-ops op))
  (case op
    :close (is (= event :control))

    :noop (is (= event :control))

    :subscribe (let [{:keys [topic]} payload]
                 (is (= topic "gregor.test")))

    :unsubscribe (is (= event :control))

    :subscriptions (let [{:keys [subscriptions]} payload]
                     (is (not-empty subscriptions)))

    :commit (is (= event :control))

    :partitions-for (let [partitions (:partitions payload)]
                      (is (= event :control))
                      (is (vector? partitions))
                      (is (not-empty partitions))
                      (is (every? #(= (:type-name %) :partition-info) partitions))
                      (is (every? #(contains? % :isr) partitions))
                      (is (every? #(contains? % :offline) partitions))
                      (is (every? #(contains? % :leader) partitions))
                      (is (every? #(contains? % :partition) partitions))
                      (is (every? #(contains? % :replicas) partitions))
                      (is (every? #(contains? % :topic) partitions))))
  true)

(deftest ^:integration message-passing
  (let [consumer (c/create {:output-policy #{:data :control :error}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"
                                                  :group.id "gregor.consumer.test"}
                            :topics :gregor.test.messagepassing})
        producer (p/create {:output-policy #{:data :control :error}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"}})]
    (let [{:keys [in-ch]} producer]
      (a/>!! in-ch {:topic :gregor.test.messagepassing :message-key {:id 1} :message-value {:a 1 :b 2}})
      (a/>!! in-ch {:topic :gregor.test.messagepassing :message-key {:id 2} :message-value {:a 3 :b 4}})
      (a/>!! in-ch {:topic :gregor.test.messagepassing :message-key {:id 3} :message-value {:a 5 :b 6}}))
    
    (let [{:keys [out-ch]} consumer]
      (let [{:keys [topic message-key message-value]} (a/<!! out-ch)]
        (is (= topic "gregor.test.messagepassing"))
        (is (= message-key {:id 1}))
        (is (= message-value {:a 1 :b 2}))))

    (a/>!! (:ctl-ch producer) {:op :close})
    (a/>!! (:ctl-ch consumer) {:op :close})))
