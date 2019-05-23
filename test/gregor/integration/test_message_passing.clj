(ns gregor.integration.test-message-passing
  (:require [gregor.consumer :as c]
            [gregor.producer :as p]
            [clojure.core.async :as a]
            [clojure.test :refer :all]))

(defn validate-producer-data
  "Validates `data` events from the producer"
  [{:keys [type-name topic] :as payload}]
  (is (= type-name :record-metadata))
  (is (= topic "gregor.test.messagepassing"))
  (is (contains? payload :offset))
  (is (contains? payload :partition))
  (is (contains? payload :serialized-key-size))
  (is (contains? payload :serialized-value-size))
  (is (contains? payload :timestamp))
  true)

(defn validate-consumer-data
  "Validates `data` events from the consumer"
  [test-key test-value {:keys [topic message-key message-value topic]}]
  (is (= topic "gregor.test.messagepassing"))
  (is (= test-key message-key))
  (is (= test-value message-value)))

(deftest ^:integration message-passing
  (let [consumer (c/create {:output-policy #{:data :control :error}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"
                                                  :group.id "gregor.consumer.test"
                                                  :auto.offset.reset "earliest"}
                            :topics :gregor.test.messagepassing})
        producer (p/create {:output-policy #{:data :control :error}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"}})]
    ;; Verify we have gotten a properly initialized producer
    (is (contains? producer :in-ch))
    (is (contains? producer :out-ch))
    (is (contains? producer :ctl-ch))

    ;; Verify we have gotten a properly initialized consumer
    (is (contains? consumer :out-ch))
    (is (contains? consumer :ctl-ch))    
    
    (let [{:keys [in-ch out-ch]} producer]
      (a/>!! in-ch {:topic :gregor.test.messagepassing :message-key {:id 1} :message-value {:a 1 :b 2}})
      (validate-producer-data (a/<!! out-ch)))

    (->> (:out-ch consumer)
         a/<!!
         (validate-consumer-data {:id 1} {:a 1 :b 2}))
    
    (let [{:keys [in-ch out-ch]} producer]
      (a/>!! in-ch {:topic :gregor.test.messagepassing :message-key {:id 2} :message-value {:a 3 :b 4}})
      (validate-producer-data (a/<!! out-ch)))

    (->> (:out-ch consumer)
         a/<!!
         (validate-consumer-data {:id 2} {:a 3 :b 4}))
    
    (let [{:keys [in-ch out-ch]} producer]
      (a/>!! in-ch {:topic :gregor.test.messagepassing :message-key {:id 3} :message-value {:a 5 :b 6}})
      (validate-producer-data (a/<!! out-ch)))

    (->> (:out-ch consumer)
         a/<!!
         (validate-consumer-data {:id 3} {:a 5 :b 6}))

    (a/>!! (:ctl-ch producer) {:op :close})
    (a/>!! (:ctl-ch consumer) {:op :close})))
