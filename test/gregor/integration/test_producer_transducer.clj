(ns gregor.integration.test-producer-transducer
  (:require [gregor.consumer :as c]
            [gregor.producer :as p]
            [clojure.core.async :as a]
            [clojure.test :refer :all]))

(defn add-timestamp
  "Adds a timestamp to map `m`"
  [m]
  (assoc m :timestamp (System/currentTimeMillis)))

(defn key-value-split
  "Sub-divides map `m` into a key and a value"
  [m]
  {:message-key (select-keys m [:id])
   :message-value (select-keys m [:a :b])})

(defn add-topic
  "Adds `topic` to map `m`"
  [topic m]
  (assoc m :topic topic))

(def prod-transducer (comp (map add-timestamp)
                           (map key-value-split)
                           (map (partial add-topic "gregor.test.producertransducer"))))

(defn validate-producer-data
  "Validates `data` events from the producer"
  [{:keys [type-name topic] :as payload}]
  (is (= type-name :record-metadata))
  (is (= topic "gregor.test.producertransducer"))
  (is (contains? payload :offset))
  (is (contains? payload :partition))
  (is (contains? payload :serialized-key-size))
  (is (contains? payload :serialized-value-size))
  (is (contains? payload :timestamp))
  true)

(defn validate-consumer-data
  "Validates `data` events from the consumer"
  [test-key test-value {:keys [topic message-key message-value topic]}]
  (is (= topic "gregor.test.producertransducer"))
  (is (= test-key message-key))
  (is (= test-value message-value)))

(deftest ^:integration producer-transducer
  (let [consumer (c/create {:output-policy #{:data :control :error}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"
                                                  :group.id "gregor.consumer.test"}
                            :topics :gregor.test.producertransducer})
        producer (p/create {:output-policy #{:data :control :error}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"}
                            :transducer prod-transducer})]
    (let [{:keys [in-ch out-ch]} producer]
      (a/>!! in-ch {:id 1 :a 1 :b 2})
      (validate-producer-data (a/<!! out-ch)))
    
    (->> (:out-ch consumer)
         a/<!!
         (validate-consumer-data {:id 1} {:a 1 :b 2}))
        
    (let [{:keys [in-ch out-ch]} producer]
      (a/>!! in-ch {:id 2 :a 3 :b 4})
      (validate-producer-data (a/<!! out-ch)))

    (->> (:out-ch consumer)
         a/<!!
         (validate-consumer-data {:id 2} {:a 3 :b 4}))
    
    (let [{:keys [in-ch out-ch]} producer]
      (a/>!! in-ch {:id 3 :a 5 :b 6})
      (validate-producer-data (a/<!! out-ch)))

    (->> (:out-ch consumer)
         a/<!!
         (validate-consumer-data {:id 3} {:a 5 :b 6}))
    
    (a/>!! (:ctl-ch producer) {:op :close})
    (a/>!! (:ctl-ch consumer) {:op :close})))
