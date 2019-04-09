(ns gregor.integration.test-message-passing
  (:require [gregor.consumer :as c]
            [gregor.producer :as p]
            [clojure.core.async :as a]
            [clojure.test :refer :all]))

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
        (is (= message-value {:a 1 :b 2})))

      (let [{:keys [topic message-key message-value]} (a/<!! out-ch)]
        (is (= topic "gregor.test.messagepassing"))
        (is (= message-key {:id 2}))
        (is (= message-value {:a 3 :b 4})))

      (let [{:keys [topic message-key message-value]} (a/<!! out-ch)]
        (is (= topic "gregor.test.messagepassing"))
        (is (= message-key {:id 3}))
        (is (= message-value {:a 5 :b 6}))))
    
    (a/>!! (:ctl-ch producer) {:op :close})
    (a/>!! (:ctl-ch consumer) {:op :close})))
