(ns gregor.integration.test-consumer-transducer
  (:require [gregor.consumer :as c]
            [gregor.producer :as p]
            [clojure.core.async :as a]
            [clojure.test :refer :all]))

(defn is-data-event?
  "Returns `true` if `event` is `:data`, `false` otherwise"
  [{:keys [event]}]
  (= event :data))

(defn source-is-acme?
  "Returns `true` if `source` in the message key is `ACME`, `false` otherwise"
  [{:keys [message-key]}]
  (= (:source message-key) "ACME"))

(def cons-transducer (comp (filter is-data-event?)
                               (filter source-is-acme?)))

(defn valid-consumer-data?
  "Returns `true` if the consumer data is valid, `false` otherwise"
  [{:keys [event message-key message-value]}]
  (and (= event :data)
       (= (:source message-key) "ACME")
       (map? message-value)))

(deftest ^:integration consumer-transducer
  (let [consumer (c/create {:output-policy #{:data :control :error}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"
                                                  :group.id "gregor.consumer.test"
                                                  :auto.offset.reset "earliest"}
                            :topics :gregor.test.consumertransducer
                            :transducer cons-transducer})
        producer (p/create {:output-policy #{}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"}})]
    (let [{:keys [in-ch ctl-ch]} producer]
      (a/>!! in-ch {:topic :gregor.test.consumertransducer
                    :message-key {:source "ACME"}
                    :message-value {:a 1 :b 2}})

      (a/>!! in-ch {:topic :gregor.test.consumertransducer
                    :message-key {:source "MACE"}
                    :message-value {:y 1 :z 2}})

      (a/>!! in-ch {:topic :gregor.test.consumertransducer
                    :message-key {:source "ACME"}
                    :message-value {:c 3 :d 4}})

      (a/>!! ctl-ch {:op :close}))

    (let [{:keys [out-ch ctl-ch]} consumer]
      ;; We only check for 2 values as the message sourced from `MACE` should
      ;; be filtered out by the transducer
      (is (valid-consumer-data? (a/<!! out-ch)))
      (is (valid-consumer-data? (a/<!! out-ch)))

      (a/>!! ctl-ch {:op :close}))))
