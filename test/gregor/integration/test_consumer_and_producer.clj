(ns gregor.integration.test-consumer-and-producer
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
  {:key (select-keys m [:location-id :pos-provider])
   :value (select-keys m [:menu :timestamp])})

(defn add-topic
  "Adds `topic` to map `m`"
  [topic m]
  (assoc m :topic topic))

(defn validate-error
  "Validates the `error` event output by the producer"
  [{:keys [type-name message]}]
  (is (keyword? type-name))
  (is (string? message)))

(def prod-transducer (comp (map add-timestamp) (map key-value-split) (map (partial add-topic "gregor.test"))))

(defn validate-producer-data
  "Validates `data` events from the producer"
  [{:keys [type-name topic] :as payload}]
  (is (= type-name :record-metadata))
  (is (= topic "gregor.test"))
  (is (contains? payload :offset))
  (is (contains? payload :partition))
  (is (contains? payload :serialized-key-size))
  (is (contains? payload :serialized-value-size))
  (is (contains? payload :timestamp))
  true)

(def producer-control-ops #{:close :flush :partitions-for})

(defn validate-producer-control
  "Validates the `control` event output by the producer"
  [{:keys [event op] :as payload}]
  (is (producer-control-ops op))
  (case op
    :close (is (= event :control))
    
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
                      (is (every? #(contains? % :topic) partitions)))

    :flush (is (= event :control)))
  true)

(defn producer-output-test-loop
  "Gets output from the producer output channel and tests its validity"
  [{:keys [out-ch]}]
  (a/go-loop []
    (when-let [{:keys [event] :as payload} (a/<! out-ch)]
      (case event
        :data (do (validate-producer-data payload)
                  (recur))

        :control (do (validate-producer-control payload)
                     (recur))

        :error (do (validate-error payload)
                   (recur))

        event))))

(defn validate-consumer-data
  "Validates `data` events from the consumer"
  [{:keys [type-name topic record-key record-value] :as payload}]
  (is (= type-name :consumer-record))
  (is (= topic "gregor.test"))
  (is (map? record-key))
  (is (contains? record-key :location-id))
  (is (contains? record-key :pos-provider))
  (is (map? record-value))
  (is (contains? record-value :menu))
  (is (contains? record-value :timestamp))
  (is (contains? payload :offset))
  (is (contains? payload :partition))
  (is (contains? payload :timestamp))
  true)

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

(defn consumer-output-test-loop
  "Gets output from the consumer output channel and tests its validity"
  [{:keys [out-ch]}]
  (a/go-loop []
    (when-let [{:keys [event] :as payload} (a/<! out-ch)]
      (case event
        :data (do (validate-consumer-data payload)
                  (recur))

        :control (do (validate-consumer-control payload)
                     (recur))

        :error (do (validate-error payload)
                   (recur))

        event))))

(deftest ^:integration end-to-end
  (let [consumer (c/create {:output-policy #{:data :control :error}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"
                                                  :group.id "gregor.consumer.test"}
                            :topics :gregor.test})
        producer (p/create {:output-policy #{:data :control :error}
                            :kafka-configuration {:bootstrap.servers "localhost:9092"}
                            :transducer prod-transducer})
        producer-test-ch (producer-output-test-loop producer)
        consumer-test-ch (consumer-output-test-loop consumer)]
    ;; Verify we have gotten a properly initialized producer
    (is (contains? producer :in-ch))
    (is (contains? producer :out-ch))
    (is (contains? producer :ctl-ch))

    ;; Verify we have gotten a properly initialized consumer
    (is (contains? consumer :out-ch))
    (is (contains? consumer :ctl-ch))

    ;; Send control commands to the consumer
    (let [{:keys [ctl-ch]} consumer]
      (a/>!! ctl-ch {:op :noop})
      (a/>!! ctl-ch {:op :partitions-for :topic "gregor.test"})
      (a/>!! ctl-ch {:op :subscribe :topic "gregor.test"})
      (a/>!! ctl-ch {:op :partitions-for :topic "gregor.test"})
      (a/>!! ctl-ch {:op :subscriptions}))
    
    (let [{:keys [in-ch ctl-ch]} producer]
      (a/>!! in-ch {:location-id 1 :pos-provider "ACME" :menu {:meat [:brisket :sausage]
                                                               :veggies {:house-salad :caesar}}})
      (a/>!! in-ch {:location-id 2 :pos-provider "ACME" :menu {:meat [:hamburger :cheeseburger]
                                                               :veggies [:house-salad :cucumber-salad]}})
      (a/>!! ctl-ch {:op :flush})
      (a/>!! ctl-ch {:op :partitions-for :topic "gregor.test"})
      (a/>!! ctl-ch {:op :foobar})
      (a/>!! ctl-ch {:the-op "is missing"})
      (a/>!! ctl-ch {:op :close})
      (is (= (a/<!! producer-test-ch) :eof)))

    (let [{:keys [ctl-ch]} consumer]
      (a/>!! ctl-ch {:op :close})
      (is (= (a/<!! consumer-test-ch) :eof)))

    (let [{:keys [in-ch ctl-ch out-ch]} producer]
      (is (not (a/>!! in-ch {})))
      (is (not (a/>!! ctl-ch {})))
      (is (not (a/<!! out-ch))))))
