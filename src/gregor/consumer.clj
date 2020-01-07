(ns gregor.consumer
  (:require [gregor.details.consumer :refer [make-consumer]]
            [gregor.details.transform :as xform]
            [gregor.details.transducer :refer [ex-handler]]
            [gregor.details.protocols.consumer :as consumer]
            [gregor.defaults :refer [default-output-buffer default-timeout]]
            [gregor.output-policy :refer [data-output? control-output? error-output? any-output?]]
            [clojure.core.async :as a])
  (:import [java.util ConcurrentModificationException]
           [java.lang IllegalStateException]))

(defn close-tap
  [ch-mult ch]
  (a/untap ch-mult ch)
  (a/close! ch))

(defn safe-poll
  "Polls for data arriving from Kafka, catching appropriate exceptions to dispatch control events"
  [{:keys [driver timeout output-policy]}]
  (try
    (->> (consumer/poll! driver 2000)
         (map #(xform/->event :data %))
         (assoc {} :data)
         (xform/->event :data))
    (catch org.apache.kafka.common.errors.WakeupException _
      ;; The Kafka consumer is not thread safe.  All access must be done via a single owner thread.  The only
      ;; function that is thread safe is `.wakeup` which triggers a wake up exception.  Gregor uses `.wakeup`
      ;; to force the Kafka consumer out of a blocking poll in order to handle a control event.  This call
      ;; is made in the go-loop created by `wakeup-loop` function.
      (xform/->event :control))
    (catch IllegalStateException _
      ;; This exception is thrown when we call `.poll!` without being subscribed to any topics.  We handle it
      ;; here and return it as a control event.  This will allow us to handle control events prior to being
      ;; subscribed to a topic.  Most important, we can handle the control command to `subscribe`.
      (xform/->event :control))
    (catch ConcurrentModificationException e
      ;; This should not happen.  If it does, there is a bug in Gregor and we propogate the exception
      (throw e))
    (catch Exception e
      ;; All other exceptions are converted to data and returned
      (->> (xform/exception->data e)
           (xform/->event :error)))))

(defn subscribed?
  "Returns `true` if `driver` is subscribed to the specified `topic`"
  [driver]
  (-> (consumer/subscription driver)
      not-empty))

(defn handle-control-event
  "Handle a control event after it has been read from the channel"
  [{:keys [op topic-offsets topic topics] :as command} {:keys [driver timeout ctl-ch out-ch output-policy]}]
  (cond
    (nil? op)
      (when (error-output? output-policy)
        (->> {:type-name :missing-control-operation
              :message "`op` key is missing, no operation specified"}
             (xform/->event :error)))

    (= op :noop)
      (xform/->event :control command)

    (= op :subscribe)
      (let [t (or topics topic)]
        (try
          (consumer/subscribe! driver t)
          (xform/->event :control command)
          (catch IllegalStateException e
            (->> (xform/exception->data e)
                 (xform/->event :error)))))

    (= op :close)
      (do
        (consumer/close! driver timeout)
        (xform/->event :control command))

    (= op :unsubscribe)
      (do
        (consumer/unsubscribe! driver)
        (xform/->event :control command))

    (= op :subscriptions)
      (->> (consumer/subscription driver)
           (into [])
           (assoc command :subscriptions)
           (xform/->event :control))
    
    (= op :commit)
      (do
        (consumer/commit! driver)
        (xform/->event :control command))

    (= op :partitions-for)
      (if topic
        (->> (name topic)
             (consumer/partitions-for driver)
             (assoc command :partitions)
             (xform/->event :control))
        (->> {:type-name :missing-topic
              :message "`topic` is requred by `partitions-for` operation"}
             (xform/->event :error)))

    :else
      (->> {:type-name :unknown-control-operation
            :message (str op " is an unknown control operation")}
           (xform/->event :error))))

(defn processing-loop
  "Creates loop for processing messages from Kafka `driver`v"
  [{:keys [out-ch ctl-ch ctl-mult ctl-handler-ch ctl-ready-ch output-policy] :as context}]
  (a/go-loop []
    (let [{:keys [event data] :as result} (safe-poll context)]
      (case event
        ;; Copy all of the data onto `out-ch`. The `false` parameter indicates
        ;; the channel should be kept open after all items have been read
        :data (do
                ;; `onto-chan` returns a channel that is closed when the copying completes
                (a/<! (a/onto-chan out-ch data false))
                (recur))

        ;; Read the control event off of `ctl-handler-ch`, using `a/alt!` to prevent
        ;; us from blocking the main data processing thread.  If the operation
        ;; indicated from the control event is `:close`, we shut down the consumer
        ;; and all of its channels.  Otherwise, keep processing.
        :control (let [{:keys [op] :as command} (a/alt!
                                                  (a/timeout default-timeout) ([_] {:op :noop})
                                                  ctl-handler-ch ([r] r))
                       ctl-result (handle-control-event command context)]
                   (when (and (control-output? output-policy) (not= op :noop))
                     (a/>! out-ch ctl-result))
                   (if (= op :close)
                     (do
                       (a/>! out-ch (xform/->event :eof))
                       (a/close! out-ch)
                       (close-tap ctl-mult ctl-handler-ch)
                       (a/close! ctl-ch))
                     (do
                       (a/>! ctl-ready-ch true)
                       (recur))))

        ;; If an exception was raised, it is turned into data and written to the output channel
        :error (do
                 (when (error-output? output-policy)
                   (a/>! out-ch result))
                 (recur))))))

(defn wakeup-loop
  "Listens on tapped channel for control commands, waking up the Kafka client when a message is received"
  [{:keys [driver ctl-mult ctl-wakeup-ch ctl-ready-ch]}]
  (a/go-loop []
    (when (a/<! ctl-ready-ch)
      (when-let [{:keys [op]} (a/<! ctl-wakeup-ch)]
        (consumer/wakeup! driver)
        (if (= op :close)
          (do
            (close-tap ctl-mult ctl-wakeup-ch)
            (a/close! ctl-ready-ch))
          (recur))))))

(defn create-context
  "Builds context map containing all the necessary channels and channel connections"
  [{:keys [output-buffer timeout output-policy transducer]
    :or {output-buffer default-output-buffer
         timeout default-timeout
         output-policy #{}}
    :as config}]
  (let [ctl-ch (a/chan)
        output-policy (conj output-policy :data)]
    {:driver (make-consumer config)
     :ctl-ch ctl-ch
     :ctl-ready-ch (a/chan)
     :ctl-mult (a/mult ctl-ch)
     :ctl-wakeup-ch (a/chan)
     :ctl-handler-ch (a/chan)
     :out-ch (if transducer (a/chan output-buffer transducer ex-handler) (a/chan output-buffer))
     :timeout timeout
     :output-policy output-policy}))

(defn create
  "Create a consumer, returning a map that contains 2 channels:

   `:out-ch` - Output channel used to receive all of the data from kafka as well as controll and error
               events that occur.  The publishing of these events will depend on your `output-policy`

   `:ctl-ch` - Input channel used to issue control commands to the consumer.

   Configuration options:

   `:output-policy`: A set containing the output sources that should be returned via the `out-ch`.
                     Valid options include:

                     `:control`: Output from control operations
                     `:error`: Any exceptions or errors that occur
                     `:data`: Data retrieved from the kafka consumer.  The `data` setting will always be
                              included in the `output-policy`, else none of the data will be accessible
                     
                     The Kafka Java interface returns each of these as a Java object. Gregor converts each
                     to pure data (i.e. a map).
   `:topics`: Either a vector of topics, represented as strings or keywords or a single topic represented as a
              string or keyword or a regex to match a set of topics.
    
   `:timeout`: time to wait, in milliseconds, for queued message retrieval when closing the consumer.
  
   `:key-deserializer`: Serializer to use for key serialization, default is `:edn`.
                        Valid serializers are `:edn`, `:string`, `:json` and `:keyword`
  
   `:value-deserializer`: Serializer to use for value serialization, default is `:edn`.
                          Valid serializers are `:edn`, `:string` and `:json`
  
   `:output-buffer`: Buffer size of `:out-ch`, default is 100

   `:kafka-configuration`: Map containing kafka producer configuration settings.  This map will be converted
                           into key/value properties and passed directly to the KafkaProducer object.

   `:transducer`: Transformation function to be applied to all data placed on `out-ch` channel.

   Example Configuration:

   `{:kafka-configuration {:bootstrap.servers \"localhost:9092\"
                           :max.poll.recordss 1000
                           :group.id \"gregor.test\"}
     :output-policy #{:control :error}
     :key-deserializer :string
     :value-deserializer :json
     :output-buffer 50
     :timeout 100}`

   All key value pairs in `:kafka-configuration` will be converted and inserted into a property map and
   passed into the Kafka Java client as configuration options."
  
  [config]
  (let [{:keys [ctl-mult ctl-wakeup-ch ctl-handler-ch ctl-ready-ch] :as context} (create-context config)]
    (a/tap ctl-mult ctl-wakeup-ch)
    (a/tap ctl-mult ctl-handler-ch)
    (wakeup-loop context)
    (processing-loop context)
    (a/>!! ctl-ready-ch true) ;; Tell the wake up loop that we are ready for the first control command
    (select-keys context [:ctl-ch :out-ch])))
