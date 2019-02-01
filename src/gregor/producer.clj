(ns gregor.producer
  (:require [gregor.details.producer :refer [make-producer]]
            [gregor.details.transform :as t]
            [gregor.details.protocols.producer :as prod]
            [gregor.details.protocols.shared :as shared]
            [clojure.core.async :as a]))

(def default-input-buffer
  "Default number of messages on input channel"
  10)

(def default-output-buffer
  "Default number of messages on the output channel"
  100)

(def default-timeout
  "Default timeout in milli-seconds"
  100)

(defn process-input
  "Sends `message` to the kafka `producer`, returning results via the `out` channel"
  [out driver message {:keys [::output-policy] :or {output-policy :no-output}}]
  (try
    (let [response (prod/send! driver message)]
      (when ()
        (->> @response
             t/record-metadata->map
             (a/>! out))))
    (catch Exception e
      (->> (t/exception->map e)
           (a/>! out)))))

(defn input-loop
  "Starts the loop that processes input, waiting for the user to send input on `in` channel"
  [in out driver timeout]
  (a/go-loop []
    (if-let [message (a/<! in)]
      (do (process-input out driver message)
          (recur))
      (do (a/close! out)
          (if timeout
            (shared/close! driver timeout)
            (shared/close! driver))))))

(defn control-loop
  "Starts the loop that process control commands from the user"
  [in out ctl driver]
  (a/go-loop []
    (when-let [{:keys [op topic] :as payload} (a/<! ctl)]
      (cond
        (and (nil? op) out)
          (a/>! out {:status :error :type :missing-control-operation
                     :reason "`op` key is missing, no operation specified"})

        (= op :close)
          (let [ctl-msg {:status :success :op :close}]
            (a/close! in)
            (a/close! ctl)
            (when out
              (a/>! out ctl-msg) ;; send response before closing output channel
              (a/close! out)))

        (= op :flush)
          (let [ctl-msg {:status :success :op :flush}]
            (prod/flush! driver)
            (when out
              (a/>! out ctl-msg)))

        (and out (= op :partitions-for))
          (if topic
            (a/>! out {:op :partitions-for :topic topic :partitions (shared/partitions-for driver topic)})
            (a/>! out {:status :error :op :partitions-for :type :topic-missing
                       :reason "`topic` is required by `partitions-for` operation"}))

        out
          (a/>! out {:status :error :type :bad-control-operation
                     :payload payload :reason (str op " is not a valid control operation")}))

      (when (not= op :close)
        (recur)))))

(def ctl-out #{:control :both})
(def prod-out #{:producer :both})

(defn create
  "Create a producer, returning a map that contains 3 channels:

   `:gregor.producer/in-ch`  - Input channel used to send messages to the producer. 

   `:gregor.producer/ctl-ch` - Control channel used to manage the producer connection

   `:gregor.producer/out-ch` - Output channel used to receive messages from the Kafka producer and controller.
                               This channel will be `nil` unless an `output-policy` is specified.

   Configuration options:

   `:gregor.producer/output-policy`: Policy for handling return value from KafkaProducer::send
                                     Valid output policies are `:controller`, `:producer` or `:both`
                                     If no output policy is specified, output from the producer and
                                     controller will be discarded.
  
   `:gregor.producer/close-timeout`: time to wait for queued messages to send when closing the producer
                                     `close-timeout` is in milliseconds
  
   `:gregor.producer/key-serializer`: Serializer to use for key serialization, default is `:edn`.
                                      Valid serializers are `:edn`, `:string`, `:json` and `:keyword`
  
   `:gregor.producer/value-serializer`: Serializer to use for value serialization, default is `:edn`.
                                        Valid serializers are `:edn`, `:string` and `:json`
  
   `:gregor.producer/input-buffer`: Buffer size of `:gregor.producer/in-ch`
  
   `:gregor.producer/output-buffer`: Buffer size of `:gregor.producer/out-ch`

   `:gregor.producer/kafka-configuration`: Map containing kafka producer configuration settings.  This
                                           map will be converted into key/value properties and passed
                                           directly to the KafkaProducer object.

   Example Kafka Configuration:

   `{ :gregor.producer/kafka-configuration { :bootstrap.servers \"localhost:9092\"
                                             :max.poll.recordss 1000 }}`

   All key value pairs in `:gregor.producer/kafka-configuration` will be converted and inserted into a
   property map and passed into the Kafka Java client as configuration options."

  [{:keys [::input-buffer ::output-buffer ::timeout ::output-policy]
    :or {input-buffer default-input-buffer output-buffer default-output-buffer timeout default-timeout}
    :as config}]
  (let [p (make-producer config)
        in (a/chan input-buffer)
        ctl (a/chan input-buffer)
        out-ctl (when (ctl-out output-policy) (a/chan output-buffer))
        out-prod (when (prod-out output-policy) (a/chan output-buffer))
        out (some-> (filter identity [out-ctl out-prod]) not-empty vec (a/merge output-buffer))]
    (input-loop in out-prod p config)
    (control-loop ctl out-ctl p config)
    (if out
      {::in-ch in ::out-ch out ::control-ch ctl}
      {::in-ch in ::control-ch ctl})))
