(ns gregor.producer
  (:require [gregor.details.producer :refer [make-producer]]
            [gregor.details.transform :as xform]
            [gregor.details.protocols.producer :as producer]
            [gregor.defaults :refer [default-input-buffer default-output-buffer default-timeout]]
            [gregor.output-policy :refer [data-output? control-output? error-output? any-output?]]
            [clojure.core.async :as a]))

(defn process-input
  "Sends `message` to the kafka `producer`, returning results via the `out` channel"
  [message driver output-policy]
  (try
    (let [response (producer/send! driver message)]
      (when (data-output? output-policy)
        (->> @response
             xform/record-metadata->data
             (xform/->event :data))))
    (catch Exception e
      (when (error-output? output-policy)
        (->> (xform/exception->data e)
             (xform/->event :error))))))

(defn input-loop
  "Starts the loop that processes input, waiting for the user to send input on `in` channel"
  [{:keys [in-ch out-ch output-policy driver timeout] :as context}]
  (a/go-loop []
    (if-let [message (a/<! in-ch)]
      (let [result (process-input message driver output-policy)]
        (when (and out-ch result)
          (a/>! out-ch result))
        (recur))
      (producer/close! driver timeout))))

(defn control-loop
  "Starts the loop that process control commands from the user"
  [{:keys [in-ch ctl-ch out-ch driver output-policy]}]
  (a/go-loop []
    (when-let [{:keys [op topic] :as payload} (a/<! ctl-ch)]
      (cond
        (and (nil? op) (error-output? output-policy))
          (a/>! out-ch (-> {:reason :missing-control-operation :description "`op` key is missing, no operation specified"}
                           (xform/->event :error)))

        (= op :close)
          (let [ctl-msg (xform/->event :control payload)]
            (a/close! in-ch)
            (a/close! ctl-ch)
            (when (control-output? output-policy)
              (a/>! out-ch ctl-msg)
              (a/>! out-ch (xform/->event :eof))) ;; send response before closing output channel            
            (when out-ch
              (a/close! out-ch)))

        (= op :flush)
          (let [ctl-msg (xform/->event :control payload)]
            (producer/flush! driver)
            (when (control-output? output-policy)
              (a/>! out-ch ctl-msg)))

        (and (control-output? output-policy) (= op :partitions-for))
          (if topic
            (a/>! out-ch (->> (producer/partitions-for driver topic) (xform/->event :control)))
            (when (error-output? output-policy)
              (a/>! out-ch (->> {:reason :missing-topic :description "`topic` is required by `partitions-for` operation"}
                                (xform/->event :error)))))

        (error-output? output-policy)
          (a/>! out-ch (->> {:name :invalid-control-operation :op op :message (str op " is not a valid control operation")}
                            (xform/->event :error))))

      (when (not= op :close)
        (recur)))))

(defn create-context
  "Builds context map containing all the driver, various channels and configuration options"
  [{:keys [input-buffer output-buffer output-policy timeout transducer]
    :or {input-buffer default-input-buffer
         output-buffer default-output-buffer
         timeout default-timeout
         output-policy #{}}
    :as config}]
  (let [ctl-out-ch (when (control-output? output-policy) (a/chan output-buffer))
        prod-out-ch (when (or (data-output? output-policy) (error-output? output-policy)) (a/chan output-buffer))]
    {:driver (make-producer config)
     :in-ch (if transducer (a/chan input-buffer transducer) (a/chan input-buffer))
     :ctl-ch (a/chan input-buffer)
     :ctl-out-ch ctl-out-ch
     :prod-out-ch prod-out-ch
     :out-ch (some-> (filter identity [ctl-out-ch prod-out-ch]) not-empty vec (a/merge output-buffer))
     :output-policy output-policy
     :timeout timeout}))

(defn create
  "Create a producer, returning a map that contains 3 channels:

   `:in-ch`  - Input channel used to send messages to the producer. 

   `:ctl-ch` - Control channel used to manage the producer connection

   `:out-ch` - Output channel used to receive messages from the Kafka producer and controller.
               This channel will be `nil` unless a non-empty `output-policy` set is specified.

   Configuration options:

   `:output-policy`: A set containing the output sources that should be returned via the `out-ch`.
                     Valid options include:

                     `:control`: Output from control operations
                     `:error`: Any exceptions or errors that occur
                     `:data`: Response from kafka producer after sending a message
                     
                     The Kafka Java interface returns each of this as a Java object. Gregor converts each
                     to pure data (i.e. a map).
  
   `:timeout`: time to wait, in milliseconds, for queued messages to send when closing the producer
  
   `:key-serializer`: Serializer to use for key serialization, default is `:edn`.
                      Valid serializers are `:edn`, `:string`, `:json` and `:keyword`
  
   `:value-serializer`: Serializer to use for value serialization, default is `:edn`.
                        Valid serializers are `:edn`, `:string` and `:json`
  
   `:input-buffer`: Buffer size of `:in-ch` and `ctl-ch`, default is 10
  
   `:output-buffer`: Buffer size of `:out-ch`, default is 100

   `:kafka-configuration`: Map containing kafka producer configuration settings.  This map will be converted
                           into key/value properties and passed directly to the KafkaProducer object.

   `:transducer`: Transformation function to be applied to all data received via `in-ch` channel, prior to being
                  posted to Kafka.

   Example Configuration:

   `{:kafka-configuration {:bootstrap.servers \"localhost:9092\"
                           :max.poll.recordss 1000}
     :output-policy #{:control :error}
     :key-serializer :string
     :value-serializer :json
     :input-buffer 20
     :output-buffer 50
     :timeout 100}`

   All key value pairs in `:kafka-configuration` will be converted and inserted into a property map and
   passed into the Kafka Java client as configuration options."

  [config]
  (let [{:keys [output-policy] :as context} (create-context config)]
    (input-loop context)
    (control-loop context)
    (if (any-output? output-policy)
      (select-keys context [:in-ch :out-ch :ctl-ch])
      (select-keys context [:in-ch :ctl-ch]))))
