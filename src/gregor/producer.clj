(ns gregor.producer
  (:require [gregor.details.producer :as impl]
            [gregor.details.protocols.producer :as iprod]
            [gregor.details.protocols.common-functions :as icf]
            [clojure.core.async :as a])
  (:import java.lang.IllegalStateException
           ))

(def default-input-buffer
  "Default number of messages on input channel"
  10)

(def default-output-buffer
  "Default number of messages on the output channel"
  100)

(def default-timeout
  "Default timeout in milli-seconds"
  100)

(defn input-loop
  "Starts the loop that processes input, waiting for the user to send input on `in` channel"
  [in out producer]
  (a/go-loop []
    (when-let [message (a/<! in)]
      (try
        (->> (iprod/send! producer message)
             deref
             impl/record-metadata->map
             (a/>! out))
        (catch Exception e
          (->> (impl/exception->map e)
               (a/>! out))))
      (recur))))

(defn control-loop
  "Starts the loop that process control commands from the user"
  [ctl out producer]
  (a/go-loop []
    (when-let [message (a/<! ctl)]
      (cond
        :else
          (a/>! out {:type :error :name :bad-command})))))

(defn create
  "Build a producer, returning a map that contains 3 channels:

   `:in`  - Input channel used to send messages to the producer. 
   `:out` - Output channel used to receive messages from the Kafka producer, including error information
   `:ctl` - Control channel used to manage the producer connection"
  ([config key-serializer value-serializer]
   (create (assoc config :gregor/key-serializer key-serializer :gregor/value-serializer value-serializer)))
  ([config serializer]
   (create (assoc config :gregor/key-serializer serializer :gregor/value-serializer serializer)))
  ([{:keys [gregor/input-buffer gregor/output-buffer]
     :or {input-buffer default-input-buffer output-buffer default-output-buffer}
     :as config}]
   (let [producer (impl/make-producer config)
         in (a/chan input-buffer)
         out (a/chan output-buffer)
         ctl (a/chan input-buffer)]
     (input-loop in out producer)
     (control-loop ctl out producer)
     {:in-ch in :out-ch out :control-ch ctl})))
