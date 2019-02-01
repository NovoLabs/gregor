(ns gregor.config)

(defn default-producer
  "Creates a default configuration map"
  [bootstrap-servers]
  {:gregor.producer/input-buffer 10
   :gregor.producer/output-buffer 100
   :gregor.producer/timeout 200
   :gregor.producer/kafka-config {:bootstrap.servers bootstrap-servers}})
