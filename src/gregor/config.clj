(ns gregor.config)

(defn default-producer
  "Creates a default configuration map"
  [bootstrap-servers]
  {:gregor.producer/kafka-config {:bootstrap.servers bootstrap-servers}})
