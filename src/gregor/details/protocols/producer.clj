(ns gregor.details.protocols.producer)

(defprotocol ProducerProtocol
  "Protocol defining the required functions of a producer implementation"
  (send! [this record]
    "Produce a record on a topic.
     When using the single arity version, a map
     with the following keys is expected:
     `:key`, `:topic`, `:partition`, and `:value`.

     `:partition` is not required")
  (flush! [this]
    "Ensure that produced messages are flushed to their specified topics.")
  (init-transactions! [this]
    "https://kafka.apache.org/0110/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#initTransactions()")
  (begin-transaction! [this]
    "https://kafka.apache.org/0110/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#beginTransaction()")
  (commit-transaction! [this]
    "https://kafka.apache.org/0110/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#commitTransaction()"))
