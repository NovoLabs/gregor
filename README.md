# Gregor

[![PyPI](https://img.shields.io/pypi/l/Django.svg?style=plastic)]()
[![CircleCI](https://circleci.com/gh/NovoLabs/gregor/tree/master.svg?style=svg)](https://circleci.com/gh/NovoLabs/gregor/tree/master)


> As Gregor Samsa awoke one morning from uneasy dreams he found himself transformed in his bed into a gigantic insect.

― Franz Kafka, _The Metamorphosis_

## Description

Gregor provides a channel-based API for asynchronously producing and consuming messages from [Apache Kafka](https://kafka.apache.org/).  Through the use of transducers, Gregor allows the user to transform data during production or consumption or both.

Gregor provides a control channel for interacting with the producer and consumer APIs.  Though it is not a complete implementation of the current Kafka producer and consumer objects, the current set of operations supports most use cases.  Additional control operations will be implemented as necessary.

## Inspirations

Gregor was inspired by [kinsky](https://github.com/pyr/kinsky) and [ring](https://github.com/ring-clojure).

## Dependencies

Since Gregor is an interface to Kafka, using it requires a Kafka instance.  To help you get up and running quickly, Gregor provides a `docker-compose.yaml` file that can be used to start up a local instance of Kafka.  Assuming you have [`docker-compose`](https://docs.docker.com/compose/install/) installed, you can copy [Gregor's docker compose file](https://github.com/NovoLabs/gregor/blob/master/docker/docker-compose.yaml) from GitHub to your local machine.

Once you copy Gregor's `docker-compose.yaml` file to your local machine (and `docker-compose` is installed), you can start Kafka with the following command:

```shell
$ docker-compose up
```

## Installation

To install Gregor, add the following to your Leiningen `:dependencies` vector:

```clojure
[codes.novolabs/gregor "0.1.0"]
```

## TL;DR Examples

If you want to get started quickly, here are some self-contained examples that you can copy-pasta into your REPL.

### Simple Message Production and Consumption

```clojure
(require '[clojure.core.async :as a])
(require '[gregor.consumer :as c])
(require '[gregor.producer :as p])

;; Create a consumer
(def consumer (c/create {:output-policy #{:data :control :error}
                         :kafka-configuration {:bootstrap.servers "localhost:9092"
                                               :group.id "gregor.consumer.test"}
                         :topics :gregor.test}))

;; Create a producer
(def producer (p/create {:output-policy #{:error}
                         :kafka-configuration {:bootstrap.servers "localhost:9092"}}))

;; Bind the input channel of the producer to `in-ch`
(def in-ch (:in-ch producer))

;; Bind the output channel of the consumer to `out-ch`
(def out-ch (:out-ch consumer))

;; Create a go-loop to print messages received on the output channel of the consumer
(a/go-loop []
  (when-let [msg (a/<! out-ch)]
    (println (pr-str msg))
    (recur)))

;; Post 2 messages to the input channel of the producer
(a/>!! in-ch {:topic :gregor.test :message-value {:a 1 :b 2}})
(a/>!! in-ch {:topic :gregor.test :message-value {:a 3 :b 4}})

;; Close the producer
(a/>!! (:ctl-ch producer) {:op :close})

;; Close the consumer
(a/>!! (:ctl-ch consumer) {:op :close})
```

### Producer Transducer

```clojure
(require '[clojure.core.async :as a])
(require '[gregor.consumer :as c])
(require '[gregor.producer :as p])

;; Function to add a `:producer-timestamp`
(defn add-timestamp
  [m]
  (assoc m :producer-timestamp (.toEpochMilli (java.time.Instant/now)))

;; Function to set the `:message-value`
(defn add-message-value
  [m]
  (->> (dissoc m :uuid)
       (assoc m :message-value)))

;; Function to set the `:message-key`
(defn add-message-key
  [{:keys [uuid] :as m :or {uuid (java.util.UUID/randomUUID)}}]
  (-> (assoc m :message-key {:uuid uuid})
      (dissoc :uuid)))

;; Function to set the `:topic`
(defn add-topic
  [topic m]
  (assoc m :topic topic))
	   
;; Compose the transducer which will reshape the message into something
;; that the Gregor producer understands
(def transducer (comp (map add-timestamp)
                      (map add-message-value)
                      (map add-message-key)
                      (map (partial add-topic :gregor.test.send))))

;; Create a consumer
(def consumer (c/create {:output-policy #{:data :control :error}
                         :kafka-configuration {:bootstrap.servers "localhost:9092"
                                               :group.id "gregor.consumer.test"}
                         :topics :gregor.test}))

;; Create a producer with the transducer from above
(def producer (p/create {:output-policy #{:error}
                         :kafka-configuration {:bootstrap.servers "localhost:9092"}
						 :transducer transducer}))

;; Bind the input channel of the producer to `in-ch`
(def in-ch (:in-ch producer))

;; Bind the output channel of the consumer to `out-ch`
(def out-ch (:out-ch consumer))

;; Post 2 messages to the input channel of the producer.  Note that they
;; are not currently of the correct shape.  The transducer on the input
;; channel will handle this for us
(a/>!! in-ch {:a 1 :b 2})
(a/>!! in-ch {:a 3 :b 4})

;; Close the producer
(a/>!! (:ctl-ch producer) {:op :close})

;; Close the consumer
(a/>!! (:ctl-ch consumer) {:op :close})
```

## Usage

Gregor provides 2 namespaces public namespaces: `gregor.consumer` for creating and using a KafkaConsumer and `gregor.producer` for creating and using a KafkaProducer.

### Creating a Consumer

To create a consumer, we first need to pull the consumer namespace into our REPL:

```clojure
(require '[gregor.consumer :as c])
```

Assuming we have Kafka running on port 9092 on our local machine (the default location if we used the `docker-compose.yaml` file provided by Gregor), we can create a consumer connection using the following code:

```clojure
(def consumer (c/create {:output-policy #{:data :control :error}
                         :topics :gregor.test
                         :kafka-configuration {:bootstrap.servers "localhost:9092"
                                               :group.id "gregor.consumer.test"}}))
;; => #'user/consumer
```

Lets take a closer look at the configuration map passed to `gregor.consumer/create`:

**`:output-policy`**

The value of `:output-policy` should be a set containing the types of events we want published to `out-ch`.  There are four types of events that Gregor supports:

* `:data` - Data events are generated by messages read from the configured topic or topics.  This event type is always included in a consumer's `:output-policy`, regardless of what is specified in the user-supplied `:output-policy`.  Without data events, the consumer would not be very useful.
* `:control` - Control events are generated by the output of control operations sent to the control channel.  We will talk more about the control channel and the supported operations below.
* `:error` - Error events are generated when errors occur, most commonly when an exception is thrown or invalid control operations are passed to the control channel.
* `:eof` - EOF events are sent when the consumer is closed by invoking the `:close` control operation.  Like `:data` events, the `:eof` event is always included as part of the `:output-policy` of a consumer.

**`:topics`**

The value of `:topics` can be a string, a keyword, a vector of strings and keywords or a regular expression:

* A string or keyword will subscribe the consumer to a single topic.  The string or keyword should be a [valid Kafka topic](https://stackoverflow.com/questions/37062904/what-are-apache-kafka-topic-name-limitations).
* A vector of strings and keywords will subscribe the consumer to each topic in the vector.  The vector can contain both strings and keywords.
* A regular expression will subscribe the consumer to all topics matching the regular expression.

You can check out the [KafkaConsumer documentation](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) for more information.

**`:kafka-configuration`**

The value of `:kafka-configuration` should be a map containing Kafka configuration options.  This map will be converted to a Java properties map and passed directly to the KafkaConsumer object during initialization.  The minimum requirements for consumer initialization are:

* `:bootstrap.servers` - A comma-delimited string containing `host:port` pairs inidicating the location of the Kafka server.
* `:group.id` - A string containing the group id of the consumer, which is used to maintain indexes and handle partitioning

You can check out the [Kafka consumer configuration documentation](https://kafka.apache.org/documentation/#consumerconfigs) for full treatment of all of the available configuration options.

### Consumer Control Operations

Assuming correct configuration, the result of calling `gregor.consumer/create` is a map containing 2 keys:

* `:out-ch` - The channel that receives all events, including `:data`, `:control`, `:error` and `:eof`.
* `:ctl-ch` - The channel that is used to send control operations

For the sake of conveneince, lets bind `out-ch` to the output channel and `ctl-ch` to the control channel:

```clojure
(def out-ch (:out-ch consumer))
;; => #'user/out-ch

(def ctl-ch (:ctl-ch consumer)) 
;; => #'user/ctl-ch
```

Now, lets check if we have been properly subscribed to the `gregor.test` topic.  We can query for the list of current subscriptions using the `:subscriptions` control operation.  The output will be written to `out-ch` as a `:control` event.  Here is the code:

```clojure
(require '[clojure.core.async :as a])

(a/>!! ctl-ch {:op :subscriptions})
;; => true

(-> (a/<!! out-ch) pr-str println)
;; => {:op :subscriptions, :subscriptions ["gregor.test"], :event :control}
```

This is a common pattern in Gregor:  The operation is submitted to the control channel and any results are pushed to the output channel as a `:control` event.

Lets look at each control operation that the consumer supports:

**`:subscribe`**

The `:subscribe` operation is used to subscribe to a topic(s).  Since Gregor requires that the initial topic(s) be passed in during initialization, the `:subscribe` operation will generally only be used if we wish to change the subscription of our consumer.  Should you need to do this, please remember that per the [KafkaConsumer documentation](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html), **topic subscriptions are not incremental**.  If you wish to remain subscribed to the current list of topics, you will need to include these along with any additions when you call the `:subscribe` operation.

The code to call `:subscribe` looks like this:

```clojure
;; Subscribe to `:gregor.test.2`
(a/>!! ctl-ch {:op :subscribe :topics :gregor.test.2})
;; => true

;; Print the output from the `:subscribe` control operation
(-> (a/<!! out-ch) pr-str println)
;; => {:op :subscribe, :topics :gregor.test.2, :event :control}

;; Query for the list of current subscriptions using the `:subscriptions` operation
(a/>!! ctl-ch {:op :subscriptions})
;; => true

;; Print the output of the `:subscriptions` operation, noting that we are
;; now subscribed to the "gregor.test.2" topic
(-> (a/<!! out-ch) pr-str println)
;; => {:op :subscriptions, :subscriptions ["gregor.test.2"], :event :control}

;; Switch subscription back to "gregor.test"
(a/>!! ctl-ch {:op :subscribe :topics :gregor.test})
;; => true

;; Print the output of the `:subscribe` command
(-> (a/<!! out-ch) pr-str println)
;; => {:op :subscribe, :topics :gregor.test, :event :control}
```

**`:subscriptions`**

The `:subscriptions` operation, as we have seen, is used to get the list of topics that the consumer is currently subscribed to.  We have already seen how to call the `:subscriptions` operation above but we include the code here for the sake of completness:

```clojure
;; Query for the list of current subscriptions using the `:subscriptions` operation
(a/>!! ctl-ch {:op :subscriptions})
;; => true

;; Print the output of the `:subscriptions` operation
(-> (a/<!! out-ch) pr-str println)
;; => {:op :subscriptions, :subscriptions ["gregor.test"], :event :control}
```

If the consumer is not subscribed to a topic(s), the `:subscriptions` operation will return an empty vector

**`:unsubscribe`**

The `:unsubscribe` operation will unsubscribe the consumer from all current topic subscriptions.  It can be invoked using the following code:

```clojure
;; Unsubscribe from the current topic
(a/>!! ctl-ch {:op :unsubscribe})
;; => true

;; Print the output of the `:unsubscribe` operation
(-> (a/<!! out-ch) pr-str println)
;; => {:op :unsubscribe, :event :control}
```

If you are switching betwen a name-based subscription (i.e. strings or keywords) to a pattern-based subscription (i.e. a regular expression), you must unsubscribe.  If you want to change from one name-based subscription to another or one pattern-based subscription to another, you do not need to unsubscribe.

**`partitions-for`**

The `:partitions-for` operation will fetch the partition information for the specified topic.  You can use the following code to test `:partitions-for` for the `"gregor.test"` topic:

```clojure
;; Query the partitions for the `:gregor.test` topic
(a/>!! ctl-ch {:op :partitions-for :topic :gregor.test})
;; => true

;; Print the output of the `:partitions-for` operation for the `:gregor.test` topic
(-> (a/<!! out-ch) pr-str println)
;; => {:op :partitions-for, 
;;     :topic :gregor.test, 
;;     :partitions [{:type-name :partition-info, 
;;                   :isr [{:type-name :node, :host "127.0.0.1", :id 1, :port 9092}], 
;;                   :offline [], 
;;                   :leader {:type-name :node, :host "127.0.0.1", :id 1, :port 9092}, 
;;                   :partition 0, 
;;                   :replicas [{:type-name :node, :host "127.0.0.1", :id 1, :port 9092}], 
;;                   :topic "gregor.test"}], 
;;     :event :control}
```

**`commit`**

The `:commit` operation will commit the offsets from the last call to poll for the subscribed list of topics.  The default value of the `enable.auto.commit` property in Kafka is `true`, which means that as messages are consumed, the offset will be committed automatically (the interval of auto commits is controlled by the `auto.commit.interval.ms` property which has a default value of `500`).  If you set `enable.auto.commit` to `false` when you create your consumer you will need to manually commit consumed offsets, which can be done with the following code:

```clojure
;; Commit the last consumed offset for this consumer
(a/>!! ctl-ch {:op :commit})
;; => true

;; Verify that the `:commit` operation was processed succesfully
(-> (a/<!! out-ch) pr-str println)
;; => {:op :commit, :event :control}
```

Leaving `enable.auto.commit` set to the default value of `true` is sufficient for most use cases.  Read [this Medium article](https://medium.com/@danieljameskay/understanding-the-enable-auto-commit-kafka-consumer-property-12fa0ade7b65) for more information about auto committing and offsets.

**`:close`**

The `:close` operation will close the consumer, including all associated channels.  The following code will close the consumer we have been working with:

```clojure
;; Close the consumer
(a/>!! ctl-ch {:op :close})
;; true

;; Verify the `:close` operation was processed
(-> (a/<!! out-ch) pr-str println)
;; => {:op :close, :event :control}

;; Should receive `:eof` event as the final event indicating the end of the stream/channel
(-> (a/<!! out-ch) pr-str println)
;; => {:event :eof}

;; Further reads result in `nil` as the `out-ch` is closed
(-> (a/<!! out-ch) pr-str println)
;; => nil

;; Attempts to write to the `ctl-ch` will fail, returning `false`
(a/>!! ctl-ch {:op :subscriptions})
;; => false
```

Not only was the `KafkaConsumer` object closed, but so were `out-ch` and `ctl-ch` channels.  Since `out-ch` is a standard `core.async` channel, all messages that were read from the consumer up to the point of the `:close` operation will be avialable.  Once `out-ch` is empty, it wil return `nil` for all future reads.  Any attempts to write to `ctl-ch` will fail, returning `false` as shown above.

Note that the final message deliverd from `out-ch` was the `:eof` event.  Posting an `:eof` event is Gregor's way of telling the user that the stream has been closed.

### Creating a Producer

The other half of the equation is the producer.  Gregor provides a namespace called `gregor.producer` for creating and interacting with a KafkaProducer object.  The following code can be used to create a producer:

```clojure
(def producer (p/create {:output-policy #{:data :control :error}
                         :kafka-configuration {:bootstrap.servers "localhost:9092"}}))
;; => #'user/producer
```

As with the consumer, the configuration map we passed to `gregor.producer/create` is worth a closer look:

**`:output-policy`**

As with the consumer, the `:output-policy` is a set containing the types of events we want published to `out-ch`.  There are four types, the same as with the consumer.  However, the requirments and meaning is different in the context of a producer:

* `:data`: - Data events are generated by serializing the result of the call to the producer's `.send` function.  Note that serializing the result of `.send` is synchronous and, as such, including `:data` as part of a producer's output policy may affect throughput.
* `:control`: - Similar to the consumer, control events are generated by the output of control operations sent to the control channel.
* `:error`: - Similar to the consumer, error events are generated when errors occur, most commonly when an exception is thrown or invalid control operations are passed to the control channel.
* `:eof`: - EOF events are sent when the producer is closed by invoking the `:close` control operation.  The `:eof` event is always included as part of the `:output-policy` of a producer.

**`:kafka-configuration`**

The value of `:kafka-configuration` should be a map containing Kafka configuration options.  This map will be converted to a Java properties map and passed directly to the KafkaConsumer object during initialization.  The minimum requirements for consumer initialization are:

* `:bootstrap.servers` - A comma-delimited string containing `host:port` pairs inidicating the location of the Kafka server.

You can check out the [Kafka producer configuration documentation](https://kafka.apache.org/documentation/#producerconfigs) for a full treatment of all of the available configuration options.

### Producer Control Operations

Assuming correct configuration, the result of calling `gregor.producer/create` is a map containing 3 keys:

* `:in-ch` - The channel used to send `:data` events to the Kafka producer
* `:out-ch` - The channel that receives all events, including `:data`, `:control`, `:error` and `:eof`
* `:ctl-ch` - The channel used to send control operations

For the sake of conveneince, lets bind `out-ch` to the output channel and `ctl-ch` to the control channel:

```clojure
(def out-ch (:out-ch producer))
;; => #'user/out-ch

(def ctl-ch (:ctl-ch producer))
;; => #'user/ctl-ch
```

The producer currently supports three control operations:

**`:partitions-for`** 

The `:partitions-for` operation will fetch the partition information for the specified topic.  You can use the following code to test `:partitions-for` for the `"gregor.test"` topic:

```clojure
;; Query the partitions for the `:gregor.test` topic
(a/>!! ctl-ch {:op :partitions-for :topic :gregor.test})
;; => true

;; Print the output of the `:partitions-for` operation for the `:gregor.test` topic
(-> (a/<!! out-ch) pr-str println)
;; => {:op :partitions-for, 
;;     :topic :gregor.test, 
;;     :partitions [{:type-name :partition-info, 
;;                   :isr [{:type-name :node, :host "127.0.0.1", :id 1, :port 9092}], 
;;                   :offline [], 
;;                   :leader {:type-name :node, :host "127.0.0.1", :id 1, :port 9092}, 
;;                   :partition 0, 
;;                   :replicas [{:type-name :node, :host "127.0.0.1", :id 1, :port 9092}], 
;;                   :topic "gregor.test"}], 
;;     :event :control}
```

**`:flush`**

Invoking the `:flush` operation will make all buffered records immediately available to send.  Note that `:flush` will cause the producer thread to block until all buffered records have been sent.  You can invoke `:flush` with the following code:

```clojure
;; Invoke flush via the control channel
(a/>!! ctl-ch {:op :flush})
;; => true

;; Print the output of the `:flush` operation
(-> (a/<!! out-ch) pr-str println)
;; => {:op :flush, :event-ctl}
```

**`:close`**

The `:close` operation will close the producer, including all associated channels.  The following code will close the producer we have been working with:

```clojure
;; Close the consumer
(a/>!! ctl-ch {:op :close})
;; true

;; Verify the `:close` operation was processed
(-> (a/<!! out-ch) pr-str println)
;; => {:op :close, :event :control}

;; Should receive `:eof` event as the final event indicating the end of the stream/channel
(-> (a/<!! out-ch) pr-str println)
;; => {:event :eof}

;; Further reads result in `nil` as the `out-ch` is closed
(-> (a/<!! out-ch) pr-str println)
;; => nil

;; Attempts to write to the `ctl-ch` will fail, returning `false`
(a/>!! ctl-ch {:op :subscriptions})
;; => false

;; Attempts to write to the `in-ch` will fail, returning `false`
(a/>!! (:in-ch producer) {:foo :bar})
;; => false
```

Not only was the `KafkaProducer` object closed, but so were `in-ch`, `out-ch` and `ctl-ch` channels.  Since `out-ch` is a standard `core.async` channel, all messages that were read from the producer up to the point of the `:close` operation will be avialable.  Once `out-ch` is empty, it wil return `nil` for all future reads.  Any attempts to write to `ctl-ch` will fail, returning `false` as shown above.

Note that the final message deliverd from `out-ch` was the `:eof` event.  Posting an `:eof` event is Gregor's way of telling the user that the stream has been closed.

### Sending and Receiving Data

Now that we have seen how to create and work with a consumer and producer in isolation, it is time for them to work together to send some data across a Kafka topic.  If you have not already done so, close the consumer and producer that you created above.  You can us the following code to do so:

```clojure
;; Close the consumer
(-> (:ctl-ch consumer) (a/>!! {:op :close}))
;; => true

;; Close the producer
(-> (:ctl-ch :producer) (a/>!! {:op :close}))
;; => true
```

Once the old consumer and producer are closed, we can create new instances of a consumer and a producer which we will use to send a message via Kafka:

```clojure
;; Create a consumer, subscribed to the `:gregor.test.send` topic
(def consumer (c/create {:output-policy #{:data :control :error}
                         :topics :gregor.test.send
                         :kafka-configuration {:bootstrap.servers "localhost:9092"
                                               :group.id "gregor.consumer.test"}}))
;; => #'user/consumer

;; Create a producer
(def producer (p/create {:output-policy #{:control :error}
                         :kafka-configuration {:bootstrap.servers "localhost:9092"}}))
;; => #'user/producer
```

A couple of things to note here.  First, we subscribed to a different topic than in the previous consumer example.  This is simply to make sure there are no messages hanging out on a previously existing topic and we are starting with a clean slate.  Second, we removed the `:data` event from the `:output-policy` for the producer.  This will keep Gregor from dereferencing the result of the call to `.send`, thus maintaining the asynchronous-ness of message production.

Now that we have a valid consumer and producer, we can send messages between them:

```clojure
;; Bind a symbol to the input channel of the producer
(def in-ch (:in-ch producer))
;; => #'user/in-ch

;; Bind a symbol to the output channel of the consumer
(def out-ch (:out-ch consumer))
;; => #'user/out-ch

;; Create message body (we will examine this closer in the next section)
(def msg {:topic :gregor.test.send :message-key {:uuid (str (java.util.UUID/randomUUID))} :message-value {:a 1 :b 2}})
;; => #'user/msg

;; Write data to the producer
(a/>!! in-ch msg)
;; => true

;; Read the message from the consumer
(a/<!! out-ch)
;; => {:message-key {:uuid "5380d92d-5997-4515-99ed-1717bf21a01e"},
;;     :offset 2,
;;     :message-value-size 12,
;;     :topic "gregor.test.send",
;;     :message-key-size 46,
;;     :partition 0,
;;     :message-value {:a 1, :b 2},
;;     :event :data,
;;     :type-name :consumer-record,
;;     :timestamp 1554751618612}
```

As you may have noticed, the message we get out of the consumer has a bit more information than what we passed into the producer.  This is because Gregor includes all of the data provided by the Kafka [`ConsumerRecord`](https://kafka.apache.org/21/javadoc/org/apache/kafka/clients/consumer/ConsumerRecord.html) object.  Most of the time, we will only be interested in the `:message-key` and `:message-value` keys, which contain the data.

### Anatomy of a Message

As promised, lets look a little closer at the keys and values of a message body.  There are 2 required keys, the `:topic` which contains a string or keyword containing the message's target topic, and the `:message-value`, which is the data that we want to send.  Additionally, there are 2 optional keys.  The first is `:message-key` which is arbitrary metadata about the `:message-value`.  The second is `:partition`, which is a valid partition index of the topic where the message should be delivered.

All messages must contain a `:topic` and a `:message-value`.  We consider it good practice to also include a `:message-key` with each message, though it is not required by Kafka or Gregor.  Generally speaking, we try to avoid specifying a partition and let Kafka handle distributing the work across available partitions.  However, if you have a good reason, you can target a specific partition within the specified topic.

### Producer Transducer

Gregor supports producer transducers.  This feature allows the user to specify a transducer that will be attached to the input channel of a producer when upon creation.  The transducer will be applied to each message sent to the input channel before it is serialized as a Kafka [`ProducerRecord`](https://kafka.apache.org/21/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html).

In this example, we will use a transducer to derive the `:message-key` and `:message-value` from the map that is passed into the input channel.  In addition, the transducer will add a `:producer-timestamp` to the `:message-value` for use by the consumer:

```clojure
;; Function to add a `:producer-timestamp`
(defn add-timestamp
  [m]
  (assoc m :producer-timestamp (.toEpochMilli (java.time.Instant/now)))

;; Function to set the `:message-value`
(defn add-message-value
  [m]
  (->> (dissoc m :uuid)
       (assoc m :message-value)))

;; Function to set the `:message-key`
(defn add-message-key
  [{:keys [uuid] :as m}]
  (-> (assoc m :message-key {:uuid uuid})
      (dissoc :uuid)))

;; Function to set the `:topic`
(defn add-topic
  [topic m]
  (assoc m :topic topic))
	   
;; Compose the transducer
(def transducer (comp (map add-timestamp)
                      (map add-message-value)
                      (map add-message-key)
                      (map (partial add-topic :gregor.test.send))))
```

In addition to adding some meta-data about the message, this transducer transforms the message into something that Gregor can understand.  Every message that passes through the input channel will come out the other side with a `:topic`, a `:message-value` and a `:message-key`, guarenteeing that all messages are correctly shaped.

(As a side note, since the `:message-key` is not required, one possible enhancement to the transducer is to leave out the `:message-key` if no UUID is found in the map.

Alternatively, we could generate a UUID if one is not found, similar to how we generated a timestamp.  

The correct answer to these types of questions will largely be contextual to the problem that is being solved.  Suffice to say, Gregor's ability to accept a user-defined transducer should facilitate an elegant implementation regardless of what the requirements dictate.)

Now that we have our transducer, lets put it to work:

```clojure
;; Close the previous producer
(a/>!! ctl-ch {:op :close})
;; => true

;; Create a new producer with the above defined transducer
(def producer (p/create {:output-policy #{:control :error}
                         :kafka-configuration {:bootstrap.servers "localhost:9092"}
						 :transducer transducer}))
;; => #'user/producer

;; Send data to the producer.  Note that we have not specified the `:topic` or the `:message-value`.
(a/>!! (:in-ch producer) {:uuid (java.util.UUID/randomUUID) :a 1 :b 2 :c 3})
;; => true

;; Read the message from the consumer
(a/<!! out-ch)
;; => {:message-key {:uuid #uuid "fd32a035-153d-4fe8-837e-d5bd33985cd0"},
;;     :offset 3,
;;     :message-value-size 53,
;;     :topic "gregor.test.send",
;;     :message-key-size 52,
;;     :partition 0,
;;     :message-value {:a 1, :b 2, :c 3, :producer-timestamp 1554755327399},
;;     :event :data,
;;     :type-name :consumer-record,
;;     :timestamp 1554755327411}
```

As you can see, the transducer was applied to the input channel of the producer, resulting in a correctly shaped message which was then read off of the topic by the consumer.

## TODO List

#### Transducer Exception Handling

Channels with transducers support having an exception handling function.  Need to create one so that any exceptions that a transducer throws are caught and converted to `:error` events.  In the case of the consumer, the event will automatically be sent to the `out-ch`.  In the case of the producer, the transducer is on the `in-ch` so Gregor will need to recognize that an error ocurred (by checking the event type) and route it to the producer's `out-ch`.

#### Exception handling for control events

Review the exceptions that the various `KafkaProducer` and `KafkaConsumer` methods can throw.  Make sure that Gregor has exception handling for each.  The exception handling should convert the exception to an `:error` event and post it to the `out-ch` of the consumer or producer.  Additionally, in the context of the consumer, the consumer control loop must be left in a correct state.  That is, after the exception is handled and routed correctly, a message needs to be sent to the `ctl-ready-ch` indicating that the control loop is ready for the next control operation.

#### Enhance `:commit` Control Operation

Currently, the `:commit` control operation takes no parameters.  It simply forces a commit to happen for all partitions for each of the currently subscribed topics.  To facilitate a finer control for the user over commits, the `:commit` operation should be able to target specific partitions of specific topics with specific offsets.  Doing so will allow the user to disable auto-commits and be explicit about committing when a message is handled.

## License

Copyright © 2019 NovoLabs, Inc.

Distributed under the BSD-3-clause LICENSE
