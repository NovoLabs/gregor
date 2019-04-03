# Gregor

[![PyPI](https://img.shields.io/pypi/l/Django.svg?style=plastic)]()
[![CircleCI](https://circleci.com/gh/NovoLabs/gregor/tree/master.svg?style=svg)](https://circleci.com/gh/NovoLabs/gregor/tree/master)


> As Gregor Samsa awoke one morning from uneasy dreams he found himself transformed in his bed into a gigantic insect.

― Franz Kafka, _The Metamorphosis_

## Description

Gregor provides a channel-based API for asynchronously producing and consuming messages via a Kafka instance.  Through the use of transducers, Gregor allows the user to transform data on either the production or consumption side, or both if necessary.

Additionally, Gregor provides a control channel for interacting with the Kafka producer and consumer API.  While not being a complete reflection of the current Kafka Producer and Consumer objects, the operation set is functional for most standard uses of Kafka.  We can expose more control operations as users ask for them.

## Inspirations

Gregor was inspired by [kinsky](https://github.com/pyr/kinsky) and [ring](https://github.com/ring-clojure).

## Dependencies

Since Gregor is an interface to Kafka, using it requires that there is a Kafka instance set up.  To help you get up and running quickly, Gregor provides a `docker-compose.yaml` file that can be used to start up a local instance of Kafka.  Assuming you have [`docker-compose`](https://docs.docker.com/compose/install/) installed, you can copy [Gregor's docker compose file](https://github.com/NovoLabs/gregor/blob/master/docker/docker-compose.yaml) from GitHub to get a local instance of Kafka up and running.

Assuming you have copied Gregor's `docker-compose.yaml` file to your local directory (and `docker-compose` is installed), you can start Kafka with the following command:

```shell
$ docker-compose up
```

## Installation

To install Gregor, add the following to your Leiningen `:dependencies` vector:

```clojure
[codes.novolabs/gregor "0.1.0"]
```

## Usage

Gregor provides 2 interfaces: One for producing messages and the other for consuming them.

### Creating a consumer

To create a consumer, we first need to pull the consumer namespace into our REPL:

```clojure
(require '[gregor.consumer :as c])
```

Once that is done, assuming we have Kafka running on port 9092 on our local machine (default location if we used the `docker-compose.yaml` file provided by Gregor), we can create a consumer connection using the following code:

```clojure
user> (def consumer (c/create {:output-policy #{:data :control :error}
                               :topics :gregor.test
                               :kafka-configuration {:bootstrap.servers "localhost:9092"
                                                     :group.id "gregor.consumer.test"}}))
;;=> #'user/consumer
```

There are a few things going on here which warrant a closer examination.  First, lets look at the configuration map passed to `gregor.consumer/create`:

**`:output-policy`**

The value of `:output-policy` should be a set containing the list of events we want published to `out-ch`.  There are three types of events that Gregor publishes:

* `:data` - Data events are generated by messages read from the configured topic or topics.  This event type is always included in the `:output-policy` of consumers, regardless of what is specified in the user-supplied `:output-policy`.  Without data events, the consumer is not very useful.
* `:control` - Control events are generated by the output of control operations sent to the control channel.  We will talk more about the control channel and the supported operations below.
* `:error` - Error events are generated when errors occur, most commonly when an exception is thrown or invalid control operations are passed to the control channel.

**`:topics`**

The value of `:topics` can be a string, a keyword, a vector of strings and keywords or a regular expression:

* A string or keyword will subscribe the consumer to a single topic.
* A vector of strings and keywords will subscribe the consumer to each topic in the vector.
* A regular expression will subscribe the consumer to all topics matching the regular expression.  

You can check out the [KafkaConsumer documentation](https://kafka.apache.org/21/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html) for more information.

**`:kafka-configuration`**

The value of `:kafka-configuration` should be a map containing Kafka configuration options.  This map will be converted to a Java properties map and passed directly to the KafkaConsumer object during initialization.  The minimum requirements for consumer initialization are:

* `:bootstrap.servers` - A comma-delimited string containing `host:port` pairs inidicating the location of the Kafka server.
* `:group.id` - A string containing the group name of the consumer, which is used to maintain indexes and handle partitioning

You can check out the [Kafka documentation](https://kafka.apache.org/documentation/#configuration) for full treatment of all of the available configuration options.

The result of calling `gregor.consumer/create` is a map containing channels that are used to interact with the consumer.  

* `out-ch` - The channel that receives all events, including `:data`, `:control` and `:error`.
* `ctl-ch` - The channel that is used to send control operations

### Using a consumer

The result of the call to `gregor.consumer/create` is a map containing 2 channels.  For convenience sake, we are going to bind each of them to a symbol:

```clojure
user> (def out-ch (:out-ch consumer))
;;=> #'user/out-ch

user> (def ctl-ch (:ctl-ch consumer)) 
;;=> #'user/ctl-ch
```

`out-ch` is the channel that we use to receive all events generated by the consumer.  `ctl-ch` is the channel we use to send control operations to the consumer.  Before we create a producer and start generating data events on the `gregor.test` topic, lets spend some time learning about the supported control operations.  Here is a list:

### Producer

Once you have included Gregor in the list of dependencies in your project, fire up a REPL and execute the following to pull in the producer interface:

```clojure
(require '[gregor.producer :as p])
```

Assuming a local instance of Kafka was setup using the `docker-compose.yaml` file provided by Greogor, we can connect a producer to Kafka using the following code:

```clojure
(def producer (p/create {:output-policy #{:data :control :error}
                         :kafka-configuration {:bootstrap.servers "localhost:9092"}))
						 
;; The result of `gregor.producer/create` is a map containing the channels used to interact with the producer
(def in-ch (:in-ch producer))
(def out-ch (:out-ch producer))
(def ctl-ch (:ctl-ch producer))
```

## License

Copyright © 2019 NovoLabs, Inc.

Distributed under the BSD-3-clause LICENSE
