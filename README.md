[![The Metamorphosis](https://i2.wp.com/prospectornow.com/wp-content/uploads/2015/02/The-Metamorphosis-e1424199006967.jpg?fit=300%2C433&ssl=1)](https://en.wikipedia.org/wiki/The_Metamorphosis)

# Gregor

[![PyPI](https://img.shields.io/pypi/l/Django.svg?style=plastic)]()
[![CircleCI](https://circleci.com/gh/NovoLabs/gregor/tree/master.svg?style=svg)](https://circleci.com/gh/NovoLabs/gregor/tree/master)

## Description

Gregor provides a channel-based API for asynchronously producing and consuming messages via a Kafka instance.  Through the use of transducers, Gregor allows the user to transform data on either the production or consumption side, or both if necessary.

Additionally, Gregor provides a control channel for interacting with the Kafka producer and consumer API.  While not being a complete reflection of the current Kafka Producer and Consumer objects, the operation set is functional for most standard uses of Kafka.  We can expose more control operations as users ask for them.

## Inspirations

Gregor was inspired by [kinsky](https://github.com/pyr/kinsky) and [ring](https://github.com/ring-clojure).

## Dependencies

Since Gregor is an interface to Kafka, using it requires that there is a Kafka instance set up.  To help you get up and running quickly, Gregor provides a `docker-compose.yaml` file that can be used to start up a local instance of Kafka.  Assuming you have [`docker-compose`](https://docs.docker.com/compose/install/) installed, you can copy [Gregor's docker compose file](https://github.com/NovoLabs/gregor/blob/master/docker/docker-compose.yaml) from GitHub to get a local instance of Kafka up and running.

## Installation

To install Gregor, add the following to your Leiningen `:dependencies` vector:

```clojure
[[codes.novolabs/gregor "0.1.0"]]
```

## Usage

Gregor provides 2 interfaces: One for producing messages and the other for consuming them.

### Consumer

First, we are going to set up a basic consumer.  First we need to pull the consumer namespace into our REPL:

```clojure
(require '[gregor.consumer :as c])
```

Once that is done, assuming we have Kafka running on port 9092 on our local machine (which we will if we used the `docker-compose.yaml` file provided by Gregor), we can create a consumer connection using the following code:

```clojure
user> (def consumer (c/create {:output-policy #{:data :control :error}
                               :topics :gregor.test
						       :kafka-configuration {:bootstrap.servers "localhost:9092"
						                             :group.id "gregor.consumer.test"}}))
=> #'user/consumer

user> (def out-ch (:out-ch consumer))
#'user/out-ch

user> (def ctl-ch (:ctl-ch consumer)) 
#'user/ctl-ch
```

There are a few things going on here which are worth a closer examination.  First, lets look at the configuration map passed to `gregor.consumer/create`.  It has a few different keys which are worth talking about:

| *key* | *value* | *notes* |
|:----- |:-------:|:------- |
| `:output-policy` | `#{:data :control :error}` | Indicates which events will be sent to `out-ch`.  `:data` is not required as it is always sent for a consumer |
| `:topics` | `:gregor.test` | `:topics` can be a single topic, represented as a keyword or string, a vector of topics, represented as keywords or strings, or a regular expression |
| <img width="200" height="0"/> | <img width="300" height="0"/> | <img height="0"/> |

The result of `gregor.consumer/create` is a map containing channels that will be used to interact with the consumer.  `out-ch` is the channel that receives all data events corresponding to the configured topic, `gregor.test` in this example.  The `out-ch` channel is also receives control and error events, provided the `output-policy` is configured to send those events.

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
