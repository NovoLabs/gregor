[![The Metamorphosis](https://i2.wp.com/prospectornow.com/wp-content/uploads/2015/02/The-Metamorphosis-e1424199006967.jpg?fit=300%2C433&ssl=1)](https://en.wikipedia.org/wiki/The_Metamorphosis)

# Gregor

[![PyPI](https://img.shields.io/pypi/l/Django.svg?style=plastic)]()
[![CircleCI](https://circleci.com/gh/NovoLabs/gregor/tree/master.svg?style=svg)](https://circleci.com/gh/NovoLabs/gregor/tree/master)

## Description

Gregor provides a channel-based API for asynchronously producing and consuming messages via a Kafka instance.  Through the use of transducers, Gregor allows the user to transform data on either the production or consumption side, or both if necessary.

Additionally, Gregor provides a control channel for interacting with the Kafka producer and consumer API.  While not being a complete reflection of the current Kafka Producer and Consumer objects, the operation set is functional for most standard uses of Kafka.  Furthermore, we are planning on exposing additional functions per user requests.

## Installation

To install Gregor, add the following to your Leiningen `:dependencies` vector:

```clojure
[[codes.novolabs/gregor "0.1.0"]]
```

## Dependencies

Since Gregor is an interface to Kafka, using it requires that there is a Kafka instance set up.

## Usage

Gregor has provides 2 interfaces, one for production of Kafka messages, the other for consumption.  We will visit each of these interfaces, starting with the production interface.

### Production



## License

Copyright © 2019 NovoLabs, Inc.

Distributed under the BSD-3-clause LICENSE
