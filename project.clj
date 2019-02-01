(defproject gregor "0.1.0-SNAPSHOT"
  :description "FIXME: write description"

  :url "http://example.com/FIXME"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.4.490"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [cheshire "5.8.1"]
                 [environ "1.1.0"]]

  :profiles {:dev {:dependencies [[environ "1.1.0"]]
                   :env {:gregor-kafka-servers "localhost:9092"}}

             :test {:dependencies [[environ "1.1.0"]]
                    :env {:gregor-kafka-servers "localhost:9092"}}

             :uberjar {:aot :all}})
