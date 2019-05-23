(defproject novolabs/gregor "0.1.0"
  :description "Clojure client library for sending and receiving messages via Kafka"

  :url "https://github.com/NovoLabs/gregor"

  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.4.490"]
                 [org.apache.kafka/kafka-clients "2.1.0"]
                 [camel-snake-kebab "0.4.0"]
                 [cheshire "5.8.1"]]

  :test-selectors {:default (complement :integration)
                   :integration :integration
                   :all (constantly true)}

  :release-tasks [["vcs" "assert-committed"]
                  ["v" "update"] ;; compute new version & tag it
                  ["v" "push-tags"]
                  ["deploy" "clojars"]]
  
  :profiles {:uberjar {:aot :all}})
