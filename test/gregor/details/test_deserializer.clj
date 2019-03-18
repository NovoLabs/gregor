(ns gregor.details.test-deserializer
  (:require [gregor.details.deserializer :as des]
            [gregor.details.serializer :as ser]
            [cheshire.core :as json]
            [clojure.test :refer :all]))

(def payload {:string "string"
              :integer 1
              :decimal 3.14
              :keyword :keyword
              :vector ["string" 1 3.14 :keyword]
              :list '("string" 1 3.14 :keyword)
              :map {:string "string"
                    :integer 1
                    :decimal 3.14
                    :keyword :keyword}
              :set #{"string" 1 3.14 :keyword}})

(def json-payload (json/parse-string (json/generate-string payload) true))

(def topic "gregor.test")

(deftest deser-roundtrip
  (testing "DeSer Round-trip of EDN"
    (let [s (ser/->serializer :edn)
          d (des/->deserializer :edn)]
      (is (= payload (->> (.serialize s topic payload) (.deserialize d topic))))))

  (testing "DeSer Round-trip of JSON"
    (let [s (ser/->serializer :json)
          d (des/->deserializer :json)]
      (is (= json-payload (->> (.serialize s topic payload) (.deserialize d topic))))))

  (testing "DeSer Round-trip of Keyword"
    (let [s (ser/->serializer :keyword)
          d (des/->deserializer :keyword)]
      (is (= :keyword (->> (.serialize s topic :keyword) (.deserialize d topic))))))

  (testing "DeSer Round-trip of String"
    (let [s (ser/->serializer :string)
          d (des/->deserializer :string)]
      (is (= "thisIsAString" (->> (.serialize s topic "thisIsAString") (.deserialize d topic)))))))
