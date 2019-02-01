(ns gregor.producer-test
  (:require [clojure.test :refer :all]
            [gregor.producer :refer :all]))

(deftest circle-ci-bootstrap
  (testing "bootstrapping of circle-ci"
    (is (= 1 1))))
