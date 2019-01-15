(ns gregor.core-test
  (:require [clojure.test :refer :all]
            [gregor.core :refer :all]))

(deftest circle-ci-bootstrap
  (testing "bootstrapping of circle-ci"
    (is (= 1 1))))
