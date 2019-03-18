(ns gregor.test-output-policy
  (:require [gregor.output-policy :as op]
            [clojure.test :refer :all]))

(def all #{:data :control :error})
(def data #{:data})
(def control #{:control})
(def error #{:error})
(def none #{})

(deftest output-policy
  (testing "data-output?"
    (is (op/data-output? all))
    (is (op/data-output? data))
    (is (not (op/data-output? control)))
    (is (not (op/data-output? error)))
    (is (not (op/data-output? none))))
  
  (testing "control-output?"
    (is (op/control-output? all))
    (is (not (op/control-output? data)))
    (is (op/control-output? control))
    (is (not (op/control-output? error)))
    (is (not (op/control-output? none))))

  (testing "error-output?"
    (is (op/error-output? all))
    (is (not (op/error-output? data)))
    (is (not (op/error-output? control)))
    (is (op/error-output? error))
    (is (not (op/error-output? none))))

  (testing "any-output?"
    (is (op/any-output? all))
    (is (op/any-output? data))
    (is (op/any-output? control))
    (is (op/any-output? error))
    (is (not (op/any-output? none)))))
