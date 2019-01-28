(ns gregor.util
  (:require [environ.core :refer [env]]))

(defn ->int
  "Convert string to integer"
  [s]
  (if-let [digits (re-find #"\d+" s)]
    (java.lang.Integer/valueOf digits 10)))

(defn env-int
  "Reads environment variable using `kw` keyword, converting to an integer"
  [kw & [default]]
  (if-let [prop (env kw)]
    (->int prop)
    (cond
      (string? default)
        (->int default)

      (int? default)
        default

      :else nil)))
