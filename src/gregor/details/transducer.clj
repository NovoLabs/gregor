(ns gregor.details.transducer
  (:require '[gregor.details.transform :as t]))

(defn ex-handler
  "Exception handler for transducers.  Converts exception into an `:error` event"
  [e]
  (->> (t/exception->data e)
       (t/->event :error)))
