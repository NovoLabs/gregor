(ns gregor.details.shared
  (:import java.util.Map))

(defn ^Map opts->props
  "Kafka configuration requres a map of string to string.
   This function converts a Clojure map into a Java Map, String to String"
  [opts]
  (into {} (for [[k v] opts] [(name k) (str v)])))
