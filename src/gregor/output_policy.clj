(ns gregor.output-policy)

(defn control-output?
  "Returns `true` if we should send control output, `false` otherwise"
  [output-policy]
  (contains? output-policy :control))

(defn data-output?
  "Returns `true` if we should send data output, `false` otherwise"
  [output-policy]
  (contains? output-policy :data))

(defn error-output?
  "Returns `true` if we should send error output, `false` otherwise"
  [output-policy]
  (contains? output-policy :error))

(defn any-output?
  "Returns `true` if any output was requested, `false` otherwise"
  [output-policy]
  (or (control-output? output-policy)
      (data-output? output-policy)
      (error-output? output-policy)))

