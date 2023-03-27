;; convert the format in file ./schema/scheduler.json which is in edn to json
;; and write it to file ./schema/scheduler1.json
;; the format in file ./schema/scheduler.json is in edn
;; the format in file ./schema/scheduler1.json is in json

(ns util.convert
  (:require [clojure.data.json :as json]
            [clojure.java.io :as io]))

(defn convert [in out]
    (let [in (slurp in)
            out (json/write-str (json/read-str in))]
        (spit out out)))

(convert "./schema/scheduler.json" "./schema/scheduler1.json")
