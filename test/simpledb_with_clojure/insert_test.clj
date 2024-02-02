(ns simpledb-with-clojure.insert-test
  (:require
   [clojure.test :as t]
   [simpledb-with-clojure.core :as simpledb]
   [simpledb-with-clojure.proto :as simpledb-proto]
   [taoensso.timbre :as timbre]))

(t/deftest Insert-Test
  (let [db (simpledb/start-db)]
    ;; inserting a simple map
    (timbre/info "---case #1---")
    (simpledb-proto/insert! db {:id         1
                                :name       "ali"
                                :student-id "123456"})

    ;; inserting things other than map should throw
    ;; exception
    (timbre/info "---case #2---")
    (t/is (thrown? clojure.lang.ExceptionInfo
                   (simpledb-proto/insert! db '(1 2 3 4))))))


(comment
  (t/run-tests))