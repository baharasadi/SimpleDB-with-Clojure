(ns simpledb-with-clojure.flush-retrieve-test
  (:require
   [clojure.test :as t]
   [simpledb-with-clojure.core :as simpledb]
   [simpledb-with-clojure.proto :as simpledb-proto]
   [taoensso.timbre :as timbre]))

(defn insert-init-data [db]
  (let [students (atom '())]
    (doseq [id (range 10)]
      (let [stu-a {:student-id id
                   :name       (str "stu-" id)
                   :gender     (if (odd? id) "male" "female")
                   :class      "A"}
            stu-b {:student-id id
                   :name       (str "stu-" (+ 10 id))
                   :gender     (if (odd? id) "male" "female")
                   :class      "B"}
            stu-c {:student-id id
                   :name       (str "stu-" (+ 20 id))
                   :gender     (if (odd? id) "male" "female")
                   :class      "C"}]
        (simpledb-proto/insert! db stu-a)
        (simpledb-proto/insert! db stu-b)
        (simpledb-proto/insert! db stu-c)
        (swap! students #(conj % stu-a))
        (swap! students #(conj % stu-b))
        (swap! students #(conj % stu-c))))
    (simpledb-proto/insert! db {:teacher-id 0
                                :name       "Teacher-0"
                                :classes    ["A" "B"]})
    (simpledb-proto/insert! db {:teacher-id 1
                                :name       "Teacher-1"
                                :classes    ["B" "C"]})
    (-> @students)))

(t/deftest Flush-And-Retrieve-Test
  (let [db       (simpledb/start-db)
        students (insert-init-data db)
        ;; _        (timbre/info "students => " students)
        path     "./flushed-db.edn"
        _        (simpledb-proto/flush! db path)
        new-db   (simpledb/start-db)
        q        '[:find ?student
                   :where
                   [?student :class _]]]
    (timbre/info "---case #1---")
    ;; at first, there is not any data in new-db, so there
    ;; is an empty list returned from the query
    (let [case-1-expected (set [])
          res             (set (simpledb-proto/query new-db q))]
      (t/is (= case-1-expected res)))
    ;; after retrieving, the res should be equal to students set
    (timbre/info "---case #2---")
    (let [case-2-expected (set students)
          _               (simpledb-proto/retrieve new-db
                                                   path)
          res             (set (simpledb-proto/query new-db q))]
      (t/is (= case-2-expected res)))))

(comment
  (t/run-tests))