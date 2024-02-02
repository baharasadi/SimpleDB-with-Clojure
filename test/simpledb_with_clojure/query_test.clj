(ns simpledb-with-clojure.query-test
  (:require
   [clojure.test :as t]
   [simpledb-with-clojure.core :as simpledb]
   [simpledb-with-clojure.proto :as simpledb-proto]
   [taoensso.timbre :as timbre]))

(defn insert-init-data [db]
  (let [class-A (atom '())
        class-B (atom '())
        class-C (atom '())]
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
        (swap! class-A #(conj % stu-a))
        (swap! class-B #(conj % stu-b))
        (swap! class-C #(conj % stu-c))))
    (simpledb-proto/insert! db {:teacher-id 0
                                :name       "Teacher-0"
                                :classes    ["A" "B"]})
    (simpledb-proto/insert! db {:teacher-id 1
                                :name       "Teacher-1"
                                :classes    ["B" "C"]})
    (-> [@class-A @class-B @class-B])))

(t/deftest Query-Test
  (let [db                        (simpledb/start-db)
        [class-A class-B class-C] (insert-init-data db)]
    ;; query with incorrect format should be rejected.
    (timbre/info "---case #1---: PASSES")
    (t/is (thrown? clojure.lang.ExceptionInfo
                   (let [q   {:invalid-query-format "?"}
                         res (simpledb-proto/query db q)])))

    (timbre/info "---case #2---: PASSES") ;; student of class A
    (let [q               '[:find  ?student
                            :where
                            [?student :class "A"]]
          res             (-> (simpledb-proto/query db q)
                              set)
          case-2-expected (set class-A)]
      (t/is (= case-2-expected res)))
    ;; --FAILS--
    (timbre/info "---case #3---: FAILS") ;; students of teacher 0
    ;; (let [q               '[:find  ?student
    ;;                         :where
    ;;                         [?student :class ?c]
    ;;                         [?teacher :name "Teacher-0"]
    ;;                         [?teacher :has-class ?c]
    ;;                         :def
    ;;                         (:has-class {?teacher ?c}
    ;;                                     (contains? (-> teacher?
    ;;                                                    :classes)
    ;;                                                ?c))]
    ;;       res             (-> (simpledb-proto/query db q)
    ;;                           set)
    ;;       case-3-expected (-> class-A
    ;;                           (concat class-B)
    ;;                           set)]
    ;;   (t/is (= case-3-expected res)))
    ;; --FAILS--
    (timbre/info "---case #4---: FAILS") ;; common students of teacher 0 and 1
    ;; (let [q               '[:find  ?student
    ;;                         :where
    ;;                         [?student :class ?c]
    ;;                         [?teacher-0 :name "Teacher-0"]
    ;;                         [?teacher-1 :name "Teacher-1"]
    ;;                         [?teacher-0 :has-class ?c]
    ;;                         [?teacher-1 :has-class ?c]
    ;;                         :def
    ;;                         (:has-class {?teacher ?c}
    ;;                                     (contains? (-> teacher?
    ;;                                                    :classes)
    ;;                                                ?c))]
    ;;       res             (-> (simpledb-proto/query db q)
    ;;                           set)
    ;;       case-4-expected (-> class-B
    ;;                           set)]
    ;;   (t/is (= case-4-expected res)))
    ))

(comment
  (t/run-tests)
  )

