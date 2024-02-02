(ns simpledb-with-clojure.core
  (:require
   [clojure.core.async :as async]
   [simpledb-with-clojure.proto :as proto]
   [datascript.core :as ds]
   [taoensso.timbre :as timbre]
   [taoensso.nippy :as nippy]))

(defprotocol Access
  (<-state [_]))

(defn- sync-insert
  [x in-memory? batch-size store-path -State-]
  (if (map? x)
    (if in-memory?
      (swap! -State- conj x)
      (let [_       (swap! -State- conj x)
            -state- (-> -State- deref)
            flush?  (>= (count -state-) batch-size)]
        (when flush?
          (let [should-be-flushed-data     (take batch-size -state-)
                file                       (try
                                             (nippy/thaw-from-file store-path)
                                             (catch Exception e
                                               (timbre/spy ["No such file yet!"])
                                               (-> nil)))
                updated-data-to-be-flushed (if file
                                             (let [new-data-to-store (->> should-be-flushed-data
                                                                          (concat file)
                                                                          (into []))]
                                               (-> new-data-to-store))
                                             (-> should-be-flushed-data))
                _                          (nippy/freeze-to-file store-path updated-data-to-be-flushed)
                not-flushed-data-count     (- (count -state-) batch-size)
                _ (if (zero? not-flushed-data-count)
                    (reset! -State- [])
                    (reset! -State- (take-last not-flushed-data-count -state-)))]))))
    (throw (ex-info "Not a Map!" {:data x}))))

(defn- sync-query
  [-State- q]
  (let [-state-  (-> -State-
                     deref)
        db-conn  (ds/create-conn)
        _        (ds/transact! db-conn -state-)
        ids      (ds/q q @db-conn)
        results (->> (map first ids)
                     (map #(ds/pull @db-conn '[:*] %))
                     (map #(dissoc % :db/id)))]
    (-> results)))

(defrecord SimpleDB [-State- batch-size in-memory? store-path]
  ;; ----------- ;;  
  Access
  ;; ----------- ;;  
  (<-state [_] (-> -State-
                   deref))
  ;; ----------- ;;   
  proto/DB
  ;; ----------- ;;  
  (insert! [db x]
    (proto/insert! db x {:fn     nil
                         :async? false}))

  (insert! [db x {:keys [fn async?]}]
    (if async?
      (async/thread
        (sync-insert x in-memory? batch-size store-path -State-)
        (fn))
      (sync-insert x in-memory? batch-size store-path -State-)))

  (query [db q]
    (proto/query db q {:fn     nil
                       :async? false}))

  (piped-query [db q xf])

  (query [db q {:keys [fn async?]}]
    (if async?
      (async/thread
        (let [x (sync-query -State- q)]
          (fn x)))
      (sync-query -State- q)))

  (flush! [db path]
    (let [-State- (deref -State-)]
      (nippy/freeze-to-file path -State-)))

  (retrieve [db path]
    (let [data-from-file (nippy/thaw-from-file path)]
      (reset! -State- data-from-file))))

;; ------------------------------------- ;;
;; ------------------------------------- ;;

(defn start-db
  ([in-memory? batch-size store-path]
   (when (and in-memory?
              (or (some? batch-size)
                  (some? store-path)))
     (throw (ex-info "When in-memory is true, batch size must be nil."
                     {:batch-size batch-size
                      :in-memory? in-memory?
                      :store-path store-path})))
   (when (and (not in-memory?)
              (or (nil? batch-size)
                  (nil? store-path)))
     (throw (ex-info "When in-memory is false, batch size and store-path must not be nil."
                     {:batch-size batch-size
                      :in-memory? in-memory?
                      :store-path store-path})))

   (map->SimpleDB {:batch-size   batch-size
                   :in-memory?   in-memory?
                   :store-path   store-path
                   :-State-      (atom [])}))
  ([]
   (start-db true nil nil)))

(comment
  (def db (start-db))

  db

  (proto/insert! db {:1 "1"} {1231231 :213})

  (proto/insert! db {:1 "1"}))
