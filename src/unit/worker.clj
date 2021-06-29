(ns unit.worker
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]))

(def worker-registry*
  "Mantém uma referência de todas as Threads que foram abertas."
  (atom {}))

(defprotocol IWorker
  (registrar [this thread])
  (iniciar [this])
  (parar [this]))

(defn worker-thread-info
  "Informações sobre a Thread."
  [^Thread thread]
  {:id (.getId thread)
   :vivo? (.isAlive thread)
   :estado (.getState thread)
   :string-repr (.toString thread)})


(defrecord Worker [worker-nome callback estado outbox]
  IWorker
  (registrar [this thread]
    (swap! worker-registry* assoc worker-nome (worker-thread-info thread))
    (assoc this :registrado? true))

  (iniciar [this]
    (let [inbox (a/chan)
          unwrap (fn [msg] (when (some? msg) {:valor msg}))]
      (a/thread
        (.setName ^Thread (Thread/currentThread) (str "worker-" (name worker-nome)))
        (registrar this ^Thread (Thread/currentThread))
        (loop [data estado]
          (when-some [{:keys [valor]} (unwrap (a/<!! inbox))]
            (let [[new-data & results :as ret-val] (try (callback data valor) (catch Throwable e [data e]))]
              (when outbox
                (doseq [msg (filter some? results)]
                  (a/>!! outbox msg)))
              (if (some? ret-val)
                (recur new-data)
                (recur data)))))
        (when outbox
          (a/close! outbox)))
      (-> this
          (assoc :inbox inbox)
          (assoc :outbox outbox))))

  (parar [this]
    (a/close! (:inbox this))))



(s/def ::worker-nome string?)
(s/def ::callback fn?)
(s/def ::estado map?)
(s/def ::ret any?)
(s/fdef criar
  :args (s/cat :worker-nome ::worker-nome
               :callback ::callback
               :estado ::estado)
  :ret ::ret)

(defn criar
  ([worker-nome callback estado]
   (criar worker-nome callback estado (a/chan)))
  ([worker-nome callback estado outbox]
   (->Worker worker-nome callback estado outbox)))
