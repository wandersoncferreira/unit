(ns unit.pipeline
  (:require [clojure.core.async :as a]
            [clojure.spec.alpha :as s]
            [unit.worker :as unit.worker]))

(s/def ::nome keyword?)
(s/def ::worker-nome keyword?)
(s/def ::destinos (s/coll-of keyword?))
(s/def ::pipe (s/keys :req [::nome ::worker-nome ::destinos]))
(s/def ::pipeline (s/coll-of ::pipe :min-count 1))


(defn- get-workers
  "Pegar worker pelo nome dentro da lista de workers."
  [worker-nomes workers]
  (let [wk-names (if (vector? worker-nomes)
                   (set worker-nomes)
                   (set [worker-nomes]))]
    (filter (fn [worker] (contains? wk-names (:worker-nome worker))) workers)))


(defprotocol IPipeline
  (iniciar [this])
  (enviar-msg! [this to msg])
  (parar [this]))


(defrecord Pipeline [regras workers]
  IPipeline

  (iniciar [this]
    (let [workers-iniciados (map unit.worker/iniciar workers)
          workers-mult (map #(update % :outbox a/mult) workers-iniciados)]
      (doseq [regra regras]
        (let [worker-origem (first (get-workers (::worker-nome regra) workers-mult))
              worker-destinos (get-workers (::destinos regra) workers-mult)]
          (doseq [dest worker-destinos]
            (a/tap (:outbox worker-origem) (:inbox dest)))))
      (assoc this :running-worker workers-iniciados)))

  (enviar-msg! [this to msg]
    (let [worker (->> this :running-worker (get-workers to) first)
          to-inbox (:inbox worker)]
      (a/offer! to-inbox msg)))

  (parar [this]
    (doseq [worker (:workers this)]
      (unit.worker/parar worker))
    (dissoc this :running-worker)))


(defn criar
  [regras workers]
  {:pre (s/valid? ::pipeline regras)}
  (iniciar (->Pipeline regras workers)))
