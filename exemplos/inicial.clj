(ns inicial
  (:require [unit.pipeline :as pipeline]
            [unit.worker :as worker]))


;;; 1. definir quais workers precisamos. Quais mensagens vamos receber e quais ações serão executadas

(defn worker-1
  "Recebe mensagem `:get-basic-info` e transmite dados basicos de um usuário."
  [estado msg]
  (println "~= Worker 1 =~" msg)
  (case (:msg/type msg)
    :get-basic-info (let [counter (:counter estado)
                          novo-counter (if (nil? counter) 0 (inc counter))
                          novo-estado (assoc estado :counter novo-counter)
                          mensagem-para-transmitir-proximo-worker {:msg/type :build-offer
                                                                   :usuario/nome "Wand"
                                                                   :usuario/faturamento [{:data #inst "2021-01-01" :valor 100}]}]
                      [novo-estado mensagem-para-transmitir-proximo-worker])))


(defn worker-2
  "Recebe mensagem `:build-offer` e transmite dados com a oferta de crédito para o proximo worker."
  [estado msg]
  (println "~= Worker 2 =~" msg)
  (case (:msg/type msg)
    :build-offer (let [mensagem-para-transmitir-proximo-worker {:msg/type :show-user-offer
                                                                :usuario/offer 30000}]
                   [estado mensagem-para-transmitir-proximo-worker])))


;;; lista de workers que serão usados
(def workers
  (let [estado-inicial {}]
    [(worker/criar :worker-build-offer worker-1 estado-inicial)
     (worker/criar :worker-show-user-offer worker-2 estado-inicial)]))


(def descricao-do-pipeline
  [{::pipeline/nome :get-basic-info
    ::pipeline/worker-nome :worker-build-offer
    ::pipeline/destinos [:worker-show-user-offer]}])

(def pipeline-iniciado (pipeline/criar descricao-do-pipeline workers))

;;; 2. enviar mensagems para o pipeline.
(pipeline/enviar-msg! pipeline-iniciado :worker-build-offer {:msg/type :get-basic-info})
;;; Você deve ver o print do Worker1 e do Worker 2 com os dados adicionados pelo worker 1.
