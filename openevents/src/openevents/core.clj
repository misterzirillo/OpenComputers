(ns openevents.core
  (:require
    [aleph.tcp :as tcp]
    [clojure.core.async :as a]
    [manifold.stream :as s]
    [mount.core :as m]
    [taoensso.timbre :as log])
  (:gen-class))

(m/defstate pub-in
  :start
  (a/chan 10)
  :stop
  (a/close! pub-in))

(m/defstate pub
  :start
  (a/pub pub-in second))

(m/defstate subscription-binder
  :start
  (let [req-ch (a/chan)]
    (a/go-loop []
      (when-let [subscription (a/<! req-ch)]
        (let [[_ _ topic] subscription
              {::keys [id output-ch]} (meta subscription)]
          (log/debug "subscribing" id topic)
          (a/sub pub topic output-ch)
          (recur))))
    (a/sub pub "subscribe" req-ch)
    req-ch)
  :stop
  (a/close! subscription-binder))

(defn bytes->string [b]
  (try
    (new String b)
    (catch Exception e
      (log/error b e))))

(defn split-event [e]
  (clojure.string/split e #"::"))

(defn has-topic [[_ t]]
  t)

(def parse-event (comp split-event bytes->string))
(defn serialize-event [evt]
  (as-> evt $
        (interpose "::" $)
        (reduce str $)
        (str $ "\n")))

(defn subscriber-handler [client info]
  (let [id-chan (a/chan)]
    (a/go
      (if-let [id (a/alt!
                    id-chan ([x] (->> x parse-event first (str "@")))
                    (a/timeout 3000) nil)]
        (let [out-ch (a/chan 10 (map serialize-event))
              ch-meta {::output-ch out-ch
                           ::id id}
              in-ch  (a/chan 1 (comp (map parse-event)
                                     (map #(apply vector id %))
                                     (filter has-topic)
                                     (map (fn [e] (log/debug e) e))
                                     (map #(with-meta % ch-meta))))]
          (log/debug "connected" id info)
          (s/on-closed client
                       #(do (a/close! in-ch)
                            (a/close! out-ch)
                            (log/debug "closing" info)))
          (a/close! id-chan)
          (a/pipe in-ch pub-in false)
          (a/sub pub id out-ch)
          (s/connect client in-ch)
          (s/connect out-ch client))
        (do
          (log/warn "did not receive identity in time" info)
          (s/close! client))))
    (s/connect client (s/->sink id-chan))))

(m/defstate
  server
  :start
  (tcp/start-server subscriber-handler {:port 1738})
  :stop
  (.close server))

(defn client
  [host port client-id]
  (let [c @(tcp/client {:host host, :port port})]
    @(s/put! c client-id)
    ;; keepalive
    (a/go-loop [i 0]
      (a/<! (a/timeout 30000))
      (when-not (s/closed? c)
        (s/put! c (str "heartbeat::" i))
        (recur (inc i))))
    c))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
