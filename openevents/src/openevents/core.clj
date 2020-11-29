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

(defn deserialize-event [e]
  (as-> e $
        (new String $)
        (clojure.string/split $ #"::")))

(defn serialize-event [evt]
  (as-> evt $
        (interpose "::" $)
        (reduce str $)
        (str $ "\n")))

(defn xf-input [id output-ch]
  (let [ch-meta {::id        id
                 ::output-ch output-ch}]
    (comp (map deserialize-event)
          (map #(apply vector id %))
          (filter second)
          (map (fn [e] (log/debug e) e))
          (map #(with-meta % ch-meta)))))

(def xf-output
  (map serialize-event))

(defn subscriber-handler [client info]
  (let [id-chan (a/chan)]
    (a/go
      (if-let [id (a/alt!
                    id-chan ([x] (->> x deserialize-event first (str "@")))
                    (a/timeout 3000) nil)]
        (let [out-ch (a/chan 10 xf-output)
              in-ch (a/chan 1 (xf-input id out-ch))]
          (log/debug "connected" id info)
          (s/on-closed client
                       #(do (a/close! in-ch)
                            (a/close! out-ch)
                            (log/debug "closing" id)))
          (a/close! id-chan)
          (a/pipe in-ch pub-in false)
          (a/sub pub id out-ch)
          (s/connect client in-ch)
          (s/connect out-ch client))
        (do
          (log/warn "did not receive identity in time" info)
          (s/close! client))))
    (s/connect client (s/->sink id-chan))))

(m/defstate server
  :start
  (tcp/start-server subscriber-handler {:port 1738})
  :stop
  (.close server))

(defn client
  ([] (client "localhost" 1738 "repl"))
  ([host port client-id]
   (let [c @(tcp/client {:host host :port port})]
     @(s/put! c client-id)
     ;; keepalive
     (a/go-loop [i 0]
       (a/<! (a/timeout 30000))
       (when-not (s/closed? c)
         (s/put! c (str "heartbeat::" i))
         (recur (inc i))))
     c)))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (log/info "starting server")
  (m/start))
