(ns konserve-gcs.retry-test
  "A transient 5xx from Google must not become a permanent failure.

   An overwrite first copies the old object to `<key>.backup` with a server-side
   rewrite. The Google client library retries only operations it can prove are
   safe to repeat, and a rewrite without a generation precondition is not one of
   them, so a single 503 failed the write for good. In production that 503 shut
   down the datahike writer and the service stayed dead for 16 hours."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve-gcs.core :as gcs])
  (:import [com.sun.net.httpserver HttpExchange HttpHandler HttpServer]
           [java.net InetSocketAddress]
           [java.util.concurrent.atomic AtomicInteger]))

(def ^:private object-json
  "{\"kind\":\"storage#object\",\"id\":\"b/p/to/1\",\"name\":\"p/to\",\"bucket\":\"b\",\"generation\":\"1\",\"metageneration\":\"1\",\"size\":\"2\"}")

(def ^:private rewrite-ok
  (str "{\"kind\":\"storage#rewriteResponse\",\"totalBytesRewritten\":\"2\",\"objectSize\":\"2\",\"done\":true,\"resource\":"
       object-json "}"))

(def ^:private unavailable
  "{\"error\":{\"code\":503,\"message\":\"We encountered an internal error. Please try again.\",\"errors\":[{\"message\":\"We encountered an internal error. Please try again.\",\"domain\":\"global\",\"reason\":\"backendError\"}]}}")

(defn- respond [^HttpExchange ex code body]
  (let [bytes (.getBytes ^String body "UTF-8")]
    (.add (.getResponseHeaders ex) "Content-Type" "application/json")
    (.sendResponseHeaders ex code (alength bytes))
    (with-open [o (.getResponseBody ex)] (.write o bytes))))

(defn- with-flaky-rewrite
  "Run `f` with the URL of a local server whose `rewriteTo` call answers 503
   `fails` times and then succeeds. `f` also gets the call counter."
  [fails f]
  (let [calls  (AtomicInteger. 0)
        ^HttpServer server (HttpServer/create (InetSocketAddress. "127.0.0.1" 0) 0)]
    (.createContext server "/"
                    (reify HttpHandler
                      (handle [_ ex]
                        (.readAllBytes (.getRequestBody ex))
                        (if (re-find #"rewriteTo" (str (.getRequestURI ex)))
                          (if (< (.getAndIncrement calls) fails)
                            (respond ex 503 unavailable)
                            (respond ex 200 rewrite-ok))
                          (respond ex 404 "{}")))))
    (.start server)
    (try (f (str "http://127.0.0.1:" (.getPort (.getAddress ^HttpServer server))) calls)
         (finally (.stop server 0)))))

(deftest copy-survives-a-transient-503
  (testing "a rewrite that answers 503 once is retried and succeeds"
    (with-flaky-rewrite 1
      (fn [url ^AtomicInteger calls]
        (let [client (gcs/cloud-storage-client {:project-id "p" :host url})
              blob   (gcs/copy-blob client "b" "p" "from" "to")]
          (is (some? blob) "the copy returned the new object")
          (is (= 2 (.get calls)) "one failure, then one success"))))))

(deftest copy-survives-two-503s
  (with-flaky-rewrite 2
    (fn [url ^AtomicInteger calls]
      (let [client (gcs/cloud-storage-client {:project-id "p" :host url})]
        (is (some? (gcs/copy-blob client "b" "p" "from" "to")))
        (is (= 3 (.get calls)))))))

(deftest a-caller-supplied-client-is-left-alone
  (testing "a client passed in as :client is used as it is"
    (let [c (gcs/cloud-storage-client {:project-id "p" :host "http://127.0.0.1:1"})]
      (is (identical? c (gcs/cloud-storage-client {:client c}))))))
