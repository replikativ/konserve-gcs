(ns konserve-gcs.dependency-test
  "The adapter deliberately carries only Google Storage's default HTTP/JSON
   transport. Keep this separate from emulator compliance so dependency drift
   fails before an external service is involved."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve-gcs.core])
  (:import [com.google.cloud.storage StorageOptions]))

(deftest default-client-is-http-without-grpc-runtime
  (testing "the default Storage client still initializes"
    (let [options (StorageOptions/getDefaultInstance)]
      (is (= "com.google.cloud.storage.HttpStorageOptions"
             (.getName (class options))))
      (is (= "com.google.cloud.storage.StorageImpl"
             (.getName (class (.getService options)))))))
  (testing "the unused gRPC transport and its native runtime are absent"
    (is (thrown? ClassNotFoundException
                 (Class/forName
                  "io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder")))))
