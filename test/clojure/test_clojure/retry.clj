(ns clojure.test-clojure.retry
  (:use clojure.test))

(deftest test-simple []
  "Simple test of retry."
  (let [x (ref 0)
        y (ref 0)
        retries (atom 0)
        f1 (future
             (dosync
               (swap! retries inc)
               (when (= @x 0)
                 (retry)) ; block until second transaction has committed
               (is (= @x 1))
               (ref-set y 1)))
        f2 (future
             (Thread/sleep 100)
             (dosync
               (is (= @x 0)) ; first transaction shouldn't have happened yet
               (is (= @y 0))
               (ref-set x 1)))] ; now first transaction can proceed
    @f1
    @f2
    (println "Retried" @retries "times (expect 2, but might be 1)")
    (is (> @retries 0))
    (is (< @retries 3))))
