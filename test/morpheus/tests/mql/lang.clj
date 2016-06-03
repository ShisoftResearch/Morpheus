(ns morpheus.tests.mql.lang
  (:require [midje.sweet :refer :all]
            [morpheus.query.lang.base :as base]
            [morpheus.query.lang.evaluation :as eva]))

(facts "Test AST"
       (fact "Eval With Data"
             (eva/eval-with-data {} '(+ 1 1)) => 2
             (eva/eval-with-data {} '(+ 1 2)) => 3
             (eva/eval-with-data {} '(+ 1 2 3 4)) => 10
             (eva/eval-with-data {} '(+ 1 (+ 2 (+ 3 4)))) => 10
             (eva/eval-with-data {} '(+ 1 (* 2 3) (/ (- 10 4) 2))) => 10
             (eva/eval-with-data {:a 1 :b 2} '(+ :a :b :a)) => 4
             (eva/eval-with-data {:a {:b {:c 1}} :b {:c 2}} '(+ :a|b|c :b|c)) => 3
             (eva/eval-with-data {:a [0 {:b 3} 4]} '(+ :a|1|b :a|2)) => 7)
       (fact "Test Homebrew Internal Functions"
             (eva/eval-with-data {} '(!= 1 2)) => truthy
             (eva/eval-with-data {} '(!= 1 1)) => falsey
             (eva/eval-with-data {} '(has? [1 2 3] 1)) => truthy
             (eva/eval-with-data {} '(has? [1 2 3] [1 2])) => truthy
             (eva/eval-with-data {} '(has? [1 2 3] 4)) => falsey
             (eva/eval-with-data {} '(has? [1 2 3] [4 5])) => falsey
             (eva/eval-with-data {} '(has? [1 2 3] [1 5])) => falsey
             (eva/eval-with-data {} '(has? "This is a test" "test")) => truthy
             (eva/eval-with-data {} '(has? "This is a test" "morpheus")) => falsey
             (eva/eval-with-data {} '(concat [1 2] [3 4] [5 6])) => [1 2 3 4 5 6]
             (eva/eval-with-data {} '(concat "ab" "cd" "ef")) => "abcdef"
             (eva/eval-with-data {} '(and true true true)) => truthy
             (eva/eval-with-data {} '(and true false true)) => falsey
             (eva/eval-with-data {} '(and (= 1 1) (= 2 2))) => truthy
             (eva/eval-with-data {} '(and (= 1 1) (= 2 3))) => falsey
             (eva/eval-with-data {} '(or true false true false)) => truthy
             (eva/eval-with-data {} '(or true true true)) => truthy
             (eva/eval-with-data {} '(or (= 1 2) (= 2 2))) => truthy
             (eva/eval-with-data {} '(if (= 1 2) 1 2)) => 2
             (eva/eval-with-data {} '(if (= 2 2) 1 2)) => 1
             (eva/eval-with-data {} '(if (= 2 2) 2)) => 2
             (eva/eval-with-data {} '(if (= 1 2) 2)) => nil
             ))
