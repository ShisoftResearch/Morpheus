(ns morpheus.tests.mql.lang
  (:require [midje.sweet :refer :all]
            [morpheus.query.lang.base :as base]
            [morpheus.query.lang.AST :as AST]))

(facts "Test AST"
       (fact "Eval With Data"
             (AST/eval-with-data {} '(+ 1 1)) => 2
             (AST/eval-with-data {} '(+ 1 2)) => 3
             (AST/eval-with-data {} '(+ 1 2 3 4)) => 10
             (AST/eval-with-data {} '(+ 1 (+ 2 (+ 3 4)))) => 10
             (AST/eval-with-data {} '(+ 1 (* 2 3) (/ (- 10 4) 2))) => 10
             (AST/eval-with-data {:a 1 :b 2} '(+ :a :b :a)) => 4
             (AST/eval-with-data {:a {:b {:c 1}} :b {:c 2}} '(+ :a|b|c :b|c)) => 3
             (AST/eval-with-data {:a [0 {:b 3} 4]} '(+ :a|1|b :a|2)) => 7)
       (fact "Test Homebrew Internal Functions"
             (AST/eval-with-data {} '(!= 1 2)) => truthy
             (AST/eval-with-data {} '(!= 1 1)) => falsey
             (AST/eval-with-data {} '(has? [1 2 3] 1)) => truthy
             (AST/eval-with-data {} '(has? [1 2 3] [1 2])) => truthy
             (AST/eval-with-data {} '(has? [1 2 3] 4)) => falsey
             (AST/eval-with-data {} '(has? [1 2 3] [4 5])) => falsey
             (AST/eval-with-data {} '(has? [1 2 3] [1 5])) => falsey
             (AST/eval-with-data {} '(has? "This is a test" "test")) => truthy
             (AST/eval-with-data {} '(has? "This is a test" "morpheus")) => falsey
             (AST/eval-with-data {} '(concat [1 2] [3 4] [5 6])) => [1 2 3 4 5 6]
             (AST/eval-with-data {} '(concat "ab" "cd" "ef")) => "abcdef"
             (AST/eval-with-data {} '(and true true true)) => truthy
             (AST/eval-with-data {} '(and true false true)) => falsey
             (AST/eval-with-data {} '(and (= 1 1) (= 2 2))) => truthy
             (AST/eval-with-data {} '(and (= 1 1) (= 2 3))) => falsey
             (AST/eval-with-data {} '(or true false true false)) => truthy
             (AST/eval-with-data {} '(or true true true)) => truthy
             (AST/eval-with-data {} '(or (= 1 2) (= 2 2))) => truthy
             (AST/eval-with-data {} '(if (= 1 2) 1 2)) => 2
             (AST/eval-with-data {} '(if (= 2 2) 1 2)) => 1
             (AST/eval-with-data {} '(if (= 2 2) 2)) => 2
             (AST/eval-with-data {} '(if (= 1 2) 2)) => nil
             ))
