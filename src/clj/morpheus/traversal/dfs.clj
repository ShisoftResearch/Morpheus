(ns morpheus.traversal.dfs
  (:require [morpheus.messaging.core :as msg]
            [morpheus.models.edge.core :as edges]
            [morpheus.models.vertex.core :as vertex]
            [morpheus.computation.base :as compute]
            [morpheus.traversal.dfs.rebuild :as rebuild]
            [neb.base :as nb]
            [clojure.core.async :as a]
            [neb.core :as neb]
            [cluster-connector.utils.for-debug :refer [$ spy]]
            [morpheus.models.edge.base :as eb]
            [morpheus.query.lang.AST :as AST]
            [morpheus.utils :refer [and* or*]])
  (:import (java.util.concurrent TimeoutException)))

;; Distributed deepeth first search divised by S.A.M. Makki and George Havas

;; Message schama [vertex-id stack]

(def pending-tasks (atom {}))

(defn send-stack [task-id action vertex-id stack]
  (let [server-name (nb/locate-cell-by-id vertex-id)]
    (case action
      :DFS-FORWARD (msg/send-msg server-name action [vertex-id stack] :task-id task-id)
      :DFS-RETURN  (msg/send-msg server-name action stack             :task-id task-id))))

(defn proc-forward-msg [task-id data]
  (let [[vertex-id stack] data
        {:keys [filters max-deepth stop-cond with-edges?]} (compute/get-task task-id)
        vertex (vertex/get-veterx-by-id vertex-id)
        vertex-criteria (get-in filters [:criteria :vertex])
        vertex-vailed (if vertex-criteria (AST/eval-with-data vertex vertex-criteria) true)
        neighbours (if vertex-vailed (apply edges/neighbours-edges vertex (if filters (mapcat identity filters) [])) [])
        current-vertex-stat (atom nil)
        proced-stack (doall (map (fn [v]
                                   (let [[svid] v]
                                     (if (= svid vertex-id)
                                       (do (reset! current-vertex-stat v)
                                           (assoc v 1 1) ;; reset flag to visited
                                           )
                                       v)))
                                 stack))
        proced-stack (if vertex-vailed
                       proced-stack
                       (filter (fn [[svid]] (not= svid vertex-id)) proced-stack))
        deepth (@current-vertex-stat 2)
        edge (@current-vertex-stat 4)
        vertex (if edge (assoc vertex :*edge* edge) vertex)
        next-depth (inc deepth)
        stack-verteics (set (map first stack))
        neighbour-oppisites (->> (map (fn [edge]
                                        (let [opptsite-id (eb/get-oppisite edge vertex-id)]
                                          (when (and opptsite-id (not (stack-verteics opptsite-id)))
                                            [opptsite-id 0 next-depth vertex-id (when with-edges? edge)])))
                                      neighbours)
                                 (filter identity))
        final-stack (if-not (> deepth (or max-deepth Long/MAX_VALUE))
                      (concat neighbour-oppisites proced-stack)
                      proced-stack)
        unvisited-id (first (first (filter (fn [[_ flag]] (= flag 0)) final-stack)))
        all-visted? (or (nil? unvisited-id) (and stop-cond (AST/eval-with-data vertex stop-cond)))]
    (if all-visted?
      (let [root-id (first (last stack))]
        (send-stack task-id :DFS-RETURN root-id proced-stack))
      (send-stack task-id :DFS-FORWARD unvisited-id final-stack))))

(defn proc-return-msg [task-id data]
  (let [feedback-chan (get @pending-tasks task-id)]
    (a/>!! feedback-chan data)))



(defn dfs [vertex & {:keys [filters max-deepth timeout stop-cond path-only? tail-only? destination-id with-edges? with-vertices?
                            adjacancy-list? complete-adjacancy-list?] :as extra-params
                     :or {timeout 60000}}]
  "Perform distributed deepth first search. stop-cond is for vertex."
  (let [task-id (neb/rand-cell-id)
        vertex-id (:*id* vertex)
        feedback-chan (a/chan 1)]
    (compute/new-task task-id extra-params)
    (swap! pending-tasks assoc task-id feedback-chan)
    ;;                                          [svid      flag depth parent edge]
    (send-stack task-id :DFS-FORWARD vertex-id [[vertex-id 0    0     nil    nil]])
    (let [feedback (first (a/alts!! [(a/timeout timeout) feedback-chan]))]
      (swap! pending-tasks dissoc task-id feedback-chan)
      (compute/remove-task task-id)
      (a/close! feedback-chan)
      (if (nil? feedback)
        (throw (TimeoutException.))
        (let [stack (map (fn [[vid visited deepeth parent edge]]
                           (merge
                             {:id vid
                              :deepth deepeth
                              :parent parent}
                             (when with-edges?
                               {:edge edge})
                             (when with-vertices?
                               {:vertex (vertex/get-veterx-by-id vid)})))
                         feedback)]
          (cond
            path-only?
            (rebuild/paths-from-stack
              stack vertex-id
              (or destination-id
                  (:id (last stack))))
            adjacancy-list?
            (rebuild/adjacancy-list stack)
            complete-adjacancy-list?
            (rebuild/complete-adjacancy-list stack)
            tail-only?
            (last stack)
            :else
            stack))))))

(defn has-path? [vertex-a vertex-b & params]
  (let [vertex-b-id (:*id* vertex-b)
        dfs-stack (apply dfs vertex-a
                         :stop-cond ['(= :*id* :.vid)
                                     {:.vid vertex-b-id}]
                         params)]
    (or*
      (map
        (fn [vm]
          (let [vid (:id vm)]
            (fn [] (= vertex-b-id vid))))
        dfs-stack))))

(msg/register-action :DFS-FORWARD proc-forward-msg)
(msg/register-action :DFS-RETURN  proc-return-msg)
