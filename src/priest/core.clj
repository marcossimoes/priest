(ns priest.core
  (:require [clojure.set :as cset]
            [clojure.pprint :as p]
            [clojure.data.csv :as csv])
  (:import (java.util UUID))
  (:gen-class))


;; #### BUILDING SELLERS PROMISES MAP

(defn promises-sellers
  "Receives the csv-file-path an returns a map containing regions as keys and
  as values maps that on their turn contain promise to deliver as key
  and a set with seller ids as values"
  [csv-file-path]
  (let [csv (csv/read-csv (slurp csv-file-path))
        header (first csv)
        body (rest csv)
        region-pos (.indexOf header "region")
        leadtime-pos (.indexOf header "leadtime")
        seller-pos (.indexOf header "seller")]
    (reduce #(update-in %1 [(get %2 region-pos) (get %2 leadtime-pos)]
                        (fn [promise-sellers]
                          (set (conj promise-sellers (get %2 seller-pos)))))
            {}
            body)))

;;TODO: allow for the column to have their names case insensitive (i.e. region or REGION or Region)

(defn distribution-scheme
  "Receives sellers per promise (region + leadtime) and creates a distribution map with keys
  promises-sellers, sellers, regions, promises-by-most-sellers, sellers-regions-count and regions-sellers"
  [promises-sellers]
  {:promises-sellers promises-sellers
   :sellers nil
   :regions nil
   :promises-by-most-sellers nil
   :sellers-regions-count nil
   :regions-sellers nil})

(defn sellers
  "Receives a distribution map and updates the sellers list from the promises-sellers map"
  [distribution]
  (->> (:promises-sellers distribution)
       ; gets the leadtimes-sellers from each region
       vals
       ; gets the sets with sellers from each leadtime
       (map vals)
       ; merge all sets in one single set with unique sellers from the group of all promises-by-most-sellers
       (flatten)
       (apply cset/union)
       ; assocs it on the distribution map
       (assoc distribution :sellers)))

(defn regions
  "Receives a map with all the sellers for each promise (region + leadtime) and returns a set of regions"
  [distribution]
  (->> (:promises-sellers distribution)
       ; gets all the regions (promises sellers father key)
       (keys)
       ; inserts all the regions in the set to guarantee uniqueness
       (set)
       ; change the coll to vec. later .indexOf will be used agains regions so regions needs to be a vector
       (vec)
       (assoc distribution :regions)))

(defn leadtimes-seller-count
  "receives a map with leadtime as key and a set with its respective sellers as value
  and returns a map with leadtime as key and the count of sellers for that leadtime as value"
  [leadtimes-sellers]
  (reduce-kv
    (fn [leadtimes-seller-count leadtime sellers]
      (assoc leadtimes-seller-count leadtime (count sellers)))
    {}
    leadtimes-sellers))

(defn promises-seller-count
  "receives a map with region as key and as value a map with leadtimes and their respective sellers
  and returns a map with region as key and as value a map with leadtimes and the count of their respective sellers"
  [promises-sellers]
  (reduce-kv (fn [result region leadtimes-sellers]
               (assoc result region (leadtimes-seller-count leadtimes-sellers)))
             {}
             promises-sellers))

(defn leadtimes-seller-count-by-most-sellers
  "Receives a map with leadtimes as key and sellers count as value and returns the same map order
  by highest sellers count"
  [leadtimes-with-sellers-count]
  (sort-by (fn [[_ sellers-count]]
             (- sellers-count))
           leadtimes-with-sellers-count))

(defn promises-seller-count-by-most-sellers
  [promises-seller-count]
  "Receives a map with region as key and as value a map with leadtimes and the count of their respective sellers
  And returns the same map with its values ordered by leadtimes with most sellers"
  (reduce-kv (fn [result region leadtimes-with-sellers-count]
               (assoc result region (leadtimes-seller-count-by-most-sellers leadtimes-with-sellers-count)))
             {}
             promises-seller-count))

(defn regions-by-most-sellers
  "Receives a map of regions with their leadtimes ordered by most sellers and
  returns the same map with regions also ordered by most sellers"
  [promises-seller-count-by-most-sellers]
  (sort-by (fn [[_ leadtimes-seller-count-by-most-sellers]]
             (-> leadtimes-seller-count-by-most-sellers
                 ;decides using the first leadtime of the region (i.e. the one with most sellers)
                 (first)
                 ;gets the sellers count of that leadtime
                 (second)
                 ;order from the higest
                 (-)))
           promises-seller-count-by-most-sellers))

(defn removed-sellers-count
  "Receives a map with promises (region + leadtime) as keys and their sellers count as value,
  with promises order by most sellers and returns a map with regions as key
  and leadtimes as values mantaining the by most sellers order"
  [regions-by-most-sellers]
  (reduce-kv (fn [result region sellers-count-leadtimes]
               (assoc result region (keys sellers-count-leadtimes)))
             {}
             regions-by-most-sellers))

(defn promises-by-most-sellers
  "Receives the promises' sellers and returns the regions and their respective leadtimes
  all order by the highest sellers count"
  [distribution]
  (->> (:promises-sellers distribution)
       (promises-seller-count)
       (promises-seller-count-by-most-sellers)
       (regions-by-most-sellers)
       (removed-sellers-count)
       (assoc distribution :promises-by-most-sellers)))

(defn seller-in-leadtimes-sellers?
  "Receives a seller and a map of leadtimes as keys and their respective sellers
   as values and returns true if the seller delivers to at least on of the leadtimes
   in that region"
  [seller leadtimes-with-sellers]
  (some (fn [_ sellers]
          (some #{seller} sellers))
        leadtimes-with-sellers))

(defn num-regions-seller-delivers-to
  "Receive promises-sellers and seller and returns the number of regions
  the seller delivers to"
  [distribution seller]
  (reduce-kv (fn [total _ leadtimes-sellers]
               (if (seller-in-leadtimes-sellers? seller leadtimes-sellers)
                 (inc total)
                 total))
             0 (:promises-sellers distribution)))

(defn sellers-regions-count
  "Receives a distribution map containing a :sellers key and a set of sellers value
  and assocs on distribution the sellers' regions count"
  [distribution]
  (->> (:sellers distribution)
       (reduce (fn [result seller]
                 (assoc result
                   seller
                   (num-regions-seller-delivers-to
                     distribution
                     seller)))
               {})
       (assoc distribution :sellers-regions-count)))

(defn region-unique-sellers
  "Receives sellers leadtimes with sellers of a specific region
  and returns a set of unique sellers of that region"
  [leadtimes-sellers]
  (->> leadtimes-sellers
       ; returns a list of sets each containing the sellers of a unique leadtime
       (vals)
       ; combines all seller sets into a unique set, hence,
       ; containing only one ocurrence of each seller
       (apply cset/union)))

(defn regions-sellers
  "Receives a distribution map and assocs on it the regions sellers"
  [distribution]
  (->> (:promises-sellers distribution)
       (reduce-kv (fn [result region leadtimes-with-sellers]
                    (assoc
                      result
                      region
                      (region-unique-sellers leadtimes-with-sellers)))
                  {})
       (assoc distribution :regions-sellers)))

;; #### CALCULATING TEMPLATES

(defn nxt-region
  "Receives a list of regions and a region and returns the nxt
  region in the list of regions. Assumes region is never the
  last region in regions"
  [regions region]
  ;;(println "Started nxt region: " (java.time.LocalDateTime/now))
  (->> region
       (.indexOf regions)
       (inc)
       (nth regions)))

(defn reset-region
  "Receives a map of of regions and their respective leadtimes order by most sellers,
  a map of regions and their respective remaining leadtimes still not used to generate a template,
  and a specific region to be reseted
  and returns the original list of regions and their remaining leadtimes but
  the remaining leadtimes for the specific region provided are now
  equal to all possible leadtimes for that region"
  [promises-by-most-sellers regions-remaining-leadtimes region]
  ;;(println "Started reset-region: " (java.time.LocalDateTime/now))
  (assoc regions-remaining-leadtimes region (get promises-by-most-sellers region)))

(defn nxt-regions-remaining-leadtimes
  "Receives promises ordered by most sellers, regions and regions remaining leadtimes
  still not used to create a template and returns the nxt regions remaining leadtimes
  to be used for template generation"
  ([promises-by-most-sellers regions regions-remaining-leadtimes]
    (nxt-regions-remaining-leadtimes promises-by-most-sellers
                                     regions
                                     regions-remaining-leadtimes
                                     (first regions)))
  ([promises-by-most-sellers regions regions-remaining-leadtimes region]
   ;;(println "Started nxt-regions-remaining-leadtimes: " (java.time.LocalDateTime/now))
   (let [nxt-reg-rem-lts (-> (get regions-remaining-leadtimes region)
                             (rest))]
     (cond
       (and (empty? nxt-reg-rem-lts) (= region (last regions))) nil
       (empty? nxt-reg-rem-lts) (nxt-regions-remaining-leadtimes promises-by-most-sellers
                                                                 regions
                                                                 (reset-region promises-by-most-sellers regions-remaining-leadtimes region)
                                                                 (nxt-region regions region))
       :else (update regions-remaining-leadtimes
                     region
                     rest)))))

;;TODO: change logic of chosing nxt regions remaining leadtimes
;; organize the regions in desc order by the amount of sellers on the
;; leadtime with most sellers plus instead of going over all the lead times
;; for the first region to then change the next one,
;; the algorithm should go in max depth of 3

(defn template-content
  "Receives the regions with their respective possible remaining leadtimes, i.e.
  the leadtimes that were not already used to generate templates
  and returns a template composed by each region with the first possible leadtime for each region"
  [regions-remaining-leadtimes]
  ;;(println "Started template-content: " (java.time.LocalDateTime/now))
  (reduce-kv (fn [result region leadtimes]
               (assoc result region (first leadtimes)))
             {}
             regions-remaining-leadtimes))

(defn templates-content
  "receives regions with their respective leadtimes and returns a lazy sequence
  of all the possible templates contents, i.e. combinations of regions and leadtimes"
  ([distribution]
   (lazy-seq (cons (template-content (:promises-by-most-sellers distribution))
                   (let [regions-remaining-leadtimes (:promises-by-most-sellers distribution)]
                     (->> regions-remaining-leadtimes
                          (nxt-regions-remaining-leadtimes
                            (:promises-by-most-sellers distribution)
                            ;it is crutial that regions is a vector because we use .infexof on it later]
                            (:regions distribution))
                          (templates-content distribution))))))
  ([distribution regions-remaining-leadtimes]
   ;;(println "Started templates-content: " (java.time.LocalDateTime/now))
   (if (nil? regions-remaining-leadtimes)
     []
     (do
       (lazy-seq (cons (template-content regions-remaining-leadtimes)
                       (templates-content distribution
                                          (nxt-regions-remaining-leadtimes
                                            (:promises-by-most-sellers distribution)
                                            (:regions distribution)
                                            regions-remaining-leadtimes))))))))

;;(defn increase-region-count
;;  "Receives a distribution map, a subtotal, a seller and a promise (region + leadtime)
;;  and increases by 1 the subtotal if the seller promises that promise"
;;  [distribution subtotal seller promise]
;;  ;;(println "Started increasing-region-count: " (java.time.LocalDateTime/now))
;;  (if (some #{seller} (get-in (:promises-sellers distribution) promise))
;;    (inc subtotal)
;;    subtotal))

;;(defn num-promises-seller-adheres
;;  "receives a distribution map, a seller, and a template-content
;;  and returns the num of promises the seller adheres to in that template"
;;  [distribution seller template-content]
;;  ;;(println "Started num-promises-sellers-adheres" (java.time.LocalDateTime/now))
;;  (reduce (fn [region-count region-with-leadtime]
;;            (increase-region-count distribution
;;                                   region-count
;;                                   seller
;;                                   region-with-leadtime))
;;          0
;;          template-content))

;;(defn seller-adherence
;;  "Receives a distribution map, a seller and a template content
;;  and returns the adherence of that seller to that template"
;;  [distribution seller template-content]
;;  ;;(println "Started seller-adherence: " (java.time.LocalDateTime/now))
;;  (let [promise-count (num-promises-seller-adheres distribution seller template-content)]
;;    (if (zero? promise-count)
;;      promise-count
;;      (double (/
;;                promise-count
;;                (num-regions-seller-delivers-to
;;                  distribution
;;                  seller))))))

(defn template-content-avg-adherence
  "Receives a distribution map and a template content
  and returns the average adherence for that template content"
  [distribution template-content]
  ;;(println "Started template-content-avg-adherence: " (java.time.LocalDateTime/now))
  (->> (map (fn [promise]
              (get-in (:promises-sellers distribution) promise))
            template-content)
       (map vec)
       (apply concat)
       (group-by identity)
       (reduce (fn [sum-adhs [seller ocurrences-of-sellers]]
                 (+ sum-adhs
                    (double (/ (count ocurrences-of-sellers)
                               (get (:sellers-regions-count distribution) seller)))))
               0)
       (#(/ % (count (:sellers distribution))))))

(defn template-with-avg-adherence
  "receives a template and returns the template updated with the sellers
  average adherence to it"
  [distribution template]
  (println "sellers count 2: " (count (:sellers distribution)))
  ;;(println "Started template-with-avg-adherence: " (java.time.LocalDateTime/now))
  (assoc template :avg-adherence (template-content-avg-adherence distribution (:content template))))

(defn desc-adh-ordered-templates
  "receives a list of templates and returns a vector of templates
  order from the highest to the lowest average adherence temapltes"
  [templates]
  ;;(println "Started desc-adh-ordered-templates: " (java.time.LocalDateTime/now))
  (sort-by #(- (:avg-adherence %)) templates))

(defn template-scheme
  "Receives a template content and wraps it into a template scheme"
  [template-content]
  ;;(println "Started template-scheme: " (java.time.LocalDateTime/now))
  {:id nil
   :content template-content
   :avg-adherence nil})

(defn added-template-by-highest-adh
  "Receives a distribution map, a list of templates and an aditional template-content,
  and returns a new list containing the original list of templates added by
  a new template composed of the inputed template-content"
  [distribution templates template-content]
  ;;(println "Started added-template-by-highest-adh: " (java.time.LocalDateTime/now))
  (-> (conj templates
            (->> template-content
                 (template-scheme)
                 (template-with-avg-adherence distribution)))
      (desc-adh-ordered-templates)))

;; TODO: design how to attribute ids for the templates
;; the goal here would be to be able to print other files in the future for the output templates
;; e.g. sellers in lines, outputed templates ids as columns and as cells the adherence of each sellers
;; this would be useful to find sellers that are close to 100% adherence in that template and could be convinced
;; to change their promises to be 100% adherent to that templates

(defn added-if-adh-higher-then-min
  "Receives a distribution map, templates order by highest adherence and a template-content-candidate.
  If template-content-candidate adherence is equal or smaller then the minimum adherence in the original list
  returns the original list, otherwise, returns a new list substituting the
  previous min adherence template by a new template created with the template-content-candidate provided"
  [distribution templates-by-highest-adh template-content-candidate]
  ;;(println "Started added-if-adh-higher-then-min: " (java.time.LocalDateTime/now))
  (let [min-adh (:avg-adherence (last templates-by-highest-adh))
        candidate-adh (template-content-avg-adherence distribution template-content-candidate)]
    (if (< min-adh candidate-adh)
      (conj (drop-last templates-by-highest-adh)
            (->> template-content-candidate
                 (template-scheme)
                 (template-with-avg-adherence distribution)))
      templates-by-highest-adh)))

;;TODO: avg adherence for candidate template is calculated 2 times if it is a valid candidate
;;suggestion create and 3 arity option for template with avg adherence that accepts a pre calculated adherence

(defn n-templates-with-max-avg-adh
  "Receives a distribution map, a max results to be outputed, a max templates to analysed
  and a lazy sequence of templates and returns the n templates with maximum adherence from
  the ones analysed"
  [distribution max-res max-analysis templates-content]
  ;;(println "Started n-templates-with-max-avg-adh: " (java.time.LocalDateTime/now))
  (reduce (fn [partial-result template-content]
            ;;(println "adding template" (java.time.LocalDateTime/now))
            (if (< (count partial-result) max-res)
              (added-template-by-highest-adh distribution partial-result template-content)
              (added-if-adh-higher-then-min distribution partial-result template-content)))
          []
          (take max-analysis templates-content)))

(defn -main
  "Get a file with promises-sellers, a max results to be outputed, a max templates to analysed
  and creates in the same directory where the program is a file with the templates with max
  adherence from the ones that were analysed"
  [input max-res max-analysis]
  (let [distribution (-> input
                             (promises-sellers)
                             (distribution-scheme)
                             (sellers)
                             (regions)
                             (promises-by-most-sellers)
                             (sellers-regions-count)
                             (regions-sellers))
        highest-adh-templates (->> distribution
                                        (templates-content)
                                        (n-templates-with-max-avg-adh
                                          distribution
                                          (read-string max-res)
                                          (read-string max-analysis)))]
    (println highest-adh-templates)))

;;TODO: para templates with max avg adh, gerar 3 tabelas: 1. id x região = leadtime ; seller x id-temaplate-com-mais-aderência x aderência
;;TODO: organize core.clj
;;TODO: create models
;;TODO: create tests