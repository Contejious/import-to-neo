(ns import-to-neo.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojurewerkz.neocons.rest :as nr]
            [clojurewerkz.neocons.rest.cypher :as cy]
            [clojurewerkz.neocons.rest.transaction :as tx]
            ))

(def conn (nr/connect "http://neo4j:tej@192.168.11.28:7474/db/data/"))
(def upsert-dim-query "MERGE (d:dimension {name: {Name}}) ON CREATE SET d:<DIM>, d.created=timestamp() ON MATCH SET d:<DIM>, d.Updated=timestamp()")

(defn read-resource [filename]
  (with-open [rdr (io/reader (io/file (io/resource filename)))]
    (vec (line-seq rdr))))

(def Dimensions (read-resource "DimensionsImport.csv"))

(defn split-csv [str] (str/split str #"\,"))
(defn split-parent-ids [str] (str/split str #"\:"))

(defn csv-to-map [header row] (zipmap (map keyword (split-csv header)) (split-csv row)))

(defn csv-rows-to-map [header rows] (map #(csv-to-map header %) rows))

(defn inject-dimension-name [dim-name query] (str/replace query "<DIM>" dim-name))

(defn insert-dimension-into-neo
  [v]
  (cy/tquery conn (inject-dimension-name (v :Dimension) upsert-dim-query) (select-keys v [:Name])))

(defn insert-dimensions
  []
  (let [header (first Dimensions) rows (rest Dimensions)]
    (doseq [row rows] (insert-dimension-into-neo (csv-to-map header row)))))


(def create-relation "MATCH (parent:dimension {name:{p}}), (child:dimension {name:{c}}) MERGE (parent)-[:contains]->(child)")

(defn relate-dimensions-into-neo [v]
  (apply tx/in-transaction conn (map #(tx/statement create-relation {:c (v :Name) :p %}) (split-parent-ids (v :ParentIds)))))


(defn relate-dimensions
  []
  (let [header (first Dimensions) rows (filter #(not (nil? (% :ParentIds))) (csv-rows-to-map header (rest Dimensions)))]
    (doseq [row rows] (relate-dimensions-into-neo row))))


(def People (read-resource "PeopleImport5.csv"))

(def upsert-person "MERGE (n:person {customId:{CustomPersonId}})
ON CREATE SET n.firstName={FirstName}, n.email={Email}, n.createdAt = timestamp()
ON MATCH SET n.firstName={FirstName}, n.email={Email}, n.modifiedAt = timestamp()")

(def sever-existing-relations "MATCH (s:person {customId:{CustomPersonId}})<-[r:contains]-() DELETE r")

(def relate-person-to-dimension "MATCH (a:dimension),(p:person {customId:{customId}})
  WHERE a.name IN {dimensionList} MERGE (a)-[:contains]->(p)")

(defn upsert-person-to-neo
  [v]
  (
    let [_ (println v)]
    (tx/in-transaction conn
                       (tx/statement upsert-person v)
                       (tx/statement sever-existing-relations v)
                       (tx/statement relate-person-to-dimension {:customId (v :CustomPersonId) :dimensionList (map v [:LocationName :DepartmentName :BandName :BusinessUnit])})))
  )


(defn upsert-person-chunks-to-neo
  [chnk]
  (apply tx/in-transaction conn
        (map (fn [row] (
                     (tx/statement upsert-person row)
                     (tx/statement sever-existing-relations row)
                     (tx/statement relate-person-to-dimension {:customId (row :CustomPersonId) :dimensionList (map row [:LocationName :DepartmentName :BandName :BusinessUnit])})
         )) chnk)))

(def chunk-size 1)
(defn upsert-people
  []
  (let [header (first People)
        rows (csv-rows-to-map header (rest People))]
    (doseq [row (take 100 rows)] (upsert-person-to-neo row))))

