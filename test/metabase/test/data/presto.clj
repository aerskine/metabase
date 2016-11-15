(ns metabase.test.data.presto
  "Code for creating / destroying a Presto database from a `DatabaseDefinition`."
  (:require [environ.core :refer [env]]
            (metabase.driver [generic-sql :as sql]
                             postgres)
            (metabase.test.data [generic-sql :as generic]
                                [interface :as i])
            [metabase.util :as u])
  (:import  (metabase.driver.presto PrestoDriver)))

(def ^:private ^:const field-base-type->sql-type
  {:type/BigInteger "BIGINT"
   :type/Boolean    "BOOL"
   :type/Date       "DATE"
   :type/DateTime   "TIMESTAMP"
   :type/Decimal    "DECIMAL"
   :type/Float      "FLOAT"
   :type/Integer    "INTEGER"
   :type/IPAddress  "INET"
   :type/Text       "TEXT"
   :type/Time       "TIME"
   :type/UUID       "UUID"})

(defn- database->connection-details [context {:keys [database-name]}]
  (merge {:host     "localhost"
          :port     5432
          :timezone :America/Los_Angeles}
         (when (env :circleci)
           {:user "ubuntu"})
         (when (= context :db)
           {:db database-name})))


(u/strict-extend PrestoDriver
                 generic/IGenericSQLDatasetLoader
                 (merge generic/DefaultsMixin
         {:drop-table-if-exists-sql  generic/drop-table-if-exists-cascade-sql
          :field-base-type->sql-type (u/drop-first-arg field-base-type->sql-type)
          :load-data!                generic/load-data-all-at-once!
          :pk-sql-type               (constantly "SERIAL")})
                 i/IDatasetLoader
                 (merge generic/IDatasetLoaderMixin
         {:database->connection-details       (u/drop-first-arg database->connection-details)
          :default-schema                     (constantly "public")
          :engine                             (constantly :presto)
          ;; TODO: this is suspect, but it works
          :has-questionable-timezone-support? (constantly true)}))

