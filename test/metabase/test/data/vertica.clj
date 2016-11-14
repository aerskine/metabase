(ns metabase.test.data.vertica
  "Code for creating / destroying a Vertica database from a `DatabaseDefinition`."
  (:require [environ.core :refer [env]]
            (metabase.driver [generic-sql :as sql]
                             vertica)
            (metabase.test.data [generic-sql :as generic]
                                [interface :as i])
            [metabase.util :as u])
  (:import metabase.driver.vertica.VerticaDriver))

(def ^:private ^:const field-base-type->sql-type
  {:type/BigInteger "BIGINT"
   :type/Boolean    "BOOLEAN"
   :type/Char       "VARCHAR(254)"
   :type/Date       "DATE"
   :type/DateTime   "TIMESTAMP"
   :type/Decimal    "NUMERIC"
   :type/Float      "FLOAT"
   :type/Integer    "INTEGER"
   :type/Text       "VARCHAR(254)"
   :type/Time       "TIME"})


(defn- db-name []
  (or (env :mb-vertica-db)
      "docker"))

(defn- database->connection-details [& _]
  {:host         (or (env :mb-vertica-host) "localhost")
   :db           (db-name)
   :port         5433
   :timezone     :America/Los_Angeles                  ; why?
   :user         (or (env :mb-vertica-user) "dbadmin")
   :password     (env :mb-vertica-password)})

(defn- qualified-name-components
  ([_]                             [(db-name)])
  ([db-name table-name]            ["public" (i/db-qualified-table-name db-name table-name)])
  ([db-name table-name field-name] ["public" (i/db-qualified-table-name db-name table-name) field-name]))


(u/strict-extend VerticaDriver
  generic/IGenericSQLDatasetLoader
  (merge generic/DefaultsMixin
         {:create-db-sql             (constantly nil)
          :drop-db-if-exists-sql     (constantly nil)
          :drop-table-if-exists-sql  generic/drop-table-if-exists-cascade-sql
          :field-base-type->sql-type (u/drop-first-arg field-base-type->sql-type)
          :load-data!                generic/load-data-one-at-a-time-parallel!
          :pk-sql-type               (constantly "INTEGER")
          :qualified-name-components (u/drop-first-arg qualified-name-components)
          :execute-sql!              generic/sequentially-execute-sql!})
  i/IDatasetLoader
  (merge generic/IDatasetLoaderMixin
         {:database->connection-details       (u/drop-first-arg database->connection-details)
          :default-schema                     (constantly "public")
          :engine                             (constantly :vertica)
          :has-questionable-timezone-support? (constantly true)}))
