(ns metabase.driver.presto
  "Presto driver."
  (:require [metabase.driver :as driver]
            [metabase.util :as u]
            [metabase.driver.generic-sql :as sql]
            [metabase.driver.generic-sql.query-processor :as sqlqp]
            [metabase.query-processor :as qp]
            [clojure.java.jdbc :as jdbc]
            [clojure.tools.logging :as log]
            [metabase.util.honeysql-extensions :as hx]
            (honeysql [core :as hsql]
                      [helpers :as h]))
  (:import (java.sql Connection Statement DatabaseMetaData)
           (metabase.driver.generic_sql ISQLDriver)))

(defn- can-connect? [driver details-map]
  {:pre [(map? details-map)]}
  (let [desc (sql/describe-database driver {:details details-map :engine :presto})]
    (log/info "connected to database: %s" desc)
    (boolean desc)))

(defn- dissoc-schema-name [field] (dissoc field :schema-name))
(defn- dissoc-schema-names [fields] (->> fields (map dissoc-schema-name) (vec)))

(defn- honeysql-form [driver outer-query]
  (-> outer-query
      (update-in [:query :source-table] dissoc :schema)
      (->> (sqlqp/build-honeysql-form driver))))

(defn- honeysql-form->sql ^String [driver honeysql-form]
  {:pre [(map? honeysql-form)]}
  (let [[sql & args] (sql/honeysql-form->sql+args driver honeysql-form)
        ]
    (assert (empty? args)
            "Presto statements can't be parameterized!")
    sql))

(defn- mbql->native
  [driver {{{:keys [dataset-id]} :details, :as database} :database, {{table-name :name} :source-table} :query, :as outer-query}]
  {:pre [(map? database) (seq table-name)]}
  (binding [sqlqp/*query* outer-query]
    (let [hsql-form (honeysql-form driver outer-query)
          sql       (honeysql-form->sql driver hsql-form)]
      {:query      sql
       :table-name table-name
       :mbql?      true})))

(defn- exception->nice-error-message ^String [^java.sql.SQLException e]
  (or (->> (.getMessage e)     ; error message comes back like 'Column "ZID" not found; SQL statement: ... [error-code]' sometimes
           (re-find #"^(.*);") ; the user already knows the SQL, and error code is meaningless
           second)             ; so just return the part of the exception that is relevant
      (.getMessage e)))

(defn- do-with-try-catch {:style/indent 0} [f]
  (try (f)
       (catch java.sql.SQLException e
         (log/error (jdbc/print-sql-exception-chain e))
         (throw (Exception. (exception->nice-error-message e))))))

(defn- run-query
  "Run the query itself."
  [{sql :query, params :params, remark :remark} ^Connection connection]
  (let [sql               (str "-- " remark "\n" (hx/unescape-dots sql))
        statement         (into [sql] params)
        ^Statement conStm (.createStatement connection)
        rs?               (.execute conStm sql)
        rs                (.getResultSet conStm)
        [columns & rows]  (jdbc/result-set-seq rs {:as-arrays? true})]
    {:rows    (or rows [])
     :columns columns}))

(defn execute-query
  "Process and run a native (raw SQL) QUERY."
  [driver {:keys [database settings], query :native, :as outer-query}]
  (let [query (assoc query :remark (qp/query->remark outer-query))]
    (do-with-try-catch
      (fn []
        (let [db-connection-spec (sql/db->jdbc-connection-spec database)
              conn (jdbc/get-connection db-connection-spec)]
          (run-query query conn))))))

(defn- string-length-fn [field-key]
  (hsql/call :char_length (hx/cast :VARCHAR field-key)))

(defn- dbspec-presto
  "Create a database specification for a Presto database."
  [{:keys [host port catalog schema] :as opts}]
  (-> opts
      (dissoc opts :ssl :host :port :catlog :schema)
      (merge {:classname "com.facebook.presto.jdbc.PrestoDriver" ; must be in classpath
              :subprotocol "presto"
              :subname (str "//" host ":" port "/" catalog "/" schema)
              :make-pool? true
              :user "al"}
         )))

(defn- date [unit expr]
  (throw (Exception. "not implemented")))

(defn- unix-timestamp->timestamp [expr seconds-or-milliseconds]
  (throw (Exception. "not implemented")))

(defn- column->base-type
  "Map of Presto column types -> Field base types.
   Add more mappings here as you come across them."
  [column-type]
  ({:varchar       :type/Text
    :bigint        :type/BigInteger
    :boolean       :type/Boolean
    :date          :type/Date
    :decimal       :type/Decimal
    :real          :type/Float
    :double        :type/Float
    :tinyint       :type/Integer
    :smallint      :type/Integer
    :integer       :type/Integer
    :time          :type/Time
    :timestamp     :type/DateTime
    (keyword "time with timezone")          :type/Time
    (keyword "timestamp with timezone")     :type/DateTime} column-type))

(defn- describe-table-fields
  [^DatabaseMetaData metadata, ^ISQLDriver driver, {:keys [schema name]}]
  (set (for [{:keys [column_name type_name]} (jdbc/result-set-seq (.getColumns metadata nil schema name nil))
             :when (some? column_name)
             :when (some? type_name)
             :let [calculated-special-type (some-> (:column->special-type driver)
                                                   (apply column_name (keyword type_name)))]]
         (merge {:name      column_name
                 :custom    {:column-type type_name}
                 :base-type (or (column->base-type (keyword type_name))
                                (do (log/warn (format "Don't know how to map column type '%s' to a Field base_type, falling back to :type/*." type_name))
                                    :type/*))}
                (when calculated-special-type
                  (assert (isa? calculated-special-type :type/*)
                          (str "Invalid type: " calculated-special-type))
                  {:special-type calculated-special-type})))))

(defn- describe-table [driver database table]
  (sql/with-metadata [metadata driver database]
                 (assoc (select-keys table [:name :schema]) :fields (describe-table-fields metadata driver table))))

(defn- formatted-field [{:keys [table-name field-name]}]
       (keyword (hx/qualify-and-escape-dots table-name field-name)))

(defn apply-aggregation
  "Apply a `aggregation` clauses to HONEYSQL-FORM. Default implementation of `apply-aggregation` for SQL drivers."
  [driver honeysql-form {aggregations :aggregation}]
    (loop [form honeysql-form, [{:keys [aggregation-type field]} & more] aggregations]
     (let [form (sqlqp/apply-aggregation driver form aggregation-type (formatted-field field))]
       (if-not (seq more)
         form
         (recur form more)))))

(defn- apply-breakout
  "Apply a `breakout` clause to HONEYSQL-FORM."
  [_ honeysql-form {breakout-fields :breakout, fields-fields :fields}]
  (-> honeysql-form
      ;; Group by all the breakout fields
      ((partial apply h/group) (map formatted-field breakout-fields))
      ;; Add fields form only for fields that weren't specified in :fields clause -- we don't want to include it twice, or HoneySQL will barf
      ((partial apply h/merge-select) (for [field breakout-fields
                                            :when (not (contains? (set fields-fields) field))]
                                        (sqlqp/as (formatted-field field) field)))))

(defn- apply-order-by
  "Apply `order-by` clause to HONEYSQL-FORM."
  [_ honeysql-form {subclauses :order-by}]
  (loop [honeysql-form honeysql-form, [{:keys [field direction]} & more] subclauses]
    (let [honeysql-form (h/merge-order-by honeysql-form [(formatted-field field) (case direction
                                                                             :ascending  :asc
                                                                             :descending :desc)])]
      (if (seq more)
        (recur honeysql-form more)
        honeysql-form))))

(defn- apply-fields
  "Apply a `fields` clause to HONEYSQL-FORM."
  [_ honeysql-form {fields :fields}]
  (apply h/merge-select honeysql-form (for [field fields]
                                        (sqlqp/as (formatted-field field) field))))

(defn filter-subclause->predicate
  "Given a filter SUBCLAUSE, return a HoneySQL filter predicate form for use in HoneySQL `where`."
  [{:keys [filter-type field value], :as filter}]
  {:pre [(map? filter) field]}
  (let [field (formatted-field field)]
    (case          filter-type
      :between     [:between field (sqlqp/formatted (:min-val filter)) (sqlqp/formatted (:max-val filter))]
      :starts-with [:like    field (sqlqp/formatted (update value :value (fn [s] (str    s \%)))) ]
      :contains    [:like    field (sqlqp/formatted (update value :value (fn [s] (str \% s \%))))]
      :ends-with   [:like    field (sqlqp/formatted (update value :value (fn [s] (str \% s))))]
      :>           [:>       field (sqlqp/formatted value)]
      :<           [:<       field (sqlqp/formatted value)]
      :>=          [:>=      field (sqlqp/formatted value)]
      :<=          [:<=      field (sqlqp/formatted value)]
      :=           [:=       field (sqlqp/formatted value)]
      :!=          [:not=    field (sqlqp/formatted value)])))

(defn filter-clause->predicate
  "Given a filter CLAUSE, return a HoneySQL filter predicate form for use in HoneySQL `where`.
   If this is a compound clause then we call `filter-subclause->predicate` on all of the subclauses."
  [{:keys [compound-type subclause subclauses], :as clause}]
  (case compound-type
    :and (apply vector :and (map filter-clause->predicate subclauses))
    :or  (apply vector :or  (map filter-clause->predicate subclauses))
    :not [:not (filter-subclause->predicate subclause)]
    nil  (filter-subclause->predicate clause)))

(defn apply-filter
  "Apply a `filter` clause to HONEYSQL-FORM. Default implementation of `apply-filter` for SQL drivers."
  [_ honeysql-form {clause :filter}]
  (h/where honeysql-form (filter-clause->predicate clause)))

(defn apply-join-tables
  "Apply expanded query `join-tables` clause to HONEYSQL-FORM. Default implementation of `apply-join-tables` for SQL drivers."
  [_ honeysql-form {join-tables :join-tables, {source-table-name :name, source-schema :schema} :source-table}]
  (loop [honeysql-form honeysql-form, [{:keys [table-name pk-field source-field schema join-alias]} & more] join-tables]
    (let [honeysql-form (h/merge-left-join honeysql-form
                                           [(hx/qualify-and-escape-dots table-name) (keyword join-alias)]
                                           [:= (hx/qualify-and-escape-dots source-table-name (:field-name source-field))
                                            (hx/qualify-and-escape-dots join-alias (:field-name pk-field))])]
      (if (seq more)
        (recur honeysql-form more)
        honeysql-form))))

(def PrestoISQLDriverMixin
  "Implementations of `ISQLDriver` methods for `PrestoDriver`."
  (merge (sql/ISQLDriverDefaultsMixin)
         {:apply-aggregation         apply-aggregation
          :apply-breakout            apply-breakout
          :apply-fields              apply-fields
          :apply-filter              apply-filter
          :apply-join-tables         apply-join-tables
          :apply-order-by            apply-order-by
          :connection-details->spec  (u/drop-first-arg dbspec-presto)
          :column->base-type         (u/drop-first-arg column->base-type)
          :date                      (u/drop-first-arg date)
          :excluded-schemas          (constantly #{"runtime" "current" "metadata" "jdbc" "history" "presto"})
          :string-length-fn          (u/drop-first-arg string-length-fn)
          :unix-timestamp->timestamp (u/drop-first-arg unix-timestamp->timestamp)}))

(defrecord PrestoDriver []
  clojure.lang.Named
  (getName [_] "Presto"))

(u/strict-extend PrestoDriver

 driver/IDriver
 (merge (sql/IDriverSQLDefaultsMixin)
        {                     ;:analyze-table         analyze/generic-analyze-table
         :can-connect?          can-connect?
         :describe-table        describe-table
         :details-fields        (constantly [{:name         "host"
                                              :display-name "Host"
                                              :default      "marathon-lb-utility.marathon-staging.mesos"}
                                             {:name         "port"
                                              :display-name "Port"
                                              :type         :integer
                                              :default      31930}
                                             {:name         "catalog"
                                              :display-name "Catalog"
                                              :default      "accumulo"}
                                             {:name         "schema"
                                              :display-name "Schema"
                                              :default      "default"}])
         :execute-query         execute-query
         :features              (constantly #{:standard-deviation-aggregations
                                              :expressions})
         :mbql->native          mbql->native})

 sql/ISQLDriver
 PrestoISQLDriverMixin)

(driver/register-driver! :presto (PrestoDriver.))
