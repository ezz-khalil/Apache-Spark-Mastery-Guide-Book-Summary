
# Chapter 4: SparkSQL

As we mentioned in the section **"Why Apache Spark?"**, Apache Spark is a **unified platform** that serves the needs of big data projects including:

- Batch and streaming data pipelines
- Data analytics
- Machine learning

Spark provides multiple APIs, making it widely accessible for a range of skill sets — data engineers, analysts, and data scientists.

## Why SparkSQL?

- SQL is a key tool used by data professionals.
- Apache Spark offers a **SQL API** to manipulate and analyze DataFrames.
- You can work with SparkSQL just like working with tables and views in traditional RDBMS.
- Spark organizes objects into **databases**, and a **default** database is always available.

You can create **views** or **tables** from DataFrames and execute SQL queries on them.

---

## 🔧 Setting Up SparkSession for SQL

To use SparkSQL, we use the `SparkSession` object:

```scala
val spark = SparkSession.builder()
  .master("local[*]")
  .appName("SparkSQL Exercises")
  .getOrCreate()
```

---

## 🗂️ Managing Databases in Spark SQL
Spark SQL organizes data into databases, similar to traditional RDBMS. Below are common database operations.

### 📍 Get Current Database
To check the active database:
```python
spark.sql("SELECT current_database()").show()
```

**Output**:
```
+------------------+
| current_database |
+------------------+
| default          |
+------------------+
```

### 🆕 Create a New Database
To create a new database:
```python
spark.sql("CREATE DATABASE sparkMastery")
```

### 📃 List All Databases
To view all databases:
```python
spark.sql("SHOW DATABASES").show()
```

### ✅ Set Default Database
To switch to a specific database:
```python
spark.sql("USE sparkMastery")
spark.sql("SELECT current_database()").show()
```

### 🗑️ Drop a Database
To safely delete a database (if it exists):
```python
spark.sql("DROP DATABASE IF EXISTS sparkMastery")
spark.sql("SELECT current_database()").show()
```

*Clarification*: Dropping a database reverts the session to the `default` database, as shown in the output.

## 🖼️ Managing Views
Views in Spark SQL are logical transformations applied to DataFrames or tables at query time. Unlike tables, views do not store data physically, making them lightweight and dynamic.

### Example DataFrames
The chapter uses sample datasets (`sales_data` and `employees_data`) to demonstrate views. Here’s how to load them:
```python
from pyspark.sql.functions import expr

sales_data = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/src/main/resources/sales_data.csv") \
    .toDF()

employees_data = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/src/main/resources/HR_Employees.csv") \
    .toDF()
```

*Note*: Ensure the file paths match your environment when running these examples.

### Create a Temporary View
Temporary views are session-scoped and not tied to a specific database. They exist only for the duration of the `SparkSession`.

Example: Create a view for customer purchase summaries (number of orders and total purchase amount):
```python
sales_data.groupBy("CUSTOMER_ID") \
    .agg(expr("COUNT(*) as number_of_orders"), expr("SUM(PRICE) as total_purchases")) \
    .createTempView("customer_sales")

spark.sql("SELECT * FROM customer_sales").show()
```

### Create or Replace Temporary View
The `createOrReplaceTempView()` method allows overwriting an existing view without errors, unlike `createTempView()`.

Example: Attempting to create a view with the same name causes an error:
```python
sales_data.groupBy("CUSTOMER_ID") \
    .agg(expr("COUNT(*) as number_of_orders"), expr("SUM(PRICE) as total_purchases")) \
    .createTempView("customer_sales")

# This will throw an error if "customer_sales" exists
sales_data.select("REQ_ID", "CUSTOMER_ID", "PRICE") \
    .createTempView("customer_sales")
```

Using `createOrReplaceTempView()` to fix this:
```python
sales_data.groupBy("CUSTOMER_ID") \
    .agg(expr("COUNT(*) as number_of_orders"), expr("SUM(PRICE) as total_purchases")) \
    .createTempView("customer_sales")

sales_data.select("REQ_ID", "CUSTOMER_ID", "PRICE") \
    .createOrReplaceTempView("customer_sales")

spark.sql("SELECT * FROM customer_sales").show()
```

*Output*: The query returns results based on the second view definition (`REQ_ID`, `CUSTOMER_ID`, `PRICE`).

### List Views
To check all tables and views in the current session:
```python
spark.sql("SHOW TABLES").show()
```

**Output Explanation**:
- The `database` column is empty for temporary views, indicating they are not bound to a specific database.
- The `isTemporary` column is `true` for temporary views.

### 🌐 Create a Global Temporary View
Global temporary views are application-scoped, persisting for the lifetime of the Spark application, unlike session-scoped temporary views. They are stored in the system-preserved `global_temp` database and require the `global_temp` prefix when queried.

Example: Create a global temporary view for customer sales:
```python
sales_data.groupBy("CUSTOMER_ID") \
    .agg(expr("COUNT(*) as number_of_orders"), expr("SUM(PRICE) as total_purchases")) \
    .createGlobalTempView("global_customer_sales")

spark.sql("SELECT * FROM global_temp.global_customer_sales").show()
```

To demonstrate the lifetime of global temporary views, create a second `SparkSession`:
```python
spark2 = SparkSession.builder \
    .master("local[*]") \
    .appName("SparkSQLSecondSession") \
    .getOrCreate()

spark2.sql("SELECT * FROM global_temp.global_customer_sales").show()
```

*Clarification*: The global temporary view is accessible in `spark2` because it’s application-scoped, but temporary views created with `createTempView()` in the original `spark` session would not be accessible in `spark2`.

### List Global Temporary Views
To list global temporary views:
```python
spark.sql("SHOW TABLES IN global_temp").show()
```

## 🗄️ Managing Tables
Spark SQL tables are similar to RDBMS tables but come in two types:
- **Managed Tables**: Spark fully manages both metadata (e.g., schema) and data, stored in the configured SQL warehouse directory.
- **Unmanaged Tables**: Spark manages only metadata (e.g., column definitions), while data is stored externally (e.g., in a cloud storage system), and Spark has no control over it.

### 🛠️ Managed Tables
Managed tables are created directly from DataFrames using the `saveAsTable` method. Metadata and data are stored in the Spark SQL warehouse directory, specified by the `spark.sql.warehouse.dir` configuration. This directory can be shared with Hive for interoperability.

Configure a local warehouse directory for testing:
```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SparkSQLExercises") \
    .config("spark.sql.warehouse.dir", "D:/spark-warehouse") \
    .getOrCreate()
```

Create a managed table from the `sales_data` DataFrame:
```python
sales_data.write.saveAsTable("sales_table")

spark.sql("SELECT * FROM sales_table").show()
```

*Note*: The table is stored in `D:/spark-warehouse/sales_table/`. Inspect this directory to see the generated Parquet files (Spark’s default format).

### 🔄 Overwrite Managed Table
To overwrite a managed table, use the `Overwrite` save mode. The new DataFrame can have a different structure or filtered data.

Example: Overwrite `sales_table` with filtered data (specific product IDs):
```python
from pyspark.sql import SaveMode

sales_data.filter(expr("PRODUCT_ID IN (12, 15)")) \
    .write.mode(SaveMode.Overwrite) \
    .saveAsTable("sales_table")

spark.sql("SELECT * FROM sales_table").show()
```

Example: Overwrite with a different structure:
```python
sales_data.filter(expr("PRODUCT_ID IN (12, 15)")) \
    .select("REQ_ID", "CUSTOMER_ID", "PRICE") \
    .write.mode(SaveMode.Overwrite) \
    .saveAsTable("sales_table")

spark.sql("SELECT * FROM sales_table").show()
```

*Clarification*: Overwriting a table replaces both data and metadata. Ensure the new schema is compatible with downstream queries to avoid errors.

### 📊 Partitioning Tables
Partitioning tables improves query performance by organizing data into subdirectories based on a partition key. This reduces data scanning during queries, especially for joins or filters on the partition key.

Example: Partition `sales_table` by `EMP_ID`:
```python
sales_data.write.partitionBy("EMP_ID") \
    .saveAsTable("sales_table")
```

Overwrite an existing table with partitioning:
```python
sales_data.write.mode(SaveMode.Overwrite) \
    .partitionBy("EMP_ID") \
    .saveAsTable("sales_table")
```

Querying a partitioned table:
```python
spark.sql("SELECT * FROM sales_table WHERE EMP_ID = 101").show()
```

*Performance Benefit*: Spark reads only the partition for `EMP_ID=101`, reducing I/O for large datasets.

List available partitions:
```python
spark.sql("SHOW PARTITIONS sales_table").show()
```

**Output Example**:
```
+----------+
| partition|
+----------+
|EMP_ID=101|
|EMP_ID=102|
|EMP_ID=103|
+----------+
```

*Clarification*: Partitioning is most effective when the partition key (e.g., `EMP_ID`) is frequently used in filters or joins. Choose keys with high cardinality to avoid skewed partitions.

### 📂 Unmanaged Tables
Unmanaged tables (also called external tables) are tables where Spark manages only the metadata (e.g., schema, column definitions), while the data resides in an external location (e.g., local filesystem, cloud storage). Dropping an unmanaged table removes only the metadata, leaving the data files intact.

#### Option 1: Create Unmanaged Table with SQL
Use the `CREATE TABLE` statement with a specified data source and path.

Example:
```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_data_temp (
        REQ_ID INTEGER,
        EMP_ID INTEGER,
        CUSTOMER_ID INTEGER,
        PRODUCT_ID INTEGER,
        PRICE INTEGER COMMENT 'Product Price including VAT'
    )
    USING csv
    OPTIONS (header 'true', path 'D:/Source-Data/sales_data.csv')
""")

spark.sql("SELECT * FROM sales_data_temp").show()
```

*General Syntax*:
```sql
CREATE TABLE IF NOT EXISTS <table_name> (
    <column_name> <data_type> COMMENT '<comment>',
    <column_name> <data_type> COMMENT '<comment>',
    ...
)
USING <format>
OPTIONS (path '<path_to_data_files>')
```

- **Supported Formats**: `TEXT`, `CSV`, `JSON`, `JDBC`, `PARQUET`, `ORC`, `HIVE`.
- **Comments**: Adding comments to columns improves readability and documentation for other users.
- **IF NOT EXISTS**: Prevents errors if the table already exists.

#### Option 2: Create Unmanaged Table with DataFrame
Use the `saveAsTable` method with a `path` option to specify the external data location.

Example:
```python
sales_data.select("CUSTOMER_ID", "PRODUCT_ID", "PRICE") \
    .write.option("path", "D:/Source-Data/sales_data_unmanaged.csv") \
    .saveAsTable("sales_data_unmanaged")

spark.sql("SELECT * FROM sales_data_unmanaged").show()
```

#### Create Table from Another Table
Create an unmanaged table from a query on an existing table using the `CREATE TABLE AS` syntax.

Example:
```python
spark.sql("""
    CREATE TABLE IF NOT EXISTS sales_emp_101
    USING csv
    AS SELECT * FROM sales_data_temp WHERE EMP_ID = 101
""")

spark.sql("SELECT * FROM sales_emp_101").show()
```

*Clarification*: The new table inherits the format (`csv`) but stores data in the warehouse directory unless a `path` option is specified, making it managed by default. To make it unmanaged, add an `OPTIONS (path '...')` clause.

#### Overwrite Unmanaged Table
To overwrite an unmanaged table:
- **Option 1 (SQL)**: Recreate the table with a new `CREATE TABLE` statement pointing to a different data location.
- **Option 2 (DataFrame)**: Use `saveAsTable` with `Overwrite` mode and a `path` option.

Example (DataFrame approach):
```python
sales_data.select("REQ_ID", "EMP_ID", "PRICE") \
    .write.mode(SaveMode.Overwrite) \
    .option("path", "D:/Source-Data/sales_data_unmanaged_v2.csv") \
    .saveAsTable("sales_data_unmanaged")

spark.sql("SELECT * FROM sales_data_unmanaged").show()
```

#### View Table Structure
To inspect the schema of a managed or unmanaged table:
```python
spark.sql("DESCRIBE sales_data_temp").show(truncate=False)
```

**Output Example**:
```
+----------------+---------+--------------------------------+
|col_name        |data_type|comment                        |
+----------------+---------+--------------------------------+
|REQ_ID          |integer  |null                           |
|EMP_ID          |integer  |null                           |
|CUSTOMER_ID     |integer  |null                           |
|PRODUCT_ID      |integer  |null                           |
|PRICE           |integer  |Product Price including VAT    |
+----------------+---------+--------------------------------+
```

#### Check Managed vs. Unmanaged Tables
Use the `spark.catalog.listTables()` function to list tables and identify their type (`MANAGED` or `EXTERNAL`).

Example:
```python
spark.catalog.listTables().show()
```

**Output Example**:
```
+------------------+---------+-----------+---------+-----------+
|name              |database |description|tableType|isTemporary|
+------------------+---------+-----------+---------+-----------+
|sales_table       |default  |null       |MANAGED  |false      |
|sales_data_temp   |default  |null       |EXTERNAL |false      |
|sales_data_unmanaged|default|null       |EXTERNAL |false      |
|customer_sales    |null     |null       |VIEW     |true       |
+------------------+---------+-----------+---------+-----------+
```

*Clarification*: `EXTERNAL` indicates an unmanaged table, while `MANAGED` indicates a managed table. The `spark.catalog` API provides metadata access for tables, databases, and views.

## 📋 Catalog Functions
The `spark.catalog` API provides programmatic access to metadata about tables, databases, views, and functions in the Spark SQL metastore. These functions replicate SQL statements (e.g., `SHOW TABLES`, `DESCRIBE`) but are executed via the Spark API, enabling automation and integration into applications.

*General Syntax*:
```python
spark.catalog.<function>
```

### List Tables
List all tables across all databases or within a specific database.

Example (all databases):
```python
spark.catalog.listTables().show()
```

Example (specific database):
```python
spark.catalog.listTables("default").show()
```

**Output Example**:
```
+------------------+---------+-----------+---------+-----------+
|name              |database |description|tableType|isTemporary|
+------------------+---------+-----------+---------+-----------+
|sales_table       |default  |null       |MANAGED  |false      |
|sales_data_temp   |default  |null       |EXTERNAL |false      |
+------------------+---------+-----------+---------+-----------+
```

### List Columns
List columns of a table, with optional database specification.

Example (table only):
```python
spark.catalog.listColumns("sales_table").show()
```

Example (with database):
```python
spark.catalog.listColumns("default", "sales_table").show()
```

**Output Example**:
```
+------------+---------+-----------+---------+-------------+--------+
|name        |dataType |nullable   |isPartition|isBucket     |comment |
+------------+---------+-----------+---------+-------------+--------+
|REQ_ID      |integer  |true       |false    |false        |null    |
|EMP_ID      |integer  |true       |true     |false        |null    |
|CUSTOMER_ID |integer  |true       |false    |false        |null    |
|PRODUCT_ID  |integer  |true       |false    |false        |null    |
|PRICE       |integer  |true       |false    |false        |null    |
+------------+---------+-----------+---------+-------------+--------+
```

*Clarification*: The output includes metadata like `isPartition` (indicating if the column is a partition key) and `comment` (from the table schema).

### List Databases
List all databases in the metastore, including their metastore URI.

Example:
```python
spark.catalog.listDatabases().show()
```

**Output Example**:
```
+------------+--------------------+
|name        |locationUri         |
+------------+--------------------+
|default     |file:/D:/spark-warehouse|
|sparkMastery|file:/D:/spark-warehouse/sparkMastery|
+------------+--------------------+
```

### Set Current Database
Set the current database programmatically.

Example:
```python
spark.catalog.setCurrentDatabase("sparkMastery")
spark.sql("SELECT current_database()").show()
```

**Output**:
```
+------------------+
| current_database |
+------------------+
| sparkMastery     |
+------------------+
```

*Note*: Corrected the book’s typo `sparkMatsery` to `sparkMastery` for consistency.

### Get Current Database
Retrieve the current database as a string.

Example:
```python
print(spark.catalog.currentDatabase())
```

**Output**:
```
sparkMastery
```

### Create Table
The `createTable` function creates an unmanaged table, specifying the table name and data file path.

Example (basic):
```python
spark.catalog.createTable("sales_table_catalog", "D:/Source-Data/sales_data_unmanaged_v2.csv")
spark.sql("SELECT * FROM sales_table_catalog").show()
```

*Clarification*: This creates an unmanaged table with an inferred schema. The data file must exist, and the schema is inferred unless specified.

Example (with format and options):
```python
spark.catalog.createTable(
    "sales_table_catalog",
    source="csv",
    options={"path": "D:/Source-Data/sales_data_unmanaged_v2.csv", "header": "true"}
)
spark.sql("SELECT * FROM sales_table_catalog").show()
```

*Supported Formats*: `csv`, `parquet`, `json`, `orc`, `text`, `jdbc`, `hive`.

*Clarification*: The `options` parameter is a dictionary (Python’s `Map`) specifying data source options like `header` or `inferSchema`. This approach provides more control than the basic `createTable` method.


