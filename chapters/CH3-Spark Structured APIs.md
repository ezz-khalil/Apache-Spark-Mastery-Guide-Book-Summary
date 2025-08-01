# Chapter 3: Spark Structured APIs

## What Are Structured APIs?

Spark’s **Structured APIs** allow you to process data as **immutable distributed collections** in the form of **columns and rows**, similar to a **relational database table**.

This structure makes it accessible to:

- Data engineers  
- Data scientists  
- Business analysts  
- SQL developers  

---

## 🔍 Why Structured Format Matters?

1. **Ease of Use**  
   Structured data allows users familiar with SQL or tabular formats to work in Spark without needing to learn low-level APIs.

2. **Performance Optimization**  
   With structured input, Spark knows:
   - The **schema** (column names and data types)
   - The **operation** (e.g., join, filter, groupBy)

   This visibility allows Spark to perform **query planning and optimization** before execution.

   > 🔸 In contrast:  
   > When using low-level RDD APIs, Spark lacks schema context and treats operations as black boxes, so **no optimization** can be applied.

---

## 🔄 DataFrame vs Dataset

| Feature        | **DataFrame**                       | **Dataset**                          |
|----------------|--------------------------------------|--------------------------------------|
| Typing         | Untyped (internally uses `Row`)      | Strongly typed (uses JVM types)      |
| Language       | All supported languages              | Scala and Java only                  |
| Performance    | Optimized using Catalyst             | Same as DataFrame + type safety      |
| Alias          | `Dataset[Row]`                       | `Dataset[T]`, where `T` is a class    |

- Starting from **Spark 2.0**, both APIs are part of the same unified **Structured API**.
- You can use **API functions** or **SQL syntax** to work with either DataFrames or Datasets.

---

## ⚙️ Spark Execution Pipeline (Behind the Scenes)
![figure](..figures/4.a.png) 

Whenever a DataFrame, Dataset, or SQL query is submitted to Spark, it goes through multiple planning and optimization phases:

### 1. **Unresolved Logical Plan**
- Spark parses your code and builds an **initial abstract plan** (transformations & operations)
- No validation is done yet (e.g., column names or tables aren’t checked)

### 2. **Catalog Validation**
- Spark validates the logical plan against the **Catalog** (the internal store of metadata for tables/DataFrames)
- If columns and schema match, Spark generates a **Resolved Logical Plan**

### 3. **Catalyst Optimizer**
- Spark applies **logical optimizations** (e.g., predicate pushdown, constant folding)
- Produces an **optimized logical plan**

### 4. **Physical Planning**

![](../figures/4.b.png)

- Spark generates multiple **physical plans** that describe how to execute the operations
- A **cost-based model** is used to choose the most efficient one
- Final result is a **Physical Plan** → broken down into **RDDs** and **transformations**

> 🧠 This layered planning system is what makes Structured APIs in Spark both **powerful** and **performant**.

---

## 🛠 What’s Next?

Now that we understand how Spark translates Structured API code into an executable plan, in the next sections we’ll dive deeper into:

- **DataFrames**
- **Datasets**
- **Common transformations and actions**
- **SQL syntax with Spark**

