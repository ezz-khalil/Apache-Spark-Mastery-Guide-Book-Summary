# Chapter 1: Let’s Dive into Spark

## Overview

In this chapter, we explore the **core components of Apache Spark** to understand what happens behind the scenes when executing a Spark application. Instead of just explaining each part, this book takes a hands-on approach—illustrating how each component works using examples.

---

## Apache Spark Architecture

Apache Spark operates in a **clustered environment** that includes two main parts:

- **Cluster Side**: This includes the **Worker Nodes** and the **Cluster Manager**, which is responsible for:
  - Managing and scheduling resources  
  - Monitoring application execution  
  - Allocating computing resources

> The Cluster Manager can vary depending on the deployment mode—**YARN**, **Mesos**, **Spark Standalone**, or **Kubernetes**. For example, in a YARN cluster, the **Resource Manager** acts as the Cluster Manager.

---

## Executors

When a Spark application is submitted, it is broken down into smaller **tasks** that run on **Executors**. These are **JVM processes** launched on Worker Nodes.

- Each Spark application has its own set of executors.  
- Multiple executors can run on the same Worker Node—whether for the same or different applications.  
- Executors live throughout the lifecycle of the application (start to finish).  
- The Cluster Manager is responsible for launching executors and handling failures by restarting them when needed.

---

## Spark Driver

The **Driver** is the central component of a Spark application. It handles:

1. **Converting the user application** into a **physical execution plan**, breaking it into tasks for executors to run.  
2. **Communicating with the Cluster Manager** to request resources and manage executors.  
3. **Monitoring executors**, collecting results, and returning them to the user.

> The Driver is where the Spark application starts and ends. A simplified flow of this process is shown in Figure 2.B in the book.

---

## SparkSession

The **SparkSession** is the **main entry point** for interacting with Spark.

- It is created by the **Driver** at the start of the application and remains active throughout the lifecycle.  
- It serves as the bridge between the application and the cluster.  
- It allows configuration of key settings such as:
  - Application name  
  - Execution mode (local, YARN, Mesos, Standalone, or Kubernetes)  
  - Number of executors and cores  

> Since **Spark 2.0**, SparkSession **unifies all Spark contexts**, replacing the need to separately create `SparkContext`, `SQLContext`, or `HiveContext`. It simplifies development by providing a single access point for all Spark functionality.
