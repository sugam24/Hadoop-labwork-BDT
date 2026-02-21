# Hadoop MapReduce Analysis: WordCount & Titanic Dataset

This repository contains the full implementation of two Big Data tasks using **Hadoop 3.4.0**. It covers the complete lifecycle from starting the distributed cluster to solving analytical problems.

## 📋 Project Overview

The project demonstrates the use of the MapReduce framework to process unstructured text (Word Count) and structured CSV data (Titanic survival statistics).

### System Environment

* **Hadoop Version:** 3.4.0
* **Operating System:** Ubuntu 22.04 LTS
* **Java Version:** OpenJDK 11
* **Cluster Mode:** Pseudo-Distributed (Single-Node)

---

## 🛠️ 1. Managing the Hadoop Cluster

Before running any tasks, the Hadoop Distributed File System (HDFS) and YARN resource manager must be active.

### Start Hadoop Services

```bash
# Start NameNode and DataNode
start-dfs.sh

# Start ResourceManager and NodeManager
start-yarn.sh

# Verify all daemons are running
jps

```

*Expected output includes: NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager.*

### Stop Hadoop Services

```bash
# Stop all services safely
stop-all.sh

```

---

## 📝 2. Problem 1: Word Count Implementation

**Objective:** Calculate word frequencies in a text file.

### Technical Fix Applied:

The provided `WordCount.java` had a **Type Mismatch**. I updated the driver configuration to align the Mapper's output with the Job's expected types:

* **Change:** `job.setMapOutputValueClass(IntWritable.class);`

### Execution Steps:

```bash
# Move to project directory
cd ~/Desktop/Lab_Problems/Problem1_WordCount

# Set Hadoop Classpath for the current session
export HADOOP_CLASSPATH=$(hadoop classpath)

# Prepare HDFS directories
hadoop fs -mkdir -p /WordCountExample/Input

# Upload local data to HDFS
hadoop fs -put input_wc.txt /WordCountExample/Input

# Compile and Create JAR
mkdir -p classfile
javac -classpath ${HADOOP_CLASSPATH} -d classfile WordCount.java
jar -cvf wcexample.jar -C classfile/ .

# Run the MapReduce Job
hadoop jar wcexample.jar WordCount /WordCountExample/Input /WordCountExample/Output

# View Results
hadoop fs -cat /WordCountExample/Output/part-r-00000

```

---

## 🚢 3. Problem 2: Titanic Mean Age Analysis

**Objective:** Find the average age of male and female casualties.

### Technical Fix Applied:

The source code split data using `", "` (comma + space). The actual dataset was a standard CSV using only `","`.

* **Change:** Updated Mapper to `line.split(",")` to ensure correct column indexing for Survived (1), Gender (4), and Age (5).

### Execution Steps:

```bash
# Move to project directory
cd ~/Desktop/Lab_Problems/Problem2_TitanicProblem

# Prepare HDFS directories
hadoop fs -mkdir -p /TitanicProblem/Input
hadoop fs -put TitanicData.txt /TitanicProblem/Input

# Compile and Create JAR
mkdir -p classfile
javac -classpath ${HADOOP_CLASSPATH} -d classfile TitanicMeanAge.java
jar -cvf titanic.jar -C classfile/ .

# Run the MapReduce Job
# (Note: Delete output folder if re-running: hadoop fs -rm -r /TitanicProblem/Output)
hadoop jar titanic.jar TitanicMeanAge /TitanicProblem/Input /TitanicProblem/Output

# View Results
hadoop fs -cat /TitanicProblem/Output/part-r-00000

```

---

## 📊 Analytical Results Summary

| Analysis | Metric | Result |
| --- | --- | --- |
| **Word Count** | Most Frequent | "data" (15) |
| **Titanic Casualties** | Female Mean Age | 28 |
| **Titanic Casualties** | Male Mean Age | 30 |

### Debugging & Lessons Learned:

1. **Type Safety:** Hadoop jobs fail if `MapOutputValueClass` does not match the actual class emitted by the `context.write` method in the Mapper.
2. **Data Delimiters:** In structured data processing, the `split()` delimiter must match the raw file format exactly (e.g., CSV vs. TSV), otherwise, array indices will return incorrect columns.
3. **HDFS State:** Always ensure the NameNode is out of "Safe Mode" and active before attempting to write data.

---

*Created by Sugam - Big Data Technologies Lab 2026*