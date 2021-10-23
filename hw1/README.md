# Big Data Mining Homework 1--The First Data Analysis Program

## Homework Requirements
* Goal: Getting familiar with your big data mining environment and writing your first data analysis program
    * MapReduce on multi-node Spark (for CS students)
    * or Python in Jupyter Notebook (for others)
* Input: Numeric data (to be detailed later)
* Output: Results of simple statistics (to be detailed later)
* Data: an open dataset from UCI Machine Learning Repository
    * Data source: https://archive.ics.uci.edu/ml/datasets/individual+household+electric+power+consumption 

### To-do tasks
1. Output the minimum, maximum, and count of the following columns: ‘global active power’, ‘global reactive power’, ‘voltage’, and ‘global intensity’.
2. Output the mean and standard deviation of these columns.
3. Perform min-max normalization on the columns to generate normalized output.

#### Output Format
1. 3 values: min, max, count
2. 2 values: mean, standard deviation
3. 1 file:
    * Each line: <normalized global active power>, <normalized global reactive power>, <normalized voltage>, and <normalized global intensity>

#### Issues needed to deal with
* Missing values
* Conversion of data types

#### Rules
* Programming exercises can be done as a team 
    * At most two persons per team
* Programming language
    * Java, Scala, Python, or R on Spark (for CS students)
    * Java on Hadoop (for CS students)
    * Or Python in Jupyter Notebook

### Homework Submit
* For implementation projects, please submit a compressed file containing:
    * A document showing your environment setup
        * How many PCs/VMs, platform spec, CPU cores, memory size, network setup, …
    * Your source codes
    * The generated output (or snapshots)
    * Documentation on how to compile, install, or configure the environment, and also the detailed responsibility of each member

### Evaluation of Results
* In completion of each of the tasks, you get part of the scores
    * Correctness of Output 
    * Efficiency
* Please specify the environment setup of your (physical or virtual) machines 
* You might need to demo if your program was unable to run

## Description of Homework

### Documents
* Let's go to docs that containes:
    * **hw1_environment_document**: describes how to build the environment and attaches a demo snapshot
    * **Spark_installation**: a tutorial of installation and construction of a spark cluster. 

### Prerequistion
* Java 11.0.11
* Python 3.8.10
* Spark 3.1.2
* Maven 3.8.3

if want to know how to install them, please refer to **spark_installation** in `docs` directory.

### Environment

#### Physics Architecture
1. Raspberry Pi 4 Model B x2
    * OS: Linux Ubuntu 20.04 Server
    * CPU architecture: aarch64
    * RAM: 8GB
    * CPU: Broadcom BCM2711, Quad core Cortex-A72 (ARM v8) 64-bit SoC @ 1.5GHz
    * Number of CPU: 4C (CPU) 1T (Thread Per CPU)
2. Asus-vivobook notebook  
    * OS: Linux Ubuntu 20.04 LTS
    * CPU architecture: x86_64
    * RAM: 8GB
    * CPU: Intel(R) Core(TM) i3-8130U CPU @ 2.20GHz
    * Number of CPU: 4C (CPU) 2T (Thread Per CPU)

#### Spark Cluster Architecture
![spark architecture](./picture/bigdatamining2021-architecture.png)

### Source code
* The source code of spark is in `powerconsumption` directory. (Java)
    * The `pom.xml` includes some dependency library about spark.
* The source code of generating report is in `generate_report` directory. (Python)
    * Install dependency library using `pip install -r requirements`.

### Notice of Spark Submit
* Construct the spark cluster, as following:
    1. Create a master: `start-master.sh`.
    2. Create a worker to connect to master: `start-worker.sh [your spark url]`.
    3. Then what it looks:
        ![spark cluster](./picture/spark\ web\ ui\ show\ spark\ cluster.png) 
* If would like to submit your jar file, please use this command:
    `spark-submit --class "com.bdm.App" --master spark://[your spark master ip]:7077 [your path of jar file]`

### Output file (It will be ignored when submitted on github)
* The processed data per columns is in file named `part-00000` in `processed_data/[column_name]` directory.
* The output data of min-max normalization is in file named `output.csv` in `generate_report/reports` directory.
* The calculation of some values (e.g., max, min, mean, variation, standard deviation) is in file named `App.log` in `processed_data` directory.

## Contribution
* Hao-Ying Cheng
    * Affiliate: National Taipei University of Technology (NTUT)
    * Email: t109598001@ntut.org.tw



