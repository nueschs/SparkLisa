SparkLisa
=========

Spark Streaming Application calculating LISA statistics from simulated WDN sensor data

Build
-----

* For local development: `mvn clean install`
* For running on cluster: `mvn clean install -Pcluster`

Run
---

* Locally: running as a Java Application with `master="local[n]"` suffices to simulate a cluster with n threads
* On cluster use either of these two options:
  * cluster_runner.py allows to run multiple jobs (-h flag prints usage options)
  * spark-submit: `spark-submit --class <<APP CLASS>> --master yarn-cluster --num-executors <<NUMBER OF CLUSTER WORKERS>>
 --executor-cores 8 --driver-memory 3584m ../../../target/SparkLisa-0.0.1-SNAPSHOT.jar  <<WINDOW DURATION>> 
 <<RECEIVER RATE>> <<NUMBER OF CLUSTER WORKERS>> <<RUN DURATION>> <<TOPOLOGY FILE LOCATION>> 
 <<OPTIONAL: NUMBER OF PAST VALUES>> <<OPTIONAL: NUMBER OF RANDOM NEIGHBOUR SETS>>`
