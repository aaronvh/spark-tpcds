# Spark TPC-DS

The 99 queries of [TPC-DS](http://www.tpc.org/tpcds/) can be used to performance test your Spark setup and configuration.
This project is wrapped around them to persist the results of all 99 or a subset in runs.  Runs can be compared and or visaulized.   

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

Have a Spark installation locally or configure a Spark cluster, check [here](https://spark.apache.org/).

### Installing

1. Clone repo
   ```
   git clone https://github.com/aaronvh/spark-tpcds.git
   ```
1. Goto repo location
1. Build using sbt
   ```
   sbt package
   ```
   
### Execute

Execute using [Spark submit](https://spark.apache.org/docs/latest/submitting-applications.html).
```bash
spark-submit spark-tpcds.jar <<command>> <<arguments>>
```

### Commands

* **list**
  * _database_: Database containing the runs and statement results.
  
```bash
spark-submit spark-tpcds.jar list tpcds
```
  
* **execute**
  * _name_: Name of the run.
  * _description_: Description of the run.
  * _database_: Database containing the runs and statement results.
  * _resource location_: Local or HDFS location of the (99) query sql files.
  * [optional] _ids_: Comma separated list of query ids to execute. 

```bash
spark-submit spark-tpcds.jar execute Run1 "Full run" tpcds /user/nb98ic/spark-tpcds/queries
spark-submit spark-tpcds.jar execute Run2 "Partial run" tpcds /user/nb98ic/spark-tpcds/queries 1,50,99
```

* **compare**
  * _database_: Database containing the runs and statement results.
  * _name1_: Name of the base run.
  * _name2_: Name of the run to compare with the base run.

```bash
spark-submit spark-tpcds.jar compare tpcds Run1 Run2
```

## Running the tests

1. Goto repo location
1. Run tests with sbt
   ```
   sbt test
   ```

## Deployment

Add an alias to your .bashrc to execute a Spark submit with the created jar in section [Installing](#installing).
For the arguments check section [Run](#commands)
```bash
alias spark-tpcds = "spark-submit spark-tpcds.jar <<command>> <<arguments>>"
```

## Built With

* [SBT](https://www.scala-sbt.org/)

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/aaronvh/spark-tpcds/tags). 

## Authors

* **Aäron Van Hecken** - *Initial work* - [aaronvh](https://github.com/aaronvh)
