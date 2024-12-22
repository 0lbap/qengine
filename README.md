# qengine

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=0lbap_qengine&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=0lbap_qengine)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=0lbap_qengine&metric=coverage)](https://sonarcloud.io/summary/new_code?id=0lbap_qengine)

An RDF HexaStore implementation written in Java.

## Authors

Student Group 3:

- Enzo Viguier ([@enzo-viguier](https://github.com/enzo-viguier))
- Pablo Laviron ([@0lbap](https://github.com/0lbap))

## Install

To install the project, follow those steps:

```shell
git clone git@github.com:0lbap/qengine.git
cd qengine
```

## Unit tests

You can run the unit tests for the project using the following command:

```shell
mvn test
```

## Benchmarking

You can build and run the benchmarking tool JAR as follows:

```shell
mvn package # to create a JAR for benchmarking
java -jar target/qengine-0.0.1-SNAPSHOT-jar-with-dependencies.jar -i hexastore -q data/queries -d data/2M.nt -o results.txt
```
