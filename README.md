
[![Build Status](https://travis-ci.org/ouyi/tez-helloworld.svg?branch=master)](https://travis-ci.org/ouyi/tez-helloworld)

# What is this about

Apache Tez, the new execution engine of Apache Pig and Hive, is a
generalization and successor of the Hadoop MapReduce framework. For
data-processing problems, if Pig or Hive can do the job, there is normally no
point to write the solution in _native_ Tez or MapReduce code. I did this
project for my self education.

The tez-helloword project provides a working code example for a very simple
_native_ Tez application, i.e., WordCount, the `Hello, world!` application for
Hadoop.

It is based on the [WordCount code of the Tez examples](https://github.com/apache/tez/blob/master/tez-examples/src/main/java/org/apache/tez/examples/WordCount.java).
The additional values are:
- Simplified code
- Unit tests
- Gradle build scripts with minimal dependencies, and 
- Instructions for running the application on Hadoop inside a Docker container

The following steps require JDK 8 (openjdk8 or oraclejdk8) and a working Docker installation (or HDP sandbox).

# How to build

- Clone this project
- Build the artifact with Gradle
    ```
    ./gradlew build
    ```

# How to run

- Start the Docker container (or the HDP sandbox).
    ```
    docker run -P -it ouyi/hadoop-docker:install-tez /etc/bootstrap.sh -bash 
    ```
- Transfer the jar file and the test data into the container, whose name assumed to be `my_container` in the following
    ```
    docker cp tez-helloworld/build/libs/tez-helloworld.jar my_container:/root/
    docker cp tez-helloword/src/test/resources/input.txt my_container:/root/
    ```
- Upload the files to HDFS
    ```
    hadoop fs -copyFromLocal -f tez-helloworld.jar /tmp
    hadoop fs -copyFromLocal -f input.txt /tmp
    ```
- Start the application
    ```
    yarn jar tez-helloworld.jar io.github.ouyi.tez.HelloWorld -Dtez.aux.uris=/tmp/tez-helloworld.jar /tmp/input.txt /tmp/output
    ```
- Verify the results
    ```
    hadoop fs -cat /tmp/output/{*}
    ```
- Run the Pig script
    ```
    hadoop fs -rm -r /tmp/output && pig -x tez -p input=/tmp/input.txt -p output=/tmp/output wordcount.pig
    ```
- Verify the results again
    ```
    hadoop fs -cat /tmp/output/{*}
    ```
