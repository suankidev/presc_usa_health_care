#!/usr/bin/env bash

wget https://jdbc.postgresql.org/download/postgresql-42.5.2.jar

#connect pyspark with jar to valdate the table
pyspark --jars postgresql-42.5.2.jar
