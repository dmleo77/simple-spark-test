# simple-spark-test

This is a simple java app to solve some questions with Apache Spark.

To save correctly the data from csv to database you need a mysql container running with user root and password 12345

First, pull the MySql image from docker hub: 

      docker pull mysql
      
then, run the container with this command: 

      docker run --name my-sql -p 3306:3306 -e MYSQL_ROOT_PASSWORD=12345 -d mysql:latest
      
connect to the container:

      docker exec -it my-sql bash
      
execute Mysql

      mysql -p
      
enter password

      12345
      
create database 

      CREATE DATABASE bank;
      
exit mysql

      exit
      
exit container bash

      exit
