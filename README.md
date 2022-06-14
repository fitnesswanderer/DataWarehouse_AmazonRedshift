# DataWarehouse_AmazonRedshift

## Overview
This project provides the star schema and ETL pipeline to create and populate a data warehouse in the cloud using Amazon Redshift for analytics purposes at the music streaming app Sparkify.

In this project,building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for the analytics team to continue finding insights into what songs their users are listening to. 

The ETL and data warehouse has been built on AWS, with a PostgreSQL database and staging tables hosted on Amazon Redshift, pulling data from Amazon S3. 

## Database Schema
The database contains the following fact table:

>songplays - user song plays

Dimension tables are 
>users

>artists

>songs

>time 

## Project Template
The project contains the following components:

<code>create_tables.py</code> creates the Sparkify star schema in Redshift

<code>etl.py</code>  defines the ETL pipeline, extracting data from S3, loading into staging tables on Redshift, and then processing into analytics tables on Redshift

<code>sql_queries.py</code>  defines the SQL queries that underpin the creation of the star schema and ETL pipeline

<code>exploratory_analytics.ipynb</code>  allows you to more interactively execute the ETL and run queries

## Project Steps

<code>dwh.cfg</code> file contains details for cluster and path to datasets.

To execute the ETL on an existing cluster from the command line, enter the following:

<code>python3 create_tables.py</code>

<code>python3 etl.py</code>

To test analytic queries after creating cluster and staging tables on Amazon Redshift, refer to file

<code>explatory_analysis.ipynb</code>

