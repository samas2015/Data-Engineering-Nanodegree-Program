# Project Purpose
A streaming startup, Sparkify, has song database and want to move it onto the cloud. Current data exists in S3, in JSON files for songs and logs.
Here I built an ETL pipeline that extracts their data from S3 then stages them in Redshift, and transforms data into a set of dimensional tables in star schema for their analytics team to get  insights 

# Database Design Schema
Database design: 
-Data schema is star schema where songplays is a fact table and users,songs,artists and time are dimensional tables
ETL pipeline:
#1-load song json files from s3 into staging tables
#2-load data from stages tables to final analytical star schema tables on Redshift,Transform some data before insertion like timestamp and extracting time data

# Files Descritpion
#1-dwh.cfg file is configuration file where info about s3,cluster and role is saved
#2-sql_queries.py python code file contains sql scripts for the whole project
#3-create_table.py python code file that contains function to run sql queries file
#4-etl.py python code file contains implementation of ETL and functions to run sql queries

# How to Run Project
#1-create AWS Role and Redshift Cluster 
#2-edit dwh.cfg file with cluster credentials
#3-open terminal run create_tables.py then etl.py



# Examples of analytics queries: 
#Count of paid and unpaid users
![Results screenshot](https://ibb.co/fCCJp09)
#Get top 5 played songs titles and their artists
![Results screenshot](https://ibb.co/gRkGyp6)
