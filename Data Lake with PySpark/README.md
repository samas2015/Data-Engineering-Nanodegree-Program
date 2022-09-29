# Project Purpose
A streaming startup, Sparkify, has song database and want to move it onto a datalake. Current data exists in S3, in JSON files for songs and logs.
Here I built an ETL pipeline that extracts their data from S3 using pyspark then save it back in s3 after transforming data into a set of dimensional tables in star schema for their analytics team to get  insights 

# Database Design Schema
Database design: 
-Data schema is star schema where songplays is a fact table and users,songs,artists and time are dimensional tables
ETL pipeline:
#1-load song json files from s3 into analytics tables using pyspark
#2-save analytics table into parquet files in s3 bucket

# Files Descritpion
#1-dwh.cfg file is configuration file where info about aws role is saved
#2-create_table.py python code file that contains function to run sql queries file
#3-etl.py python code file contains implementation of ETL and functions to run pyspark code

# How to Run Project
#1-create AWS Role  
#2-edit dwh.cfg file with cluster credentials
#3-open terminal run etl.py 

