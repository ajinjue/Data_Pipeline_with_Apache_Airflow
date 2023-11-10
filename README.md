# Data Pipelines with Airflow
## Project Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring me into the project and expect me to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Prerequisites
- I created an IAM User in AWS.
- I created and configured Redshift Serverless in AWS.
- I connected Airflow and AWS by setting up AWS credentials into Airflow using my AWS access key and secret access key.

## Project Instructions
### Datasets
For this project, I was given the links of two datasets. Here are the s3 links for each:
- Log data: s3://udacity-dend/log_data
- Song data: s3://udacity-dend/song_data

### Copy S3 Data
The given data in S3 bucket is in the US West AWS Region. I was expected to copy the data to my own S3 bucket, so Redshift can access the bucket. I transferred the data to my own S3 bucket using AWS CloudShell in the following steps:-
1. Create my own S3 bucket:
   - aws s3 mb s3://akwayaga/
2. Copy the data from the project's bucket to the home cloudshell directory:
   - aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
   - aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
3. Copy the data from the home cloudshell directory to my own bucket
   - aws s3 cp ~/log-data/ s3://akwayaga/log-data/ --recursive
   - aws s3 cp ~/song-data/ s3://akwayaga/song-data/ --recursive



## Project Overview
This project introduced me to the core concepts of Apache Airflow. To complete the project, I needed to create my own custom operators to perform tasks such as staging the data, filling the data warehouse, and running checks on the data as the final step.

I was provided with a project template that takes care of most of the imports and four empty operators that need to be implemented into functional pieces of a data pipeline. The template also contains a set of tasks that needed to be linked to achieve a coherent and sensible data flow within the pipeline.
The DAG Graph for the final project is shown below:
![image](https://github.com/ajinjue/Data_Pipeline_with_Apache_Airflow/assets/100845693/f72ea19c-ac5d-4e3a-856c-6e5ee3ed4b74)

