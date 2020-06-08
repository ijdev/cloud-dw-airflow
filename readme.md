# Cloud based Data Warehouse
### Project Summary
This project creates a data warehouse on AWS with an ETL process managed by Airflow.

### Project Scopes
The scope of this project will be to build a data ware house with conceptual schema on the cloud to answer quotations for end user in this case data scientist and analysts.

### Technology justifications 
Python: Easy intuitive way pf programming\
AWS: It has great documentation and large community to assist you. Scalable and flexible.\
Airflow: Make it easy ot manage the process of the ETL

### Data Model
 *A SIMPLE OVERVIEW OF WHAT THE DATA SCHEMA LOOKS LIKE.*\
 **Please refer to the data dictionary to know more about all the fields.**
![image](./schema.png)

The database uses the star schema with 1 fact table and 5 dimensions tables.

* `i94fact`: contains immigration information such as visa type, gender, country of origin, etc.
* `PORT`: contains port_code and port_name
* `VISA`: contains visa_code and visa_type
* `STATE`: contains state_id and state_name
* `COUNTRY`: contains country_code and country_name
* `CITY`: contains demographic information for a city

### Data Model Justifications
The star schema is a great way yo build a DW where it's 
less normalized which lead to faster queries and to understand how table connected to each other.\
**Who can use this model?**\
Any goverment entity that is intersting in preforming
some analysis over the city and sates in respect to what immigrants are reallocating to.

**What insights can be retrieved?**\
With this model you can know the type of visa of the traveller is it for bossiness or school, etc. You can 
learn what state is more appealing for students for example.\
Known the country of the immegrant, what country are most immigrants are from? are they located in 1 or two states or scattered?

### Data pipeline
Data must be in s3 bucket the pipeline job which is an airflow Dag is to get the data, load it to a staging tables, and then create conceptual schema of dimensions and fact tables

![image](./dag_pic.png)


### Scenarios
#### Data increase by 100x
Partition of data and using Airflow to manage the infrastructure is scalable. The cloud services provided by Amazon can scale easily.

#### The pipelines would be run on a daily basis by 7 am every day.
Airflow is great recourse to manage the execution of your dags. 
A failure can be alerted the user with an email

#### Make it available to 100+ people
Redshift can handle larage numbers but it's costly. 
This [article](https://read.acloud.guru/how-we-built-a-big-data-analytics-platform-on-aws-for-100-large-users-for-under-2-a-month-b37425b6cc4)
address the problem in details and AWS Athena would a much cheaper option to go for.

### HOW TO RUN
1. Create an S3 bucket 
2. Upload the files
3. Create redshift cluster
4. Configure Airflow under connection and your **redshift** connection id and **aws_credentials**
5. Run the dag

### Project files

#### Data folder
1. data_dictionary - excel file with 6 sheets correspond to each table.
2. i94 - the immigration
3. demographic - the demographic information of each city
4. country - a simple lookup file to match each country code to its name
5. port- a simple lookup file to match each port code to a port name
6. visa - a simple lookup file to match each visa code to the type of visa

#### dags folder
1. dag.py - The only dag to run the pipeline
2. create_tables.sql - create all tables 
3. drop_tables.sql - drop all tables

#### plugins folder
- Helpers folder:
1. sql_queries.py - runs all sql queries to insert the data
- Operators folder:
1.  Data_quality.py - check the tables
2. load_dimension.py - load all dimentions tables
3. load_fact.py - load all fact tables
4. stage_redshift.py - load all staging and lookup tables

# cloud-dw-airflow
