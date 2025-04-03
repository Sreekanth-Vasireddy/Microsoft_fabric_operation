# Delta Table Optimization and Vacuum Script
This repository contains a script to optimize and vacuum Delta tables in a specified lakehouse schema. The script is designed to handle errors gracefully and provide detailed output in JSON format, which can be used in a pipeline.

# Table of Contents
Overview
Features
Requirements
Usage
Functions
optimize_and_vacuum_single_table
optimize_and_vacuum_lakehouse_schema
Example
References
# Overview
The script optimizes and vacuums Delta tables in a specified lakehouse schema. It handles errors when a table is not a Delta table and provides detailed output in JSON format. The output is serialized and sent back to the pipeline using notebook.exit(output_json).

# Features
Optimizes Delta tables by compacting small files.
Vacuums Delta tables to clean up old files.
Handles errors gracefully when a table is not a Delta table.
Provides detailed output in JSON format.
Suitable for use in a pipeline.
# Requirements
Apache Spark
Delta Lake
Python 3.x
pyspark library
delta library
# Usage
1.Clone the repository
2.Install the required libraries
3.Update the lakehouse and schema variables in the script with your lakehouse path and schema name.
4.Run the script in your environment where the delta module and notebook.exit function are supported.
Functions
optimize_and_vacuum_single_table
This function optimizes and vacuums a single Delta table.

# Parameters
table_path (str): The path to the Delta table.
Returns
dict: A dictionary containing the table path and optimization metrics or an error message.
optimize_and_vacuum_lakehouse_schema
This function optimizes and vacuums all Delta tables in a specified lakehouse schema.

# Parameters
lakehouse (str): The path to the lakehouse.
schema (str): The schema name.
Returns
None: The function serializes the output messages to JSON and exits the notebook with the JSON output.




