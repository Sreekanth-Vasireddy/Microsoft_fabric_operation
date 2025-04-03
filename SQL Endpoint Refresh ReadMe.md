
# SQL Endpoint Refresh Script

This repository contains a script to refresh SQL endpoints in a specified lakehouse schema. The script handles errors gracefully and provides detailed output in JSON format, which can be used in a pipeline.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Requirements](#requirements)
- [Usage](#usage)
- [Functions](#functions)
  - [pad_or_truncate_string](#pad_or_truncate_string)
  - [display_fabric_config](#display_fabric_config)
  - [get_sql_endpoints](#get_sql_endpoints)
  - [refresh_sql_endpoint](#refresh_sql_endpoint)
  - [refresh_lakehouse](#refresh_lakehouse)
- [References](#references)

## Overview
The script refreshes SQL endpoints in a specified lakehouse schema. It handles errors gracefully and provides detailed output in JSON format. The output is serialized and sent back to the pipeline using `notebook.exit(output_json)`.

## Features
- Refreshes SQL endpoints in a lakehouse schema.
- Handles errors gracefully.
- Provides detailed output in JSON format.
- Suitable for use in a pipeline.

## Requirements
- Apache Spark
- Python 3.x
- `pyspark` library
- `sempy.fabric` library

## Usage
1. Clone the repository.
2. Install the required libraries.
3. Update the lakehouse and schema variables in the script with your lakehouse path and schema name.
4. Run the script in your environment where the `sempy.fabric` module and `notebook.exit` function are supported.

## Functions

### pad_or_truncate_string
This function pads or truncates a string to a specified length.

**Parameters:**
- `input_string` (str): The input string.
- `length` (int): The desired length of the output string.
- `pad_char` (str, optional): The character to use for padding. Default is a space.

**Returns:**
- `str`: The padded or truncated string.

### display_fabric_config
This function displays the Fabric configuration.

**Returns:**
- `tuple`: A tuple containing tenant ID, workspace ID, lakehouse ID, and lakehouse name.

### get_sql_endpoints
This function initializes the Fabric client and retrieves SQL endpoints.

**Parameters:**
- `workspace_id` (str): The workspace ID.
- `lakehouse_id` (str): The lakehouse ID.

**Returns:**
- `tuple`: A tuple containing the Fabric client and SQL endpoints.

### refresh_sql_endpoint
This function refreshes a SQL endpoint.

**Parameters:**
- `client` (FabricRestClient): The Fabric client.
- `sqlendpoint_id` (str): The SQL endpoint ID.

**Returns:**
- `list`: A list of output messages.

### refresh_lakehouse
This function refreshes SQL endpoints in a specified lakehouse schema.

**Returns:**
- `None`: The function serializes the output messages to JSON and exits the notebook with the JSON output.

## References
- SQL Refresh Script by Mark Pryce Maher: [GitHub Gist](https://gist.github.com/MarkPryceMaherMSFT/bb797da825de8f787b9ef492ddd36111)
