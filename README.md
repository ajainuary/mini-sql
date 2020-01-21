# mini-sql
A mini SQL engine which runs a subset of SQL queries through a command line interface

# Requirements
The script requires `sqlparse` python package, you may install the same in a virtual environment
`conda create --name <env> --file requirements.txt` or
`pip install sqlparse` in your current environment

# Usage

## Database Schema
The schema of the database should be specified in `metadata.txt` in the following format:
`<begin_table>
<table_name>
<attribute1>
....
<attributeN>
<end_table>`

## Data
Data should be present in the corresponding CSV File (`<table_name>.csv`) in the format specified in `metadata.txt`

## Query
SQL Query is specified via command-line argument

# Features
The mini SQL engine currently only supports queries of the type `SELECT _ FROM _ WHERE`.
In addition to basic queries it supports the following aggregate functions: `MAX`, `MIN`, `AVERAGE` and `SUM`. It also supports displaying only distinct rows corresponding to the value in a single column. 