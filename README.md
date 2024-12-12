# Data-Migration-and-Transformation-Tool-for-Amazon-RDS-Data-Warehouses

## Project Overview
This project provides a comprehensive solution for downloading, processing, and migrating data from a JSON files to Amazon S3 and Amazon RDS. It leverages Python and PySpark to handle data transformation, storage, and querying.

### Key Features:
- Downloads JSON data from an API endpoint.
- Processes JSON data using Python and PySpark.
- Stores processed data in Amazon S3.
- Loads data into Amazon DynamoDB with unique serial numbers (sno) for each record.

---
## Installation and Setup

### Prerequisites
1. Python 3.7 or higher
2. AWS account with access to S3 and DynamoDB
3. Required Python libraries: `requests`, `boto3`, `pyspark`
4. Amazon S3 bucket and DynamoDB table configurations
## Technologies Used:
- **Programming Languages**: Python, PySpark
- **Libraries and Frameworks**:
  - `requests`: For fetching JSON data.
  - `json`: For handling JSON serialization and deserialization.
  - `boto3`: For interaction with Amazon S3 and DynamoDB.
  - `pyspark`: For distributed data processing.
- **Amazon Services**:
  - Amazon S3: For data storage.
  - Amazon DynamoDB: For NoSQL data storage.

---

## Problem Statement
You are tasked with processing data from a JSON API. The goal is to:
1. Fetch JSON data from an API endpoint.
2. Save the JSON data locally.
3. Process the JSON data using PySpark and assign unique serial numbers (sno) to each record.
4. Store the processed data in Amazon S3.
5. Load the data into Amazon DynamoDB for querying and analysis.

---

## Approach

### Step 1: Fetching JSON Data from API
- Use the `requests` library to fetch JSON data from the provided API URL.
- Save the fetched JSON data locally for further processing.

### Step 2: Initialize Spark Session
- Create a PySpark session to enable distributed data processing.
- Load the saved JSON file into a Spark DataFrame for analysis and transformations.

### Step 3: Process JSON Data
- Use PySpark to process the JSON data.
- Assign unique serial numbers (sno) to each record in the DataFrame.

### Step 4: Store Data in Amazon S3
- Utilize the `boto3` library to upload the processed JSON file to an Amazon S3 bucket.
- Ensure proper folder structure and naming conventions for efficient storage and retrieval.

### Step 5: Load Data into DynamoDB
- Use the `boto3` library to connect to Amazon DynamoDB.
- Insert each record from the processed DataFrame into a DynamoDB table with the assigned sno.

---

## Results
- JSON data is fetched from the API and processed using PySpark.
- Processed data is successfully stored in Amazon S3.
- Data is migrated to Amazon DynamoDB with unique serial numbers for easy querying and analysis.

---

## Code Description
The following components are implemented in the Python script:

### 1. Fetch JSON Data
```python
response = requests.get(json_url)
response.raise_for_status()
data = response.json()
```
- Downloads and saves JSON data from the specified API URL.

### 2. Initialize Spark Session
```python
spark = SparkSession.builder.appName("DataMigration").getOrCreate()
```
- Initializes a PySpark session for distributed data processing.

### 3. Process JSON Data with Serial Numbers
```python
counter = 1
for row in dataframe.collect():
    item = row.asDict()
    item['sno'] = str(counter)
    counter += 1
```
- Assigns unique serial numbers (sno) to each record.

### 4. Store Data in Amazon S3
```python
s3.upload_file(file_path, bucket_name, key)
```
- Uploads files to Amazon S3 using the `boto3` library.

### 5. Load Data into DynamoDB
```python
table.put_item(Item=item)
```
- Inserts processed records into Amazon DynamoDB.

---
