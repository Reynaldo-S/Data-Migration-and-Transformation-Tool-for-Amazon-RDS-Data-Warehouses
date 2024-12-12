import requests
import json
import boto3
from pyspark.sql import SparkSession
import os

# Step 1: Fetch JSON data from the API
def fetch_json_data(json_url, json_path):
    try:
        response = requests.get(json_url)
        response.raise_for_status()
        data = response.json()
        with open(json_path, 'w') as f:
            json.dump(data, f)
        print(f"Fetched and saved JSON data to {json_path}")
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")

# Step 2: Initialize Spark Session
def initialize_spark():
    try:
        spark = SparkSession.builder \
            .appName("DataMigration") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"Error initializing Spark session: {e}")
        raise

# Step 3: Read JSON file into a DataFrame
def read_json_to_df(spark, json_path):
    try:
        df = spark.read.json(json_path)
        df.show()
        return df
    except Exception as e:
        print(f"Error reading JSON to DataFrame: {e}")
        raise

# Step 4: Store Data in S3
def store_to_s3(file_path, bucket_name, key):
    try:
        s3 = boto3.client('s3')
        s3.upload_file(file_path, bucket_name, key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

# Step 5: Load Data into DynamoDB with a Simple sno
def load_to_dynamodb(dataframe, table_name, region):
    try:
        dynamodb = boto3.resource('dynamodb', region_name=region)
        table = dynamodb.Table(table_name)

        counter = 1  
        for row in dataframe.collect():
            item = row.asDict()

            item['sno'] = str(counter)
            counter += 1

            # Insert the item into DynamoDB
            table.put_item(Item=item)

        print(f"Data loaded into DynamoDB table: {table_name}")
    except Exception as e:
        print(f"Error loading data into DynamoDB: {e}")

# Main Workflow
def main():
    json_url = "http://api.open-notify.org/iss-now.json"
    json_path = "/home/reynaldo/data.json"  # Save as data.json

    # Load environment variables
    S3_BUCKET = os.getenv('S3_BUCKET')
    DYNAMODB_TABLE = os.getenv('DYNAMODB_TABLE')
    AWS_REGION = os.getenv('AWS_REGION')

    # Fetch and save JSON data
    fetch_json_data(json_url, json_path)

    # Initialize Spark
    spark = initialize_spark()

    # Read JSON into DataFrame
    df = read_json_to_df(spark, json_path)

    # Store in S3
    store_to_s3(json_path, S3_BUCKET, "sample/data.json")

    # Load into DynamoDB
    load_to_dynamodb(df, DYNAMODB_TABLE, AWS_REGION)

    # Stop Spark session
    spark.stop()
    print("Spark session stopped")

if __name__ == "__main__":
    main()
