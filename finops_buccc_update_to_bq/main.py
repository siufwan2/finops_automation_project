import io
import re
from io import BytesIO
from datetime import datetime
import os
import json

import pandas as pd 
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage

import functions_framework

def obtain_latest_csv_file(blobs):
    import pytz
    # Get the current date and time
    today = datetime.now()

    # Set the initial current datetime object to an old date
    current_dt_obj = today.replace(year=today.year - 100).replace(tzinfo=pytz.UTC)

    latest_blob = None

    # Loop through each blob to find the latest one
    for blob in blobs:
        # Compare the creation time of the blob with the current datetime object 
        if current_dt_obj < blob.time_created:
            # Update the current datetime object and latest blob if a newer blob is found
            current_dt_obj = blob.time_created
            latest_blob = blob

    return latest_blob

def obtain_bkup_csv_file(bucket, today_date, cloud_platform):
    # List all blobs in the backup directory with the specified date prefix
    backup_directory_blobs =  list(bucket.list_blobs(prefix=f"{today_date}/backup"))
    
    # Iterate through each blob in the backup directory blobs
    for blob in backup_directory_blobs:
        # Check if the cloud platform is in the blob name
        if cloud_platform in blob.name:
            # Download the data of the blob as a string
            blob_data = blob.download_as_string()

            # Convert the blob's data to a pandas DataFrame
            df_recovery = pd.read_csv(io.BytesIO(blob_data))

            # Return the DataFrame for the specified cloud platform backup
            return df_recovery
        
@functions_framework.http # HTTP Trigger by Cloud Scheduler
def update_bkt_csv_to_bq(request):

    # Get the service account credentials from the environment variables
    service_account_credentials_os = os.environ.get('gcp_service_ac_cred')

    # Check if the service account credentials are available
    if not service_account_credentials_os:
        print("Service account json string not found in run time")
        # Print a message if the service account credentials are not found
        return {'message': 'Service account json string not found in secret'}, 404

    # Load the service account credentials into JSON from the JSON string
    service_account_credentials_json = json.loads(service_account_credentials_os)

    # Create service account credentials from the loaded JSON
    service_account_credentials = service_account.Credentials.from_service_account_info(service_account_credentials_json)

    # Print a message indicating successful retrieval of service account credentials
    print("Service account credentials successfully retrieve")

    # Initialize a Google Cloud Storage client & BigQuery client
    bkt_client = storage.Client(credentials=service_account_credentials, project='it-itgc-gcp-billing')
    bq_client = bigquery.Client(credentials=service_account_credentials, project='it-itgc-gcp-billing')
    print("GCS & Big Query Client successfully set-up")

    # Get today's date in the format YYYY-MM-DD
    today_date = datetime.today().strftime("%Y-%m-%d") 

    # Get the bucket named 'finops_bu_ccc_handle'
    bucket = bkt_client.get_bucket('finops_bu_ccc_handle')

    # Retrieve all blobs with prefix: today''s date
    all_blobs_today = list(bucket.list_blobs(prefix=today_date))

    # If no tickets were received today, no update to BigQuery is needed
    if len(all_blobs_today) == 0:
        print('No tickets received today, no need for update to bq')
        return {'message': 'No tickets received today, no need for update to bq'}, 200

    # Filter blobs based on cloud platform and exclude backup files
    aws_blobs = [blob for blob in all_blobs_today if 'aws' in blob.name.lower() and 'backup' not in blob.name.lower()]
    azure_blobs = [blob for blob in all_blobs_today if 'azure' in blob.name.lower() and 'backup' not in blob.name.lower()]
    gcp_blobs = [blob for blob in all_blobs_today if 'gcp' in blob.name.lower() and 'backup' not in blob.name.lower()]

    # Create a dictionary with cloud platforms as keys and corresponding blobs as values
    blobs_dict = {
        "aws": aws_blobs,
        "gcp": gcp_blobs,
        "azure": azure_blobs
    }

    # Define the dataset path to replace in BigQuery
    replace_dataset_path = {
        "aws": "it-itgc-gcp-billing.Cloud_project_bu_ccc.AWS_BU_CCC",
        "gcp": "it-itgc-gcp-billing.Cloud_project_bu_ccc.GCP_BU_CCC",
        "azure": "it-itgc-gcp-billing.Cloud_project_bu_ccc.Azure_BU_CCC"
    }
    
    # Define the file paths for SQL queries to clear tables in BigQuery
    clear_table_query_path = {
        "aws": "./sql_query/clear_aws_buccc.sql",
        "gcp": "./sql_query/clear_gcp_buccc.sql",
        "azure": "./sql_query/clear_azure_buccc.sql"
    }
    
    # Define the schema for tables in BigQuery for different cloud platforms
    table_schema = {
        "aws": [    
            bigquery.SchemaField("ac_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("BU", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("CCC", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("IT_Owner", "STRING", mode="NULLABLE")
        ],
        "gcp":[    
            bigquery.SchemaField("Project_ID", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Project_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("BU_billing", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Cost_Centre", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Cost_Split", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("IT_owner", "STRING", mode="NULLABLE")
        ],
        "azure": [    
            bigquery.SchemaField("Project_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("BU_billing", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("Cost_Centre", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("IT_owner", "STRING", mode="NULLABLE")
        ]
    }
    print("Ready to update the bigquery tables")
    
    # Update each platform BU CCC table
    for cloud_platform in ["aws", "gcp", "azure"]:

        # Get the BigQuery dataset path for the current cloud platform
        table_ref = replace_dataset_path[cloud_platform]

        # Read the SQL query to clear the BigQuery BU CCC table for the current cloud platform
        with open(clear_table_query_path[cloud_platform]) as file:
            clear_query = file.read()

        # Clear the table and insert the latest data into it
        latest_blob = obtain_latest_csv_file(blobs_dict[cloud_platform])
        print(f"Latest csv found for {cloud_platform}: {latest_blob}")

        # Download the data at run time
        blob_data = latest_blob.download_as_string()

        # Convert the blob's data to a pandas DataFrame
        df = pd.read_csv(io.BytesIO(blob_data))

        # Clear the big query bu ccc first
        clear_query_job = bq_client.query(clear_query)
        clear_query_result =  clear_query_job.result() # Waits for the job to complete.

        # If the clear query faced error, it will auto roll back due to ACID feature in bigquery
        # https://cloud.google.com/bigquery/docs/introduction#:~:text=BigQuery%20presents%20data%20in%20tables,locations%20to%20provide%20high%20availability.
        if clear_query_job.error_result:
            print("the clear buccc query failed, function terminate")
            raise Exception(f"the clear buccc query failed, function terminate")
        
        else:
            # Insert the latest update in cloud storage bucket into big query
            # Specify the job configuration for updating BigQuery table
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                schema= table_schema[cloud_platform]
            )

            # Load the DataFrame into BigQuery to update the table
            update_query_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            update_query_result = update_query_job.result()  # Wait for the job to complete

            if update_query_job.error_result:
                print("the update buccc query failed, function terminate soon")
                print("data recovery started")
                
                # Recover data from backup CSV file
                recovery_df = obtain_bkup_csv_file(bucket, today_date, cloud_platform)

                # Load the recovered data into BigQuery
                bkup_query_job = bq_client.load_table_from_dataframe(recovery_df, table_ref, job_config=job_config)
                bkup_query_result = bkup_query_job.result() # Waits for the job to complete.
                print("Data recovery success")
                raise Exception(f"the clear buccc query failed, function terminate")
            
            else:
                print(f"Update of {cloud_platform} BUCCC in {today_date} succeed.")

    return {'message': 'Update of BUCCC for AWS, Azure, and GCP succeed.'}, 200