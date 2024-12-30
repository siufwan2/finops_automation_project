# built-in lib
import io
import re
from io import BytesIO
from datetime import datetime
import os
import json

# Self-developed lib
from jira_client import client

# External lib
import pandas as pd 
from requests.auth import HTTPBasicAuth
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage
import functions_framework

# Helper function 1 : Create bkup files & modification files for jira tickets in GCS Bucket
def create_bkup_files_in_gcs(today_date, # The date of today will be used as the filepath
                             bq_client, # The BigQuery Client object initiated in the main function
                             bucket # The GCS bucket object created in the main function
                             ):
    
    # Retrieve the get all rows query for each cloud
    with open('./sql_query/get_aws_buccc.sql', "r") as file:
        aws_buccc_query = file.read()

    with open('./sql_query/get_azure_buccc.sql', "r") as file:
        azure_buccc_query = file.read()

    with open('./sql_query/get_gcp_buccc.sql', "r") as file:
        gcp_buccc_query = file.read()

    # Logging to ensure the sql files is placed in the correct file paths
    print("Back up querys is retrieved")

    # Run the querys and get the query results for each BU CCC tables for each cloud
    aws_query_job = bq_client.query(aws_buccc_query)
    aws_results = aws_query_job.result() # Waits for the job to complete.

    azure_query_job = bq_client.query(azure_buccc_query)
    azure_results = azure_query_job.result() # Waits for the job to complete.

    gcp_query_job = bq_client.query(gcp_buccc_query)
    gcp_results = gcp_query_job.result() # Waits for the job to complete.

    # Logging to ensure the sql files is placed in the correct file paths
    print("All cloud platform latest BUCCC table retrieve")

    # Set up an runtime io object to store the query result
    aws_output = BytesIO()
    gcp_output = BytesIO()
    azure_output = BytesIO()

    # Convert the sql query result to dataframe
    aws_df = aws_results.to_dataframe()
    azure_df = azure_results.to_dataframe()
    gcp_df = gcp_results.to_dataframe()

    # Save the dataframe into csv files in function run time first
    aws_df.to_csv(aws_output, index=False)
    gcp_df.to_csv(gcp_output, index=False)
    azure_df.to_csv(azure_output, index=False)    

    # making a reference point to the beginning of the file used in the program
    aws_output.seek(0)
    gcp_output.seek(0)
    azure_output.seek(0)

    # Logging to ensure the IO objects is ready to be uploaded
    print("blobs ready to be uploaded to the bucket")

    # Set up the file paths for the blobs upload
    generate_date = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
    output_path =['aws_bu_ccc', 'gcp_bu_ccc', 'azure_bu_ccc']
    outputs = [aws_output, gcp_output, azure_output]

    # For CRUD action received from JIRA within that day
    for i in range(len(outputs)):
        # Set up the Filepath for each object
        blob_name = f'{today_date}/{output_path[i]}_{generate_date}.csv'
        # Initialize the destination blob
        destination_blob = bucket.blob(blob_name)
        # Upload the file to Cloud Storage
        destination_blob.upload_from_file(outputs[i], content_type='csv')
        # Logging to ensure the csv file is successfully uploaded
        print(f"{output_path} successfully uploaded for JIRA ticket CRUD")

    # making a reference point to the beginning of the file used in the program again
    aws_output.seek(0)
    gcp_output.seek(0)
    azure_output.seek(0)

    # For backup of that day
    # These files will not be modified within that day
    for i in range(len(output_path)):
        # Set up the Filepath for each object
        blob_name = f'{today_date}/backup/{output_path[i]}.csv'
        # Initialize the destination blob
        destination_blob = bucket.blob(blob_name)
        # Upload the file to Cloud Storage
        destination_blob.upload_from_file(outputs[i], content_type='csv')
        # Logging to ensure the csv file is successfully uploaded
        print(f"{output_path} successfully uploaded for daily backup")

    # Logging to ensure all csv files is successfully uploaded
    print(f"Daily Backup at: {today_date} Completed")

# Helper function 2: Check if an issue in JIRA is resolved or not
def issue_in_gcs(ticket_name, # Key of the JIRA issue
                 blobs # Array of blobs (csvs, excluded back up file)
                 ):
    # For each blob in array of blobs
    for blob in blobs:
        # If the Key of the JIRA issue is in any of the blob's name
        if ticket_name in blob.name:
            print("the issue resolved in gcs already")
            return True 
    print("the issue has not been resolved in gcs")
    return False

# Helper function 3: Obtain the latest blob in an array of a specific cloud platform
def obtain_latest_csv_file(cloud_platform_blobs):
    import pytz
    # Get the current date and time
    today = datetime.now()

    # Set the initial current datetime object to an old date
    current_dt_obj = today.replace(year=today.year - 100).replace(tzinfo=pytz.UTC)

    latest_blob = None

    # Loop through each blob to find the latest one
    for blob in cloud_platform_blobs:
        # Compare the creation time of the blob with the current datetime object
        if current_dt_obj < blob.time_created:
            # Update the current datetime object and latest blob if a newer blob is found
            current_dt_obj = blob.time_created
            latest_blob = blob

    return latest_blob

# Helper function 4: Update or insert rows in DataFrame based on conditions
def substitute_value(df, cloud_platform, project_or_ac_name, project_or_ac_id, bu, ccc, 
                     cost_split="null", # It's allow to be null
                     it_owner="null" # It's allow to be null
                     ):
    
    # Define schema values for different cloud platforms' BU CCC table
    schema_value = {
        "aws": {
            "ac_name": project_or_ac_name,
            "BU":bu,
            "CCC":ccc,
            "IT_Owner":it_owner
        },
        "gcp": {
            "Project_ID": project_or_ac_id,
            "Project_name": project_or_ac_name,
            "BU_billing": bu,
            "Cost_Centre": ccc,
            "Cost_Split": cost_split,
            "IT_owner": it_owner
        },
        "azure": {
            "Project_name": project_or_ac_name,
            "BU_billing": bu,
            "Cost_Centre": ccc,
            "IT_owner": it_owner
        }
    }
    # Normalize cloud platform name
    cloud_platform = cloud_platform.lower()
    
    # Get the row to update or insert based on the cloud platform
    row_to_update_or_insert = schema_value[cloud_platform]

    # Set the condition based on the cloud platform
    condition = None
    if cloud_platform == "aws": 
        condition = df['ac_name'] == project_or_ac_name
    elif cloud_platform =="gcp": 
        condition = df['Project_name'] == project_or_ac_name
    elif cloud_platform == "azure": 
        condition = df['Project_name'] == project_or_ac_name

    # Check if the project is new or existing
    if len(df[condition]) == 0:
        print(f"It is a new project in {cloud_platform}")
        # Convert dictionary values to list and create a new DataFrame
        for key, value in row_to_update_or_insert.items():
            row_to_update_or_insert[key] = [value]
        
        # Concatenate the new row to the DataFrame
        row_to_update_or_insert = pd.DataFrame(row_to_update_or_insert)
        df = pd.concat([df, row_to_update_or_insert], ignore_index=True)
        return df
    else:
        print(f"It's an existing project in {cloud_platform}")
        print([value for key, value in row_to_update_or_insert.items()])
        # Update the existing row in the DataFrame
        df.loc[condition, list(df.columns)] = [value for key, value in row_to_update_or_insert.items()]
        return df

# Helper function 5: Return a sorted array of blobs for each cloud in a dictionary format
def return_blobs_dict(all_blobs_today):
    # Filter AWS blobs excluding 'backup' in the name
    aws_blobs = [blob for blob in all_blobs_today if 'aws' in blob.name.lower() and 'backup' not in blob.name.lower()]

    # Filter Azure blobs excluding 'backup' in the name
    azure_blobs = [blob for blob in all_blobs_today if 'azure' in blob.name.lower() and 'backup' not in blob.name.lower()]

    # Filter GCP blobs excluding 'backup' in the name
    gcp_blobs = [blob for blob in all_blobs_today if 'gcp' in blob.name.lower() and 'backup' not in blob.name.lower()]
    
    # Sort the blobs based on creation time
    aws_blobs.sort(key=lambda x:x.time_created)
    azure_blobs.sort(key=lambda x:x.time_created)
    gcp_blobs.sort(key=lambda x:x.time_created)

    # Store the sorted blobs in a dictionary format
    blobs_dict = {
        "aws": aws_blobs,
        "gcp": gcp_blobs,
        "azure": azure_blobs
    }
    return blobs_dict

@functions_framework.http # HTTP Trigger by Cloud Scheduler
def handle_request(request):

    # Get the service account credentials from the environment variables
    service_account_credentials_os = os.environ.get('gcp_service_ac_cred')

    # Check if the service account credentials are available
    if not service_account_credentials_os:
        print("Service account json string not found in secret")
        # Print a message if the service account credentials are not found
        return {'message': 'Service account json string not found in secret'}, 404
    
    # Load the service account credentials into JSON from the JSON string
    service_account_credentials_json = json.loads(service_account_credentials_os)

    # Create service account credentials from the loaded JSON
    service_account_credentials = service_account.Credentials.from_service_account_info(service_account_credentials_json)

    # Print a message indicating successful retrieval of service account credentials
    print("Service account credentials successfully retrieve")

    # Get jira secrets from the environment variables
    jira_url = os.environ.get('jira_url')
    jira_username = os.environ.get('jira_username')
    jira_token =  os.environ.get('jira_token')

    # Check if Jira credentials are available
    if not (jira_url and jira_username and jira_token):
        # Print a message if Jira credentials are not found
        print("jira credential not found in secret")
        return {'message': 'jira credential not found in secret'}, 404


    # Initialize a Google Cloud Storage client & BigQuery client
    bkt_client = storage.Client(credentials=service_account_credentials, project='it-itgc-gcp-billing')
    bq_client = bigquery.Client(credentials=service_account_credentials, project='it-itgc-gcp-billing')

    # Get the bucket named 'finops_bu_ccc_handle'
    bucket = bkt_client.get_bucket('finops_bu_ccc_handle')

    # Get today's date in the format YYYY-MM-DD
    today_date = datetime.today().strftime("%Y-%m-%d") # Will later used as filepaths


    # Initialize a JIRA API client
    jira_client = client(jira_url, HTTPBasicAuth(jira_username, jira_token))

    # Check if there are any issues raised in JIRA today
    # Construct JQL query to retrieve JIRA issues created today
    jqls = [ 
        'project = "all-cloud-billing-update"',
        'created >= startOfDay()',
        'created <= endOfDay()'
        ]
    jql = ' and '.join(jqls)
    issues_today = jira_client.get_issues(jql)
    
    # Check if any of the retrieved JIRA issues are in "WORK IN PROGRESS" status or "CANCELLED"
    any_approved_issue = any(buccc_issue.get_status() == "WORK IN PROGRESS" or buccc_issue.get_status() == "CANCELLED" for buccc_issue in issues_today)

    # Ensures that there are JIRA issues retrieved for the current day AND 
    # Any of the retrieved JIRA issues are in the "WORK IN PROGRESS" or "CANCELLED" status.
    if len(issues_today) > 0 and any_approved_issue:
        # Check if backup files do not exist in Google Cloud Storage
        if len(list(bucket.list_blobs(prefix=today_date))) == 0:
            # Create and upload backup files to Google Cloud Storage
            create_bkup_files_in_gcs(today_date, bq_client, bucket)
            print("backup files uploaded to GCS")
        else:
            print("Already back up in GCS today so no more backup needed")
    else:
        # Handling case when No JIRA Issues found
        print('No BU CCC create/update requests in JIRA')
        return {'message': 'No BU CCC create/update requests in JIRA'}, 200
    
    # Process each JIRA issue retrieved from the jql
    for buccc_issue in issues_today:
        # Handle case if the issue being approved and status in WORK IN PROGRESS
        if buccc_issue.get_status() == "WORK IN PROGRESS":
            # Obatin the key of the issue, will be used to form as the blob's file path
            issue_key  = buccc_issue.data['key']

            # Obtain the form attached in the issue, that recorded the create/update BU CCC information
            buccc_form = buccc_issue.get_forms()[0]

            # Get the simplified answer(key value pairs) from the form attached to the issue
            unformated_form_answer = jira_client.get_form_simplified_answer(buccc_issue, buccc_form)
            formated_form_answer = {}

            # Format the form answers into a dictionary for easier access
            for key_values in unformated_form_answer:
                formated_form_answer[key_values['fieldKey']] = key_values['answer']

            # List all blobs in the Google Cloud Storage bucket with the prefix of today's date
            all_blobs_today = list(bucket.list_blobs(prefix=today_date))

            # Create a dictionary for easier access to blob information
            blobs_dict = return_blobs_dict(all_blobs_today)
            
            # Extract the cloud platform information from the formatted form answers and convert it to lowercase
            issue_cloud_platform = formated_form_answer['cloud_platform'].lower()

            # Check if the issue key exists in Google Cloud Storage for the specific cloud platform
            if issue_in_gcs(issue_key, blobs_dict[issue_cloud_platform]):
                # If the issue is found in Google Cloud Storage for the specified cloud platform
                print("Issue resolved")
                # Continue to the next iteration without further processing
                continue
            
            try:
                # Obtain the latest CSV file from the blobs for the specified cloud platform
                latest_blob = obtain_latest_csv_file(blobs_dict[issue_cloud_platform])

                # Make modification on the csv obtain in runtime
                # Set up an runtime io object to store the query result
                output = BytesIO()

                # Download the data at run time as a string
                blob_data = latest_blob.download_as_string()

                # Convert the blob's data to a pandas DataFrame
                df = pd.read_csv(io.BytesIO(blob_data))

                # Append or update a row in the DataFrame with the BU CCC information
                # substitute_value(df, cloud_platform, project_or_ac_name, project_or_ac_id, bu, ccc, cost_split="null", it_owner="null")
                df = substitute_value(df, 
                                    formated_form_answer['cloud_platform'], 
                                    formated_form_answer['project_name'], 
                                    formated_form_answer['project_id'], 
                                    formated_form_answer['bu'], 
                                    formated_form_answer['pj_ccc_replacement'], 
                                    formated_form_answer['cost_split'] if formated_form_answer['more_than_1_ccc'] else "null", 
                                    it_owner="null") # Not yet set in form

                df.to_csv(output, index=False) 
                output.seek(0)

                # Define the output path for the modified CSV file based on the cloud platform
                output_path_name = {
                    "aws": f'{today_date}/aws_bu_ccc_{issue_key}.csv',
                    "gcp": f'{today_date}/gcp_bu_ccc_{issue_key}.csv',
                    "azure": f'{today_date}/azure_bu_ccc_{issue_key}.csv'
                }

                # Upload the modified CSV file to the cloud storage bucket
                destination_blob = bucket.blob(output_path_name[issue_cloud_platform])
                destination_blob.upload_from_file(output, content_type='csv')

                # Transition the JIRA issue status to "completed" and add a success comment
                success_msg = f"{output_path_name[issue_cloud_platform]} update/create automation success"
                print(success_msg)
                buccc_issue.transit_issue("completed")
                buccc_issue.add_comment(success_msg, style="paragraph", attachments=[], internal=True)

            except Exception as e:
                # Transition the JIRA issue status to "fail" and add an error comment
                error_msg = f"Error during automation: {e}"
                buccc_issue.transit_issue("fail")
                buccc_issue.add_comment(error_msg, style="paragraph", attachments=[], internal=True)
                raise error_msg
        
        # Handle case if the issue changed from completed status to cancelled
        elif buccc_issue.get_status() == "CANCELLED":
            # Obatin the key of the issue, will be used to form as the blob's file path
            issue_key  = buccc_issue.data['key']

            # Obtain the form attached in the issue, that recorded the create/update BU CCC information
            buccc_form = buccc_issue.get_forms()[0]
            
            # Get the simplified answer(key value pairs) from the form attached to the issue
            unformated_form_answer = jira_client.get_form_simplified_answer(buccc_issue, buccc_form)
            formated_form_answer = {}
            
            # Format the form answers into a dictionary for easier access
            for key_values in unformated_form_answer:
                formated_form_answer[key_values['fieldKey']] = key_values['answer']

            # List all blobs in the Google Cloud Storage bucket with the prefix of today's date
            all_blobs_today = list(bucket.list_blobs(prefix=today_date))

            # Create a dictionary for easier access to blob information
            blobs_dict = return_blobs_dict(all_blobs_today)
            
            # Extract the cloud platform information from the formatted form answers and convert it to lowercase
            issue_cloud_platform = formated_form_answer['cloud_platform'].lower()

            # Check if the issue key exists in Google Cloud Storage for the specific cloud platform
            if issue_in_gcs(f'{issue_key}_cancelled', blobs_dict[issue_cloud_platform]):
                # If the cancelled issue's key is found in Google Cloud Storage for the specified cloud platform
                print("Issue resolved")
                # Continue to the next iteration without further processing
                continue
            
            # Obtain tthe array of resolved issue within a cloud platform
            resolved_issues = blobs_dict[issue_cloud_platform]

            # Locate the previous resolved issue blob (previous state)
            previous_resolved_issue = None
            for resolved_issue in resolved_issues:
                if issue_key in resolved_issue.name:
                    previous_resolved_issue = resolved_issues[resolved_issues.index(resolved_issue) - 1]

            # Download the previous_resolved_issue's csv at run time
            previous_resolved_issue_blob_data = previous_resolved_issue.download_as_string()
            
            # Convert it into a pandas dataframe for better processing
            previous_resolved_issue_df = pd.read_csv(io.BytesIO(previous_resolved_issue_blob_data))

            # Different cloud platform have different column name, so need to define it here for better retrieval of column name value
            cloud_project_name = {
                "aws": "ac_name",
                "gcp":  "Project_name",
                "azure": "Project_name"
            }

            # Obtain the backup CSV file with cloud platform provided
            bkup_csv = [blob for blob in all_blobs_today if 'backup' in blob.name.lower() and issue_cloud_platform in blob.name][0]
            bkup_csv = bkup_csv.download_as_string()
            bkup_csv= pd.read_csv(io.BytesIO(bkup_csv))

            # Check if it is a new project
            is_existing_project = formated_form_answer['project_name'] in bkup_csv[cloud_project_name[issue_cloud_platform]].values

            # Obtain the latest resolved issue blob
            latest_blob = obtain_latest_csv_file(blobs_dict[issue_cloud_platform])

            output = BytesIO()

            # Download the data at run time
            blob_data = latest_blob.download_as_string()

            # Convert the blob's data to a pandas DataFrame
            latest_df = pd.read_csv(io.BytesIO(blob_data))

            # Make update on the latest object
            if is_existing_project:
                # Indicate that the project already exists in the BUCCC table
                print("It's an existed project in the BUCCC table")

                # Check for the row corresponding to the project in the previous resolved issue csv
                condition = previous_resolved_issue_df[cloud_project_name[issue_cloud_platform]] == formated_form_answer['project_name']
                recovered_row = previous_resolved_issue_df.loc[condition].to_dict(orient='records')[0]

                # Update or append a row in the latest_df based on the recovered row data
                latest_df = substitute_value(latest_df, 
                                    issue_cloud_platform,
                                    recovered_row.get("Project_name"),
                                    recovered_row.get("project_id") if recovered_row.get("project_id") else "null",
                                    recovered_row.get("BU_billing"),
                                    recovered_row.get("Cost_Centre"),
                                    recovered_row.get('cost_split') if recovered_row.get('more_than_1_ccc') else "null", 
                                    recovered_row.get('IT_owner') if recovered_row.get('IT_owner') else "null")
                
                # Save the updated DataFrame to the output
                latest_df.to_csv(output, index=False) 

                output.seek(0)

                # Define the path for the modified CSV file based on the cloud platform
                output_path_name = {
                    "aws": f'{today_date}/aws_bu_ccc_{issue_key}_cancelled.csv',
                    "gcp": f'{today_date}/gcp_bu_ccc_{issue_key}_cancelled.csv',
                    "azure": f'{today_date}/azure_bu_ccc_{issue_key}_cancelled.csv'
                }
                
                # Upload the csv after modification to the cloud storage bucket
                destination_blob = bucket.blob(output_path_name[issue_cloud_platform])
                destination_blob.upload_from_file(output, content_type='csv')

                # Provide a success message indicating the completion of the automation process
                success_msg = f"{output_path_name[issue_cloud_platform]} cancel ticket automation success"
                print(success_msg)
                buccc_issue.add_comment(success_msg, style="paragraph", attachments=[], internal=True)

            else:
                print("It's a new project added into the BUCCC table")

                # Remove the row in the latest_df corresponding to the new project
                condition = latest_df[cloud_project_name[issue_cloud_platform]] == formated_form_answer['project_name']
                latest_df = latest_df.drop(latest_df.loc[condition].index)

                # Update it in the latest_blob
                latest_df.to_csv(output, index=False) 

                output.seek(0)

                # Define the path for the modified CSV file based on the cloud platform
                output_path_name = {
                    "aws": f'{today_date}/aws_bu_ccc_{issue_key}_cancelled.csv',
                    "gcp": f'{today_date}/gcp_bu_ccc_{issue_key}_cancelled.csv',
                    "azure": f'{today_date}/azure_bu_ccc_{issue_key}_cancelled.csv'
                }
                
                # Upload the csv after modification to the cloud storage bucket
                destination_blob = bucket.blob(output_path_name[issue_cloud_platform])
                destination_blob.upload_from_file(output, content_type='csv')

                # Provide a success message indicating the completion of the automation process
                success_msg = f"{output_path_name[issue_cloud_platform]} cancel ticket automation success"
                print(success_msg)
                buccc_issue.add_comment(success_msg, style="paragraph", attachments=[], internal=True)

    return {'message': 'All BU CCC create/update requests in JIRA handled'}, 200