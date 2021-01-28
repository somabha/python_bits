import os
import json
import boto3
import time
import re
import numpy as np
import pandas as pd
import urllib.parse
import simplejson
from airflow import DAG
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow import utils
from airflow.models import Variable, TaskInstance, DagRun
from slack_webhook_operator import SlackWebhookOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from dataflow_util import *
from collections import defaultdict
import logging


class FileNotFoundError(Exception):
    pass


# get env variables
get_param = Variable.get("airflow_param", deserialize_json=True)
src_bucket = get_param.get("staging_bucket_name")
tgt_bucket = get_param.get("shared_bucket_name")
cmsInSqsQueues = get_param.get("cms_sqs_queues")
http_proxy_connection = BaseHook.get_connection("http_proxy")
http_proxy = str(http_proxy_connection.host) + ":" + str(http_proxy_connection.port)
adobe_analytics_max_key_value = get_param.get("adobe_analytics_max_key_value")
# DateTime variables
airflow_efs_path = get_param.get("airflow_efs_path")
execution_date = '''{{macros.ds_format(macros.ds_add(ds,0),"%Y-%m-%d","%Y-%m-%d")}}'''
prev_execution_date = '''{{macros.ds_format(macros.ds_add(ds,-1),"%Y-%m-%d","%Y-%m-%d")}}'''
execution_date_f1 = '''{{macros.ds_format(macros.ds_add(ds,0),"%Y-%m-%d","%Y%m%d")}}'''
prev_execution_date_f1 = '''{{macros.ds_format(macros.ds_add(ds,-1),"%Y-%m-%d","%Y%m%d")}}'''

# S3 connection handle
s3 = boto3.resource('s3')
client = boto3.client('s3')

# Define Airflow DAG
dag_name = "ingest-adobe-analytics-mom-feeds"
description = "Ingest analytics month on month data feeds as CSV for each advertiser. We post SQS message to import reports to NCG Dashboard"
dag_type = "S3-JSON-SQS"
slack_message = dag_name + " - failed !"

# S3 locations: source and destination
src_bucket_key = get_param.get("adobe_analytics_src_bucket_key")
tgt_bucket_key = get_param.get("adobe_analytics_target_bucket_key") + prev_execution_date_f1 + '/'
processed_bucket_key = get_param.get('adobe_analytics_processed_bucket_key')
sqs_bucket_key = tgt_bucket_key


# Send Message to Slack channel on task failure
def slack_failed_task(context):
    failed_alert = SlackWebhookOperator(task_id="notify_slack",
                                        http_conn_id="slack_web_hook_alerts_ddp",
                                        message=slack_message,
                                        proxy=http_proxy,
                                        username="Airflow")
    return failed_alert.execute(context=context)


default_args = {'owner': 'Airflow',
                'depends_on_past': True,
                'on_failure_callback': slack_failed_task,
                'start_date': utils.dates.days_ago(1)
                }
# Define Airflow DAG
dag = DAG(dag_name,
          description,
          schedule_interval='0 8,9,10,11,12 1 * *',
          default_args=default_args
          )


def create_sqs_message_body(**kwargs):
    ti = kwargs['ti']
    report_file_names = ti.xcom_pull(task_ids='generate_report', key='report_file_names')
    current_execution_date = kwargs["execution_date"]
    messageId = "datahub_adobe_analytics_dashboard_{}".format(current_execution_date.strftime("%Y-%m-%d %H:%M:%S"))
    tgt_bucket = kwargs.get('templates_dict').get('tgt_bucket')
    tgt_bucket_key = kwargs.get('templates_dict').get('tgt_key')
    processed_file_loc = kwargs.get('templates_dict').get('processed_file_loc')
    advertisers_list = kwargs.get('templates_dict').get('advertisers_list')
    cmsInSqsQueues = kwargs.get('templates_dict').get('cmsInSqsQueues')
    cmsInSqsQueues = cmsInSqsQueues.split(',')

    payload_list = []
    if report_file_names:
        for file in report_file_names:
            timestamp = str(int(time.time()))
            count = 1
            advertiser = ''
            advertiser = file.split("-")[0]
            status = "BESPOKE-REPORT-COMPLETE"
            payload = {
                "source": "DataHub",
                "message_id": messageId + '_' + timestamp,
                "message_type": "response",
                "job_name": "BespokeDashboard",
                "status": status,
                "result": {
                    "report_for": {
                        "type": "analytics_report",
                        "group_name": "adobe-analytics",
                        "advertiser": advertiser,
                        "processed_file_loc": processed_file_loc,
                        "count": str(count)
                    },
                    "report_data_location": {
                        "bucket": tgt_bucket,
                        "key": tgt_bucket_key + file
                    }
                },
                "created_ts": timestamp
            }
            json_data = json.dumps(payload)
            # Send the SQS Message
            set_region_cmd = 'export AWS_DEFAULT_REGION=ap-southeast-2'
            for cmsInSqsQueue in cmsInSqsQueues:
                sqs_notification_cmd = set_region_cmd + " && aws sqs send-message --queue-url " + cmsInSqsQueue + " --message-body " + "'" + json_data + "'" + " --delay-seconds 10"
                # Issue the bash command
                print("sqs_notification_cmd:{}".format(sqs_notification_cmd))
                return_value = os.system(sqs_notification_cmd)
                if return_value != 0:
                    print("The system command exited with return code :{}".format(return_value))


def convert(o):
    if isinstance(o, np.int64):
        return int(o)


def check_file_availability(**kwargs):
    ti = kwargs['ti']
    src_bucket = kwargs.get('templates_dict').get('src_bucket')
    src_key = kwargs.get('templates_dict').get('src_key')

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(src_bucket)
    objs = list()
    objs = list(bucket.objects.filter(Prefix=src_key))
    print("objs:{}".format(objs))
    mom_file_list = []
    mom_file_regex = '(.*)' + '-analytics-' + '(.*)' + '-' + '(.*)' + '-mom-' + '(.*)' + '-' + '\d\d\d\d\d\d\d\d' + '-' + '\d\d\d\d\d\d\d\d' + '.csv'
    # Collect all MoM files into a list (irrespective of advertisers
    for obj in objs:
        if re.match(mom_file_regex, obj.key):
            if re.search(r'counts', obj.key) or re.search(r'key_value', obj.key):
                print(obj.key)
                filename = obj.key.replace(src_key, "")
                mom_file_list.append(filename)

    # Check if the files were found else abort
    if not mom_file_list:
        print("File not found!")
        return "stop_work_flow"

    # Group files according to advertiser into separate list of files: we segragate based on dates later

    advertisers_groups = defaultdict(list)
    for file in mom_file_list:
        advertisers_groups[file.split("-")[0]].append(file)

    # Share the list of advertser names
    advertisers_list = [key for key in advertisers_groups.keys() if key != 'default']
    if advertisers_list:
        ti.xcom_push(key='advertisers_list', value=advertisers_list)
    else:
        return "stop_work_flow"

    # Share the list of files for each advertiser group
    for key in advertisers_groups.keys():
        advlist = advertisers_groups[key]
        ti.xcom_push(key=key + '_file_list', value=advlist)

    print("Pushed all file lists into xcom!")

    return "generate_report"


def generate_report(**kwargs):
    ti = kwargs['ti']
    LOGGER = logging.getLogger("airflow.task")
    src_bucket = kwargs.get('templates_dict').get('src_bucket')
    tgt_bucket = kwargs.get('templates_dict').get('tgt_bucket')
    tgt_key = kwargs.get('templates_dict').get('tgt_key')
    src_key = kwargs.get('templates_dict').get('src_key')
    processed_bucket_key = kwargs.get('templates_dict').get('processed_bucket_key')
    current_execution_date = kwargs["execution_date"]
    current_execution_date = current_execution_date.strftime("%Y-%m-%d %H:%M:%S")
    report_file_names = []
    # Collect the list of advertisers
    advertisers_list = ti.xcom_pull(task_ids='check_file_availability', key='advertisers_list')
    print("advertisers_list:{}".format(advertisers_list))
    # For each advertiser, collect the corresponding file list
    if advertisers_list:
        for advertiser in advertisers_list:
            file_list = ti.xcom_pull(task_ids='check_file_availability', key=advertiser + '_file_list')
            print("file_list:{}".format(file_list))

            # Seperate the list of files start_date & end_date pair wise and generate report for each list
            date_group = defaultdict(list)
            if file_list:
                for file in file_list:
                    filename = file.replace('.csv', '')
                    # File name validation has been done earlier, the filename conforms to IC
                    date_group[filename.split("-")[7]].append(file)
            else:
                LOGGER.error("The file_list could not be fetched from xcom (check_file_availability)""")

            for key in date_group.keys():
                file_list = date_group[key]
                report_file_name = ''
                list_of_filename_components = file_list[0].split('-')
                # Dictionary to contain final JSON file structure
                json_data = {}
                json_data["advertiser"] = advertiser
                json_data["period"] = 'mom'
                json_data["start_date"] = ''
                json_data["end_date"] = ''
                json_data["data"] = {}
                start_date = ''
                end_date = ''
                for filename in file_list:
                    print("filename:{}".format(filename))
                    file = filename.replace('.csv', '')
                    list_of_filename_components = file.split('-')
                    start_date = list_of_filename_components[6]
                    end_date = list_of_filename_components[7]
                    audiencename = list_of_filename_components[2]
                    report_name = list_of_filename_components[3]
                    file_format = list_of_filename_components[5]
                    json_data["start_date"] = start_date
                    json_data["end_date"] = end_date
                    if audiencename not in json_data["data"]:
                        json_data["data"][audiencename] = {}
                    json_data["data"][audiencename][report_name] = {}

                    # Check if empty file
                    df = pd.DataFrame()
                    try:
                        df = pd.read_csv("s3://" + src_bucket + '/' + src_key + filename)
                    except pd.errors.EmptyDataError:
                        LOGGER.warn("Empty file:{}".format(filename))
                    if not df.empty:
                        if file_format == 'key_value':
                            df = df[df[df.columns[0]].notna()]
                            # Filter out rows whih has count as < adobe_analytics_max_key_value
                            # filter where views is 0
                            # Key Value must have only two columns
                            if len(df.columns.tolist()) == 2:
                                # Sort by values coulumn
                                dfSorted = df.sort_values(by=[df.columns.tolist()[1]], ascending=False)
                                # Drop rows with  value < 100 in column count
                                dfSortedFiltered = dfSorted[dfSorted[dfSorted.columns[1]] > 0]
                                names = list(dfSortedFiltered[dfSortedFiltered.columns[0]])
                                values = list(dfSortedFiltered[dfSortedFiltered.columns[1]])
                                # Count the number of rows where views > 100
                                count_of_zero_values = dfSortedFiltered[dfSortedFiltered[dfSortedFiltered.columns[1]] > 100].count()[0]
                                n = len(names)
                                # if count is > 1000 then load the records where views is > 100
                                if count_of_zero_values > 1000:
                                    for x in range(0, n):
                                        if values[x] < adobe_analytics_max_key_value:
                                            continue
                                        else:
                                            json_data["data"][audiencename][report_name][names[x]] = values[x]
                                else:
                                    # if count is < 1000 then load top 1000 or n records
                                    number_of_rows = dfSortedFiltered.count()[0]
                                    if number_of_rows < 1000:
                                        for x in range(0, number_of_rows):
                                            json_data["data"][audiencename][report_name][names[x]] = values[x]
                                    elif number_of_rows >= 1000:
                                        for x in range(0, 1000):
                                            json_data["data"][audiencename][report_name][names[x]] = values[x]

                            else:
                                LOGGER.warn("There are more than 2 columns in this key_value file:{}".format(filename))

                        elif file_format == 'counts':
                            for column in df.columns:
                                json_data["data"][audiencename][report_name][column] = df[column][0]
                        else:
                            LOGGER.error("File format 'FFF' is neither key_value nor counts, it is:{}".format(file_format))
                    else:
                        if list(df.columns.values):
                            for column in df.columns:
                                json_data["data"][audiencename][report_name][column] = ''
                        else:
                            LOGGER.error("The file:{} is empty!".format(filename))

                json_data = simplejson.dumps(json_data, ignore_nan=True, default=convert)
                # Write into S3
                print("Write into S3")
                timestamp = str(int(time.time()))
                report_file_name = advertiser + '-analytics-mom-' + start_date + '-' + end_date + '_' + timestamp + '.json'
                s3object = s3.Object(tgt_bucket, tgt_key + report_file_name)
                s3object.put(Body=(bytes(json_data.encode('UTF-8'))))
                if report_file_name:
                    report_file_names.append(report_file_name)
    else:
        LOGGER.error("No Advertisers List was found!!!")

    if report_file_names:
        ti.xcom_push(key='report_file_names', value=report_file_names)
        ti.xcom_push(key='tgt_bucket', value=tgt_bucket)
        ti.xcom_push(key='tgt_key', value=tgt_key)
        ti.xcom_push(key='advertisers_list', value=advertisers_list)


def move_to_processed_folder(**kwargs):
    ti = kwargs['ti']
    LOGGER = logging.getLogger("airflow.task")
    src_bucket = kwargs.get('templates_dict').get('src_bucket')
    src_bucket_key = kwargs.get('templates_dict').get('src_bucket_key')
    processed_bucket_key = kwargs.get('templates_dict').get('processed_bucket_key')
    current_execution_date = kwargs["execution_date"]
    current_execution_date = current_execution_date.strftime("%Y-%m-%d %H:%M:%S")
    # Collect the list of advertisers
    advertisers_list = ti.xcom_pull(task_ids='check_file_availability', key='advertisers_list')
    # For each advertiser, collect the corresponding file list
    if advertisers_list:
        for advertiser in advertisers_list:
            file_list = ti.xcom_pull(task_ids='check_file_availability', key=advertiser + '_file_list')
            if file_list:
                for filekey in file_list:
                    filename = filekey.replace(src_bucket_key, "")
                    copy_source = {
                        'Bucket': src_bucket,
                        'Key': src_bucket_key + filename
                    }
                    print("move this file:{}".format(filename))
                    destination = 's3://' + src_bucket + '/' + processed_bucket_key + current_execution_date + '/' + filename
                    client.copy_object(CopySource=copy_source, Bucket=src_bucket,
                                       Key=processed_bucket_key + current_execution_date + '/' + filename)
                    client.delete_object(Bucket=src_bucket, Key=src_bucket_key + filename)
    else:
        LOGGER.error("No advertisers list was found!!!")
    ti.xcom_push(key='processed_file_loc', value=processed_bucket_key)
    return "Success"


# ********************************************************************************************************
# Operators starts
# ********************************************************************************************************

start_task = DummyOperator(task_id='start_task',
                           depends_on_past=True,
                           wait_for_downstream=True,
                           dag=dag
                           )

check_file_availability = BranchPythonOperator(
    task_id='check_file_availability',
    python_callable=check_file_availability,
    templates_dict={
        "src_bucket": src_bucket,
        "src_key": src_bucket_key,
    },
    provide_context=True,
    dag=dag
)

generate_report = PythonOperator(
    task_id="generate_report",
    python_callable=generate_report,
    templates_dict={
        "src_bucket": src_bucket,
        "src_key": src_bucket_key,
        "tgt_bucket": tgt_bucket,
        "tgt_key": tgt_bucket_key,
        "run_date": prev_execution_date_f1,
        "processed_bucket_key": processed_bucket_key
    },
    provide_context=True,
    dag=dag
)

move_to_processed_folder = PythonOperator(
    task_id="move_to_processed_folder",
    python_callable=move_to_processed_folder,
    templates_dict={
        "src_bucket": src_bucket,
        "src_bucket_key": src_bucket_key,
        "processed_bucket_key": processed_bucket_key
    },
    provide_context=True,
    dag=dag
)

create_sqs_message_body = PythonOperator(task_id='create_sqs_message_body',
                                         python_callable=create_sqs_message_body,
                                         templates_dict={
                                             "advertisers_list": "{tgt_bucket}".format(
                                                 tgt_bucket="{{ task_instance.xcom_pull('generate_report', key='advertisers_list')}}"),
                                             "tgt_bucket": "{tgt_bucket}".format(
                                                 tgt_bucket="{{ task_instance.xcom_pull('generate_report', key='tgt_bucket')}}"),
                                             "tgt_key": "{tgt_key}".format(
                                                 tgt_key="{{ task_instance.xcom_pull('generate_report', key='tgt_key')}}"),
                                             "cmsInSqsQueues": cmsInSqsQueues,
                                             "processed_file_loc": "{processed_file_loc}".format(
                                                 processed_file_loc="{{ task_instance.xcom_pull('move_to_processed_folder', key='processed_file_loc')}}")
                                         },
                                         provide_context=True,
                                         dag=dag
                                         )

finish_work_flow = DummyOperator(task_id='finish_work_flow', dag=dag, trigger_rule="one_success")
stop_work_flow = DummyOperator(task_id='stop_work_flow', dag=dag)

start_task >> check_file_availability >> generate_report >> move_to_processed_folder >> create_sqs_message_body >> finish_work_flow

check_file_availability >> stop_work_flow >> finish_work_flow
