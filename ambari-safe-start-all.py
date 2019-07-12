# Compatible to run on Py 3.x
# Safely starts all the Amabari services in a sequential and regulated fashion
import requests
import platform
import sys
import json
import time
import sys
import os
# Defining the connection settings variables

up_threshold = 300
cluster_id = "HDP3_1v1"
try:
    raw_input_pw = sys.argv
    admin_pw = raw_input_pw[1]
except Exception as e:
    print("Unable to proceed because of missing password. Please give the valid credentials as a command line argument to this script")
    sys.exit(0)


myhost = platform.node()
all_services = ['ZOOKEEPER', 'AMBARI_METRICS','KNOX', 'HDFS', 'YARN', 'MAPREDUCE2', 'HIVE', 'HBASE', 'SPARK2', 'OOZIE', 'AMBARI_INFRA_SOLR','ZEPPELIN','KAFKA','SMARTSENSE']
headers = {'X-Requested-By': 'ambari'}

response_tracker = []
desired_response_tracker = {}

# Starting the persistent request session
global_session = requests.Session()


#Checking the end sate of the service -
def check_service(service_name):
    try:
        status_check = global_session.get('http://'+myhost+':8080/api/v1/clusters/'+cluster_id+'/services/'+ service_name +'?fields=ServiceInfo/state', headers=headers, auth=('admin', admin_pw))
        if list(status_check.json().keys())[1] in ('ServiceInfo'):
            return status_check.json()['ServiceInfo']['state']
        else:
            print("Unable to fetch the check status of",service_name,". Please provide the valid connection settings or the password then try again later")
            sys.exit(0)
    except Exception as e:
        print("Unable to get the check status info of", service_name , "because - ", e)
        print("\n Please check your connection settings and try again later")
        sys.exit(0)
    # List of possible API values - https://github.com/apache/ambari/blob/trunk/ambari-server/docs/api/v1/service-resources.md

# Stop the service
def stop_service(service_name):
    data = '{"RequestInfo": {"context" : "Stop '+service_name+' via REST"}, "Body": {"ServiceInfo": {"state": "INSTALLED"}}}'
    try:
        stop_response = global_session.put('http://'+myhost+':8080/api/v1/clusters/'+cluster_id+'/services/'+service_name, headers=headers, data=data, auth=('admin', admin_pw))
        print(stop_response.text)
    except Exception as e:
        print("Unable to stop the service",service_name,  "because - ", e)
        print("\nPlease try again later")
        sys.exit(0)

# Start the service only if the end state is 'INSTALLED'
def start_service(service_name):
    if check_service(service_name) == 'INSTALLED':
        data = '{"RequestInfo": {"context" : "Start '+ service_name +' via REST"}, "Body": {"ServiceInfo": {"state": "STARTED"}}}'
        try:
            start_response = global_session.put('http://'+myhost+':8080/api/v1/clusters/'+cluster_id+'/services/'+service_name, headers=headers, data=data, auth=('admin', admin_pw))
            response_tracker.append({service_name:start_response.json()['href']})
            for items in response_tracker:
                for service_key in items.keys():
                    desired_response_tracker[service_key] = items
        except Exception as e:
            print("Unable to get the request tracking details of",service_name, "because of -", e)
            print("\nPlease try again later")
            sys.exit(0)

    elif check_service(service_name) in ('STARTING'):
        print("The service ", service_name, "is in starting phase. Will let it continue to start and queuing the immediate next service in meantime")

    else:
        print("The service is currently not ready to start or may experiencing problems. Safe stopping the rest of the services")
        for i in all_services:
            stop_service(i)

# Function to keep track the running status of the given service via async api
def async_status_tracker(service_name):
    try:
        async_status = global_session.get(desired_response_tracker[service_name][service_name],headers=headers, auth=('admin', admin_pw))
        if list(async_status.json()['Requests'].keys())[15] in ('request_status'):
            get_async_response = async_status.json()
        else:
            print("Unable to get the async status of",service_name,".Please check the connection settings and right version the cluster is pointing out to")
            sys.exit(0)
        return get_async_response['Requests']['request_status']
    except Exception as e:
        print("Unable to get the async request tracker details of",service_name, "because of -", e)
        print("\nPlease try again later")
        sys.exit(0)

# Function to keep track the percent progress of the given service via async api
def async_percent_tracker(service_name):
    try:
        async_status = global_session.get(desired_response_tracker[service_name][service_name],headers=headers, auth=('admin', admin_pw))
        if list(async_status.json()['Requests'].keys())[11] in ('progress_percent'):
            get_async_response = async_status.json()
        else:
            print("Unable to get progress percentage of the service",service_name,". Please check the connection settings and right version the cluster is pointing out to")
        return get_async_response['Requests']['progress_percent']
    except Exception as e:
        print("Unable to get the async request percentage details of",service_name, "because of -", e)
        print("\nPlease try again later")
        sys.exit(0)

# The main logic of starting the individual service as per the desired order and in the controlled fashion withing the threshold time to accomplish.
last_executed_service = []

try:
    for service in all_services:
        start_time = time.time()
        last_executed_service.append(service)
        if (check_service(last_executed_service[-1])) == 'INSTALLED':
            start_service(service)
            time.sleep(0.3)
            last_executed_service_start_time = time.time()
            while async_status_tracker(last_executed_service[-1]) in ('PENDING','IN_PROGRESS') and ((time.time() - start_time) < up_threshold ):
                time.sleep(0.3)
                print("\r> Run state of current executing service",last_executed_service[-1]," is", async_status_tracker(last_executed_service[-1]),"with progress of", int(async_percent_tracker(last_executed_service[-1])),'%', end = '')
            if ((time.time() - start_time) >  up_threshold ):
                print("The threshold time value exceeded ",round((time.time() - start_time),2),"secs", "Stopping the service")
                stop_service(service)
                sys.exit(0)
            elif async_status_tracker(last_executed_service[-1]) in ('ABORTED','FAILED'):
                print("Unable to start the service",last_executed_service[-1], "Not going forward to start rest of the _services")
                sys.exit(0)
            else:
                print("\nThe", last_executed_service[-1], "started running. Took almost", round((time.time() - last_executed_service_start_time - 0.3),2) , "secs")
                continue
            continue
        elif check_service(last_executed_service[-1]) in ('STARTED'):
            print("\n> The service",last_executed_service[-1], "is already in running state. Going forward to start the next service" )

        else:
            continue
except Exception as e:
    print("Unable to perform the operation - ", e)
    sys.exit(0)

# Closing the persistent request session as no further action is required.
global_session.close()
