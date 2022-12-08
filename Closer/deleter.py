import sys
from azure.cosmos import CosmosClient
import json
import time
import datetime


#QUERY= "SELECT * FROM c where c.pulse_header.action = 'hids_alarm' and c.pulse_payload.status != 'closed' and c.alarm_data.rule.sidid = 18151 and c.pulse_header.organization_id = '207D3ACC-B87F-42BA-BFD9-769F726E3C45' and c.alarm_data.full_log like '%AUDIT_FAILURE(4673)%' and c.alarm_data.full_log like '%\chrome.exe%'"

#QUERY = "SELECT * FROM c where c.pulse_header.action = 'hids_alarm' and c.pulse_payload.status != 'closed' and c.alarm_data.rule.sidid = 18113 and c.pulse_header.organization_id = 'E5412F90-D3CB-4BA8-8994-C5DD08B6F216' and c.alarm_data.full_log like '%WinEvtLog: Security: AUDIT_SUCCESS(4907)%' and c.alarm_data.full_log like '%TiWorker.exe%'"

#NEW_QUERY = 'where c.pulse_header.action = 'hids_alarm' and c.alarm_data.full_log like '%AUDIT_FAILURE(5152)%' and c.pulse_payload != 'closed' and c.pulse_header.organization_id = '''


# where c.pulse_header.action = 'hids_alarm' and c.pulse_payload.status != 'closed' and c.alarm_data.rule.sidid = 18153 and c.pulse_header.organization_id != '9555EDD8-1E67-428C-8132-E9012CCB58F0'


print('=============================================')
print(f'Running Query:\n\n{QUERY}\n\nAre you sure you\'d like to continue, this is a destructive action that cannot be reversed')
print('=============================================')
resp = input('(y/N)\n')

if resp != 'y':
    print(f'Exiting')
    sys.exit(-2)

url="https://sngtest.documents.azure.com:443/"
key="rz7Y0IVBoasEMSSwyuVWM7XswkjHxpnaoSb3U8wpuaKT5CeDRnhY8pdHIEc7hXBuBFItlInPn2Z6X9zyOG17eQ=="


client = CosmosClient(url, credential=key)

database_name = 'cosdbNightingale'
database = client.get_database_client(database_name)
container_name = 'collActivityFeed'
container = database.get_container_client(container_name)

#save = open('saver.log', 'a+')
count = 0
for item in container.query_items(
        query=QUERY,
        enable_cross_partition_query=True):
        container.delete_item(body=item)
        time.sleep(.000000001)
        count +=1
        if count % 50 == 0:
            print(count)

            
        

print(f'\n\n Update complete, {count} has been deleted.')
