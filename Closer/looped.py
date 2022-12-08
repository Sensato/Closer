import sys
from azure.cosmos import CosmosClient
import json
import time
import datetime


QUERY= "SELECT *  FROM c where c.pulse_header.action = 'hids_alarm' and c.pulse_payload.status != 'closed' and c.alarm_data.rule.sidid = 18151 and c.pulse_header.organization_id = '7cb29c78-edaf-4737-94dd-d6122dcf1bb0' and c.alarm_data.full_log like '%AUDIT_FAILURE(4673)%' and c.alarm_data.full_log like '%\chrome.exe%'"

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

#url = 'https://cosdbinstnightingale.documents.azure.com:443/'
#key = '3KRXIQQe5ZEhoBZJLL7Hcgti6Pmmg5RnmCNWIyhS3V7pybW4T3Rd5ctMcHPVCJ7NuXKlX2BArltKGCQRYKOOAQ=='

client = CosmosClient(url, credential=key)

database_name = 'cosdbNightingale'
database = client.get_database_client(database_name)
container_name = 'collActivityFeed'
container = database.get_container_client(container_name)
total = 0
#save = open('saver.log', 'a+')
while True:
    count = 0
    for item in container.query_items(
            query=QUERY,
            enable_cross_partition_query=True):

            if 'AUDIT_FAILURE(4673)' in item['alarm_data']['full_log']:
                #print("Found one")

                item['pulse_payload']['status'] = 'closed'
                item['pulse_payload']['analyst'] = 'fdee66be-2fc4-4504-9c69-7f44a2506c36'
                #"2021-10-06T16:28:15.9222092Z",
                item['pulse_payload']['last_update_time'] = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f') + 'Z'
                item['pulse_payload']['last_update_by'] = 'fdee66be-2fc4-4504-9c69-7f44a2506c36'
                #print(json.dumps(item, indent=5))
                #time.sleep(100)
                #print('get ready\a')
                #time.sleep(30)
                container.upsert_item(body=item)
                time.sleep(.000000001)
                count +=1
                if count % 50 == 0:
                    print(count)
            else:
                print("No matching found")
                
            

    print(f'\n\n Update complete, {count} has been deleted.')
    total += count
    print(f'\n\n Update complete, {total} has been deleted.')
    time.sleep(180) # Run every 3 minutes


