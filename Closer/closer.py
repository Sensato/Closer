import sys
from azure.cosmos import CosmosClient
import json
import time
import datetime


QUERY= "SELECT * FROM c where c.pulse_header.pulse_sub_title = 'arch-hp-2' AND c.pulse_payload.status != 'closed'"

print('=============================================')
print(f'Running Query:\n\n{QUERY}\n\nAre you sure you\'d like to continue, this is a destructive action that cannot be reversed')
print('=============================================')
resp = input('(y/N)\n')

if resp != 'y':
    print(f'Exiting')
    sys.exit(-2)

url = 'https://cosdbinstnightingale.documents.azure.com:443/'
key = '3KRXIQQe5ZEhoBZJLL7Hcgti6Pmmg5RnmCNWIyhS3V7pybW4T3Rd5ctMcHPVCJ7NuXKlX2BArltKGCQRYKOOAQ=='

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

        if True:
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
