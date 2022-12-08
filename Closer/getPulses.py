import sys
from azure.cosmos import CosmosClient
import json
import time
import datetime


days_back = 90
org_id = '7e589e84-9ca6-4297-aed7-21ee0d978421'

QUERY = "SELECT * FROM c WHERE c.pulse_header.organization_id = '" + org_id + "' AND c.pulse_payload.created_datetime >= DateTimeAdd(\"DD\", -91, GetCurrentDateTime()) ORDER BY c.pulse_payload.created_datetime"


print('=============================================')
print(f'Running Query:\n\n{QUERY}\n\nThis will make a file with the last 90 days of pulses for org_id: ' + org_id +', OK?')
print('=============================================')
resp = input('(y/N)\n')

if resp == 'n' or resp == 'N':
    print(f'Exiting')
    sys.exit(-1)

url = 'https://cosdbinstnightingale.documents.azure.com:443/'
key = '3KRXIQQe5ZEhoBZJLL7Hcgti6Pmmg5RnmCNWIyhS3V7pybW4T3Rd5ctMcHPVCJ7NuXKlX2BArltKGCQRYKOOAQ=='

client = CosmosClient(url, credential=key)

database_name = 'cosdbNightingale'
database = client.get_database_client(database_name)
container_name = 'collActivityFeed'
container = database.get_container_client(container_name)

all_items = container.query_items(query=QUERY, enable_cross_partition_query=True)

newList = []


for item in all_items:
    newList.append(item)

all_items_len = len(newList)
print(all_items_len)

newFile = open(".\\Jefferson_Cosmos_Last_90_days.json", "w")
newFile.write('[')

count = 0
for item in newList:
    newFile.write(str(item) + '\n')
    if all_items_len - 1 != count:
        newFile.write(',')

    time.sleep(.000000001)
    count +=1
    if count % 50 == 0:
        print(count)

newFile.write(']')
newFile.close()

print(f'\n\nQuery complete, {count} have been added to the file.')
