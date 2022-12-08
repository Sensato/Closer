from calendar import c
import sys, time
from azure.cosmos import CosmosClient

COPY_QUERY = "SELECT * FROM c WHERE c.pulse_header.action = 'msft_sentinel_incident' AND c.pulse_payload.msft_sentinel_incident.properties.createdTimeUtc >= '2022-10-04' AND c.pulse_payload.msft_sentinel_incident.properties.createdTimeUtc <= '2022-10-07'"

print('=============================================')
print(f'Running copy query:\n\nAre you sure you\'d like to continue, this will copy pulses from PROD to MADDIE.')
print('=============================================')
resp = input('(y/N)\n')

if resp != 'y':
    print(f'Exiting')
    sys.exit(-2)

prod_url = 'https://cosdbinstnightingale.documents.azure.com:443/'
prod_key = '3KRXIQQe5ZEhoBZJLL7Hcgti6Pmmg5RnmCNWIyhS3V7pybW4T3Rd5ctMcHPVCJ7NuXKlX2BArltKGCQRYKOOAQ=='

prod_client = CosmosClient(prod_url, credential=prod_key)
database_name = 'cosdbNightingale'
prod_database = prod_client.get_database_client(database_name)
container_name = 'collActivityFeed'
prod_container = prod_database.get_container_client(container_name)

dev_url = 'https://sngtest.documents.azure.com:443/'
dev_key = 'OolVRZZ7EDj7EXMRgLMWwXdHaTCP3dlFIzPuF1ixiZNM8HD2kv6YRmJfuEFAvNTRxc2ENHdkiRjxOYh6Bpnqiw=='

dev_client = CosmosClient(dev_url, credential=dev_key)
dev_database = dev_client.get_database_client(database_name)
dev_container = dev_database.get_container_client(container_name)

count = 0
for item in prod_container.query_items(
    query=COPY_QUERY,
    enable_cross_partition_query=True):

    dev_container.create_item(body=item)
    time.sleep(.000000001)
    count +=1
    if count % 50 == 0:
        print(count)

print(f'\n\n Update complete, {count} have been copied.')


