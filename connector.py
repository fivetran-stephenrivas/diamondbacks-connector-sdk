# This is a simple example for how to work with the fivetran_connector_sdk module.
# It defines a simple `update` method, which upserts some data to a table named "hello".
# This example is the simplest possible as it doesn't define a schema() function, however it does not therefore provide a good template for writing a real connector.
# See the Technical Reference documentation (https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update)
# and the Best Practices documentation (https://fivetran.com/docs/connectors/connector-sdk/best-practices) for details

# Import required classes from fivetran_connector_sdk
from fivetran_connector_sdk import Connector
from fivetran_connector_sdk import Logging as log
from fivetran_connector_sdk import Operations as op
import json
from datetime import datetime
import boto3
from botocore.config import Config

def schema(configuration: dict):
    return [
        {
            "table": "vehicles",  # Name of the table in the destination.
            "primary_key": ["vehicle_name"],  # Primary key column(s) for the table.
            "columns": {  # Define the columns and their data types.
                "vehicle_name": "STRING",  # String column for the period name.
                "arn": "STRING",  # UTC date-time column for the start time.
                "created": "UTC_DATETIME",  # UTC date-time column for the end time.
                "updated": "UTC_DATETIME",  # Integer column for the temperature.
                "model_manifest_arn": "STRING",  # UTC date-time column for the start time.
                "decorder_manifest_arn": "STRING",  # UTC date-time column for the start time.
                "attributes": "STRING",  # UTC date-time column for the start time.
            },
        }
    ]

# Define the update function, which is a required function, and is called by Fivetran during each sync.
# See the technical reference documentation for more details on the update function
# https://fivetran.com/docs/connectors/connector-sdk/technical-reference#update
# The function takes two parameters:
# - configuration: dictionary contains any secrets or payloads you configure when deploying the connector
# - state: a dictionary contains whatever state you have chosen to checkpoint during the prior sync
# The state dictionary is empty for the first sync or for any full re-sync
def update(configuration: dict, state: dict):
    # log.warning("Example: QuickStart Examples - Hello")
    
    # INITIALIZE API
    # session = boto3.Session(profile_name='csg-demo-admin')
    # client = session.client('iotfleetwise')
    # TODO: check that configuration has values
    client = boto3.client(
        'iotfleetwise',
        region_name = "us-east-1",
        aws_access_key_id = configuration['aws_access_key_id'],
        aws_secret_access_key = configuration['aws_secret_access_key']
    )
    log.info("=== API CLIENT CONFIGURED ===")
    #log.fine(f"=== AID: {configuration['aws_access_key_id']} ===")
    #log.fine(f"=== AK: {configuration['aws_secret_access_key']} ===")

    log.info("=== Start table: vehicles")
    # LIST VEHICLES API CALL
    response = client.list_vehicles()
    log.info("=== LIST VEHICLES API CALL COMPLETE ===")

    log.info("=== BUILDING ROWS ===")
    # BUILD ROWS FROM RESPONSE
    vehicles = []
    for item in response["vehicleSummaries"]:
        vehicles.append({  # Define the columns and their data types.
            "vehicle_name": item["vehicleName"],
            "arn": item["arn"],
            "created": item["creationTime"].isoformat(),
            "updated": item["lastModificationTime"].isoformat(),
            "model_manifest_arn": item["modelManifestArn"],
            "decorder_manifest_arn": item["decoderManifestArn"],
            "attributes": item["attributes"]
        })
        temp_time = datetime(item["creationTime"]).isoformat()
        print(type(temp_time))
        print(temp_time)
    log.info("=== BUILDING ROWS COMPLETE ===")

    # UPSERT ROWS TO FIVETRAN
    for row in vehicles:
        log.info("=== UPSERT DATA COMPLETE ===")    
        yield op.upsert(table="vehicles", data=row)
    log.info("=== UPSERT DATA COMPLETE ===")
    log.info(f"=== TOTAL ROWS: {len(vehicles)} ===")
    log.info("=== End table: vehicles")


# This creates the connector object that will use the update function defined in this connector.py file.
# This example does not use the schema() function. If it did, it would need to be included in the connector object definition. 
connector = Connector(update=update)

# Check if the script is being run as the main module.
# This is Python's standard entry method allowing your script to be run directly from the command line or IDE 'run' button.
# This is useful for debugging while you write your code. Note this method is not called by Fivetran when executing your connector in production.
# Please test using the Fivetran debug command prior to finalizing and deploying your connector.
if __name__ == "__main__":
    # Adding this code to your `connector.py` allows you to test your connector by running your file directly from your IDE:
    connector.debug()

# Resulting table:
# ┌───────────────┐
# │    message    │
# │    varchar    │
# ├───────────────┤
# │ hello, world! │
# └───────────────┘