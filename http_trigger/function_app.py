import azure.functions as func
import logging
import json
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Configuration variables
KEY_VAULT_NAME = "toss-function-eventhub"
SECRET_NAME = "toss-function-connection-string"
vault_url = f"https://{KEY_VAULT_NAME}.vault.azure.net/"

# Get Event Hub connection string from Azure Key Vault
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)
EVENT_HUB_CONNECTION_STR = client.get_secret(SECRET_NAME).value
EVENT_HUB_NAME = "toss-eventhub"

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="http_trigger")
async def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Extract data from the request
    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Send data to Event Hub
    await send_to_eventhub(data)

    return func.HttpResponse("Data sent to Event Hub successfully.", status_code=200)


async def send_to_eventhub(data):
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONNECTION_STR, eventhub_name=EVENT_HUB_NAME
    )
    async with producer:
        event_data = EventData(json.dumps(data))
        await producer.send_batch([event_data])  # Send individual event
        logging.info(f"Sent event: {data}")

if __name__ == '__main__':
    app.run()

