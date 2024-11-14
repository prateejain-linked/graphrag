import logging
import os
import sys

import azure.functions as func
from utility import find_next_target_index_blob, water_mark_target

from graphrag.common.storage.blob_pipeline_storage import BlobPipelineStorage
from graphrag.common.storage.queue_storage import QueueStorageClient
from graphrag.index.cli import index_cli
from graphrag.index.context_switch.context_manager import ContextManager

context_functions = func.Blueprint()

# Create a handler that writes log messages to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(handler)

def initialize_incoming_msg_queue() -> QueueStorageClient:
    max_messages = int(os.environ.get("MAX_QUEUE_MESSAGE_COUNT", default="1"))
    queue_url = os.environ.get("AZURE_QUEUE_URL")
    queue_name = os.environ.get("AZURE_CTX_QUEUE_NAME")
    client_id = os.environ.get("AZURE_CLIENT_ID")

    queue_storage_client = QueueStorageClient(account_url=queue_url, queue_name=queue_name, client_id=client_id, max_message=max_messages)

    return queue_storage_client

def initialize_watermark_client() -> BlobPipelineStorage:
    # blob_account_url = 'https://inputdatasetsa.blob.core.windows.net'
    # watermark_container_name='watermark'

    blob_account_url = os.environ.get("AZURE_WATERMARK_ACCOUNT_URL")
    watermark_container_name = os.environ.get("WATERMARK_CTX_CONTAINER_NAME")
    client_id = os.environ.get("AZURE_CLIENT_ID")

    watermark_storage_account = BlobPipelineStorage(connection_string=None, container_name=watermark_container_name, storage_account_blob_url=blob_account_url)

    return watermark_storage_account


@context_functions.function_name('contextpoller')
@context_functions.timer_trigger(schedule="0 */1 * * * *", arg_name="mytimer", run_on_startup=True)
def indexing(mytimer: func.TimerRequest) -> None:
    logging.info('Python HTTP trigger function processed a request.')

    input_base_dir=None
    # if "input_base_dir" in req.params:
    #     input_base_dir = req.params['input_base_dir']
    output_base_dir=None

    queue_client = initialize_incoming_msg_queue()
    watermark_client = initialize_watermark_client()

    targets = find_next_target_index_blob(queue_storage_client=queue_client, watermark_client=watermark_client, caller='context')
    if len(targets) <= 0:
        logging.info("No target to index. Silently skipping the iteration")
        return

    #file_targets: list[str] = []
    for target in targets:
        file_target = target[1]
        #input for the artifcact storage account
        # context switching for all the target blobs.
        context_id = target[0].split("/")[0]
        try:
            index_cli(
                root = "settings",
                verbose=False,
                resume=False,
                memprofile=False,
                nocache=False,
                config=None,
                emit=None,
                dryrun=False,
                init=False,
                overlay_defaults=False,
                cli=True,
                context_id=context_id,
                context_operation='activate',
                community_level=2,
                use_kusto_community_reports=None,
                optimized_search=None,
                input_base_dir=input_base_dir,
                output_base_dir=output_base_dir,
                files=[file_target]
            )
            logging.info("Successfully processed the message from the queue")

            water_mark_target(targets=[target[1:]], queue_storage_client=queue_client, watermark_client=watermark_client, path_prefix=context_id)
        except:
            logging.error("Error executing the function")
            raise

@context_functions.function_name('contextpollv2')
@context_functions.route(route="contextpoll", auth_level=func.AuthLevel.FUNCTION)
def context_poll(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    input_base_dir=None
    # if "input_base_dir" in req.params:
    #     input_base_dir = req.params['input_base_dir']

    output_base_dir=None
    # if "output_base_dir" in req.params:
    #     output_base_dir = req.params['output_base_dir']

    queue_client = initialize_incoming_msg_queue()
    watermark_client = initialize_watermark_client()

    targets = find_next_target_index_blob(queue_storage_client=queue_client, watermark_client=watermark_client, caller='context')
    if len(targets) <= 0:
        logging.info("No target to index. Silently skipping the iteration")
        return func.HttpResponse(
            "No content to polled for the context",
            status_code=200
        )

    #file_targets: list[str] = []
    for target in targets:
        file_target = target[1]
        #input for the artifact storage account
        # context switching for all the target blobs.
        context_id = target[0].split("/")[0]
        try:
            index_cli(
                root = "settings",
                verbose=False,
                resume=False,
                memprofile=False,
                nocache=False,
                config=None,
                emit=None,
                dryrun=False,
                init=False,
                overlay_defaults=False,
                cli=True,
                context_id=context_id,
                context_operation='activate',
                community_level=2,
                use_kusto_community_reports=None,
                optimized_search=None,
                input_base_dir=input_base_dir,
                output_base_dir=output_base_dir,
                files=[file_target]
            )
            logging.info("Successfully processed the message from the queue")

            water_mark_target(targets=[target[1:]], queue_storage_client=queue_client, watermark_client=watermark_client, path_prefix=context_id)
            return func.HttpResponse(
                "Successfully processed the create / initialized request",
                status_code=200
            )
        except Exception as ex:
            logging.error(ex)
            return func.HttpResponse(
                "The request failed to be processed",
                status_code=500
            )

@context_functions.function_name('contextmanager')
@context_functions.route(route="context", auth_level=func.AuthLevel.FUNCTION)
def context_switch(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    logging.info("Parameters: "+str(req.params))

    if 'req_type' not in req.params:
        return func.HttpResponse(
            "The Request must have req_type parameters passed.",
            status_code=400
        )

    req_type: str = req.params['req_type']
    req_type = req_type.lower()
    context_name = req.params.get('context_name',None)
    if context_name is None or len(context_name) <= 0:
        return func.HttpResponse(
                f"The {req_type} request must be passed with context name",
                status_code=400
            )

    content_mgr = ContextManager(context_name=context_name)

    try:
        if req_type == 'create' or req_type == 'update':
            content_ids = req.params.get('content_ids',None)

            if content_ids is None or len(content_ids) <= 0:
                return func.HttpResponse(
                    f"The {req_type} request must be passed with context name and content ids to initialize.",
                    status_code=400
                )
            files = content_ids.split(";")

            if(req_type == 'create'):
                content_mgr.initialize(files=files)
            else:
                content_mgr.update(files=files)

        elif req_type == 'switch':
            content_mgr.switch_context_state()

        else:
            return func.HttpResponse(
                f"Unsupported {req_type}",
                status_code=400
            )
        return func.HttpResponse(
            "Successfully processed the create / initialized request",
            status_code=200
        )
    except Exception as ex:
        logging.error(ex)
        return func.HttpResponse(
            "The request failed to be processed",
            status_code=500
        )


from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
from time import sleep
import json

@context_functions.route(route="context-data")
def context_data(req: func.HttpRequest) -> func.HttpResponse:
    '''Retrieve context data from Azure Blob'''
    logging.info('HTTP route to retrieve context data.')

    # Azure Blob Storage connection details
    storage_account_blob_url = os.getenv('AZURE_WATERMARK_ACCOUNT_URL')
    container_name = "context2"
    if 'cube_id' in req.params:
        cube_id = req.params['cube_id']
        if cube_id == '1':
            container_name = "context"
        else:
            container_name = "context" + req.params['cube_id']
    blob_name = req.params.get('blob_name')

    try:
        # Create the BlobServiceClient object
        if 'AZURE_CLIENT_ID' in os.environ:
            print("AZURE_CLIENT_ID: ", os.environ['AZURE_CLIENT_ID'])

        # credential = DefaultAzureCredential()
        credential = DefaultAzureCredential(managed_identity_client_id=os.environ['AZURE_CLIENT_ID'], exclude_interactive_browser_credential = False)
        print("DefaultAzureCredential: ", credential)
        sleep(1)
        blob_service_client = BlobServiceClient(
                account_url=storage_account_blob_url,
                credential=credential,
            )
        print("Successfully connected to Blob Storage")

        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)

        # Get the blob client
        if blob_name is not None:
            blob_client = container_client.get_blob_client(blob_name)
            blob_data = blob_client.download_blob().readall()
        else:
            # Return a list of all blobs in the container
            blobs = container_client.list_blobs()
            # Convert the blobs to a list
            blob_data = [blob.name for blob in blobs]
            # Filter to just get the files which follow the pattern *_init.json
            blob_data = [blob for blob in blob_data if blob.endswith("_init.json")]
            # Read each blob content and return a list of all the jsons
            blob_data = [container_client.get_blob_client(blob).download_blob().readall().decode('utf-8') for blob in blob_data]
            # Test only returning an array of the first blob
            # blob_data = blob_data[0]
            # as an array
            # blob_data = [blob_data]

            print(blob_data)
            return func.HttpResponse(
                body=json.dumps(blob_data),
                status_code=200,
                mimetype="application/json"
            )

        # Download the blob content

        return func.HttpResponse(
            body=blob_data,
            status_code=200,
            mimetype="application/octet-stream"
        )
    except Exception as e:
        logging.error(f"Error querying Blob Storage: {e}")
        return func.HttpResponse(
            body="Error querying Blob Storage",
            status_code=500
        )


def executing_correct_func_app(req: func.HttpRequest, route: str):
    return os.getenv("ENVIRONMENT") == "AZURE" and  os.getenv("APP_NAME")!= route

