import logging
import os
import sys

import azure.functions as func
from utility import find_next_target_index_blob, water_mark_target

from graphrag.common.storage.blob_pipeline_storage import BlobPipelineStorage
from graphrag.common.storage.queue_storage import QueueStorageClient
from graphrag.index.cli import index_cli

index_functions = func.Blueprint()

# Create a handler that writes log messages to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(handler)

def initialize_incoming_msg_queue() -> QueueStorageClient:
    max_messages = int(os.environ.get("MAX_QUEUE_MESSAGE_COUNT", default="1"))
    queue_url = os.environ.get("AZURE_QUEUE_URL")
    queue_name = os.environ.get("AZURE_QUEUE_NAME")
    client_id = os.environ.get("AZURE_CLIENT_ID")

    queue_storage_client = QueueStorageClient(account_url=queue_url, queue_name=queue_name, client_id=client_id, max_message=max_messages)

    return queue_storage_client

def initialize_watermark_client() -> BlobPipelineStorage:
    blob_account_url = os.environ.get("AZURE_WATERMARK_ACCOUNT_URL")
    watermark_container_name = os.environ.get("WATERMARK_CONTAINER_NAME")
    client_id = os.environ.get("AZURE_CLIENT_ID")

    watermark_storage_account = BlobPipelineStorage(connection_string=None, container_name=watermark_container_name, storage_account_blob_url=blob_account_url)

    return watermark_storage_account


def process_indexing():
    queue_client = initialize_incoming_msg_queue()
    watermark_client = initialize_watermark_client()

    targets = find_next_target_index_blob(queue_storage_client=queue_client, watermark_client=watermark_client, caller="indexer")
    if len(targets) <= 0:
        logging.info("No target to index. Silently skipping the iteration")
        return "No target to index"

    file_target = []
    for target in targets:
        file_target.append(target[0])

    try:
        index_cli(
            root="settings",
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
            context_id=None,
            context_operation=None,
            community_level=2,
            use_kusto_community_reports=None,
            optimized_search=None,
            input_base_dir=None,
            output_base_dir=None,
            files=file_target
        )

        water_mark_target(targets=[target[1:]], queue_storage_client=queue_client, watermark_client=watermark_client)

    except Exception as e:
        logging.error(f"Error executing indexing: {e}")
        raise

    print("Indexing executed successfully")

@index_functions.function_name('csindexer')
@index_functions.timer_trigger(schedule="0 */1 * * * *", arg_name="mytimer", run_on_startup=True)
def csindexer(mytimer: func.TimerRequest) -> None:
    logging.info('Indexer timer function kicking off.')
    process_indexing()

@index_functions.function_name('triggerindexer')
@index_functions.route(route="index", auth_level=func.AuthLevel.FUNCTION)
def triggerindexer(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Indexer trigger function kicking off.')

    process_indexing()
    return func.HttpResponse("Indexing executed successfully", status_code=200)

def executing_correct_func_app(req: func.HttpRequest, route: str):
    return os.getenv("ENVIRONMENT") == "AZURE" and  os.getenv("APP_NAME")!= route

