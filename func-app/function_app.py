import azure.functions as func
import logging
import os
import sys
from graphrag.index.cli import index_cli
from graphrag.query.cli import run_local_search

from graphrag.common.storage.queue_storage import QueueStorageClient
from graphrag.common.storage.blob_pipeline_storage import BlobPipelineStorage

from utility import find_next_target_blob
from utility import water_mark_target

app = func.FunctionApp()
# Create a handler that writes log messages to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(handler)


def initialize_incoming_msg_queue() -> QueueStorageClient:
    # queue_url = 'https://inputdatasetsa.queue.core.windows.net' 
    # queue_name='inputdataetqu'
    # client_id = '500051c4-c242-4018-9ae4-fb983cfebefd'
    
    max_messages = int(os.environ.get("MAX_QUEUE_MESSAGE_COUNT", default="1"))
    queue_url = os.environ.get("AZURE_QUEUE_URL")
    queue_name = os.environ.get("AZURE_QUEUE_NAME")
    client_id = os.environ.get("AZURE_CLIENT_ID")

    queue_storage_client = QueueStorageClient(account_url=queue_url, queue_name=queue_name, client_id=client_id, max_message=max_messages)

    return queue_storage_client

def initialize_watermark_client() -> BlobPipelineStorage:
    # blob_account_url = 'https://inputdatasetsa.blob.core.windows.net'
    # watermark_container_name='watermark'

    blob_account_url = os.environ.get("AZURE_WATERMARK_ACCOUNT_URL")
    watermark_container_name = os.environ.get("AZURE_WATERMARK_ACCOUNT_URL")
    client_id = os.environ.get("AZURE_CLIENT_ID")

    watermark_storage_account = BlobPipelineStorage(connection_string=None, container_name=watermark_container_name, storage_account_blob_url=blob_account_url)

    return watermark_storage_account

@app.function_name('cscontextwriter')
@app.timer_trigger(schedule="0 */3 * * * *", arg_name="mytimer", run_on_startup=True) 
def context_write(mytimer: func.TimerRequest) -> None:

    queue_client = initialize_incoming_msg_queue()
    watermark_client = initialize_watermark_client()

    targets = find_next_target_blob(queue_storage_client=queue_client, watermark_client=watermark_client)

    files_to_activate=[]
    for target in targets:
        artifact_folder=target[0][:target[0].rfind('/')]
        if artifact_folder not in files_to_activate:
            files_to_activate.append(artifact_folder)

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
        context_id="test_context3",
        context_operation="activate",
        community_level=2,
        use_kusto_community_reports=False,
        optimized_search=False,
        files=files_to_activate
    )
    water_mark_target(targets=targets, queue_storage_client=queue_client, watermark_client=watermark_client)


def executing_correct_func_app(req: func.HttpRequest, route: str):
    return os.getenv("ENVIRONMENT") == "AZURE" and  os.getenv("APP_NAME")!= route
            
