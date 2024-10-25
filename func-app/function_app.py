import azure.functions as func
import datetime
import json
import logging
import csv
import codecs
from graphrag.index.cli import index_cli
import os 

from graphrag.query.cli import run_local_search, summarize,rrf_scoring
from time import sleep
from query_save import query_functions
from index import index_functions
app = func.FunctionApp()
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
    # blob_account_url = 'https://inputdatasetsa.blob.core.windows.net'
    # watermark_container_name='watermark'

    blob_account_url = os.environ.get("AZURE_WATERMARK_ACCOUNT_URL")
    watermark_container_name = os.environ.get("WATERMARK_CONTAINER_NAME")
    client_id = os.environ.get("AZURE_CLIENT_ID")

    watermark_storage_account = BlobPipelineStorage(connection_string=None, container_name=watermark_container_name, storage_account_blob_url=blob_account_url)

    return watermark_storage_account


@app.function_name('contextpoller')
@app.timer_trigger(schedule="0 */1 * * * *", arg_name="mytimer", run_on_startup=True)
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

app = func.FunctionApp()

function_type = os.environ.get("FUNCTIONTYPE", default="indexing")
if(function_type == "query"):
    app.register_functions(query_functions)

if(function_type == "indexing"):
    app.register_functions(index_functions)


