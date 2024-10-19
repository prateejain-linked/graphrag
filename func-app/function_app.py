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
app = func.FunctionApp()

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
        return func.HttpResponse(
        "Must send context id and context operation",
        status_code=200
        )

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
                response_type="",
                context_id=context_id,
                query=query,
                optimized_search=False,
                use_kusto_community_reports=False,
                path=int(path),
            )
    
    return func.HttpResponse(
        "\n[>] Query completed\n\n\n"+result,
        status_code=200
    )


@app.function_name('contextpollv2')
@app.route(route="contextpoll", auth_level=func.AuthLevel.FUNCTION)
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
        "Must send context id and context operation",
        status_code=200
        )

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
                response_type="",
                context_id=context_id,
                query=query,
                optimized_search=False,
                use_kusto_community_reports=False,
                path=int(path),
                save_result=True
            )
    
    return func.HttpResponse(
        "\n[>] Query completed\n\n\n"+result,
        status_code=200
    )

@app.function_name('summarization')
@app.route(route="summarize", auth_level=func.AuthLevel.ANONYMOUS)
def summarize_query(req: func.HttpRequest) -> func.HttpResponse:

    query_id = req.params['query_id']
    output = summarize(query_id=query_id, root_dir='.\\exe')
    return func.HttpResponse(
        output,
        status_code=200
    )


@app.function_name('rrf-app')
@app.route(route="rrf", auth_level=func.AuthLevel.ANONYMOUS)
def rrf(req: func.HttpRequest) -> func.HttpResponse:

    query_ids = req.params['query_ids']
    output = rrf_scoring(query_ids=query_ids,root_dir='.\\exe')
    return func.HttpResponse(
        str(output),
        status_code=200
    )

@app.function_name('index')
@app.route(route="index", auth_level=func.AuthLevel.ANONYMOUS)    
def context_switch(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    logging.info("Parameters: "+str(req.params))

    if 'req_type' not in req.params:
        return func.HttpResponse(
        "Must send context id and context operation",
        status_code=200
        )

    req_type: str = req.params['req_type']
    req_type = req_type.lower()
    context_name = req.params['context_name']
    if context_name is None or len(context_name) <= 0:
        return func.HttpResponse(
                f"The {req_type} request must be passed with context name",
                status_code=400
            )

    content_mgr = ContextManager(context_name=context_name)

    try:
        if req_type == 'create' or req_type == 'update':
            content_ids = req.params['content_ids']

            if content_ids is None is len(content_ids) <= 0:
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


def executing_correct_func_app(req: func.HttpRequest, route: str):
    return os.getenv("ENVIRONMENT") == "AZURE" and  os.getenv("APP_NAME")!= route

