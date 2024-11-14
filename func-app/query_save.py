import azure.functions as func
import datetime
import json
import logging
import csv
import codecs
from graphrag.index.cli import index_cli
import os 

from graphrag.query.cli import run_local_search, summarize,rrf_scoring,expand_node_graph,generate_graph
from time import sleep

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

query_functions = func.Blueprint()

@query_functions.function_name('query')
@query_functions.route(route="query", auth_level=func.AuthLevel.FUNCTION)
def query(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Query.')
    logging.info("Parameters: "+str(req.params))
    
    if 'context_id' in req.params:
        context_id=req.params['context_id']
        query=req.params['query']
        path=req.params['path']
    else:
        return func.HttpResponse(
        "Must send context id and context operation",
        status_code=200
        )
    logging.info("Query start")
    result=run_local_search(
                None,
                data_dir="",
                root_dir="settings",
                community_level=2,
                response_type="",
                context_id=context_id,
                query=query,
                use_kusto_community_reports=False,
                path=int(path),
            )
    
    return func.HttpResponse(
        "\n[>] Query completed\n\n\n"+result,
        status_code=200
    )


@query_functions.function_name('query-save')
@query_functions.route(route="query-save", auth_level=func.AuthLevel.FUNCTION)
def query_save(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Query and save.')
    logging.info("Parameters: "+str(req.params))
    
    if 'context_id' in req.params:
        context_id=req.params['context_id']
        query=req.params['query']
        path=req.params['path']
    else:
        return func.HttpResponse(
        "Must send context id and context operation",
        status_code=200
        )
    logging.info("Query start")
    result=run_local_search(
                None,
                data_dir='',
                root_dir='settings',
                community_level=2,
                response_type="",
                context_id=context_id,
                query=query,
                use_kusto_community_reports=False,
                path=int(path),
                save_result=True
            )
    
    json_res={'query_id':result}

    return func.HttpResponse(
        json.dumps(json_res),
        status_code=200,
        headers={'Access-Control-Allow-Origin':"*",'Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'Content-Type, Authorization'}
    )

@query_functions.function_name('summarization')
@query_functions.route(route="summarize", auth_level=func.AuthLevel.FUNCTION)
def summarize_query(req: func.HttpRequest) -> func.HttpResponse:

    query_id = req.params['query_id']
    output = summarize(query_id=query_id, root_dir='settings')
    return func.HttpResponse(
        json.dumps(output),
        status_code=200,
        headers={'Access-Control-Allow-Origin':"*",'Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'Content-Type, Authorization'}
    )


@query_functions.function_name('rrf_app')
@query_functions.route(route="rrf", auth_level=func.AuthLevel.FUNCTION)
def rrf(req: func.HttpRequest) -> func.HttpResponse:

    query_ids = req.params['query_ids']
    output = rrf_scoring(query_ids=query_ids,root_dir='settings')
    json_res={'query_id':output}
    return func.HttpResponse(
        json.dumps(json_res),
        status_code=200,
        headers={'Access-Control-Allow-Origin':"*",'Access-Control-Allow-Methods':'POST','Access-Control-Allow-Headers':'Content-Type, Authorization'}
    )


@query_functions.function_name('generate_graph')
@query_functions.route(route="generate_graph", auth_level=func.AuthLevel.FUNCTION)
def generate_graphml(req: func.HttpRequest) -> func.HttpResponse:

    context_id = req.params['context_id']
    query = req.params['query']
    output = generate_graph(context_id,query)
    return func.HttpResponse(
        str(output),
        status_code=200
    )

@query_functions.function_name('query_expansion')
@query_functions.route(route="query_expansion", auth_level=func.AuthLevel.FUNCTION)
def query_expansion(req: func.HttpRequest) -> func.HttpResponse:

    node = req.params['node']
    context_id = req.params['context_id']
    query = req.params['query']
    depth = int(req.params['depth'])
    excluding_edges_ids = req.params['excluding_edges_ids']
    output = expand_node_graph(node,context_id,query,depth,excluding_edges_ids=excluding_edges_ids)
    return func.HttpResponse(
        str(output),
        status_code=200
    )


@query_functions.route(route="contexts")
def http_context(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Azure Blob Storage connection details
    # connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    storage_account_blob_url = "https://inputdatasetsa.blob.core.windows.net"
    container_name = "context"
    blob_name = req.params.get('blob_name')

    try:
        # Create the BlobServiceClient object
        if 'AZURE_CLIENT_ID' in os.environ:
            print("AZURE_CLIENT_ID: ", os.environ['AZURE_CLIENT_ID'])

        # credential = DefaultAzureCredential()
        credential = DefaultAzureCredential(managed_identity_client_id="500051c4-c242-4018-9ae4-fb983cfebefd", exclude_interactive_browser_credential = False)
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
            print("FAILED HERE0!!!!!")
            blobs = container_client.list_blobs()
            # Convert the blobs to a list
            print("FAILED HERE1!!!!!")
            print("BLOBS"+str(blobs))
            blob_test = [blob for blob in blobs]
            print("BLOB TEST"+str(blob_test))
            blob_data = [blob.name for blob in blobs]
            # Filter to just get the files which follow the pattern *_init.json
            print("FAILED HERE2!!!!!")
            blob_data = [blob for blob in blob_data if blob.endswith("_init.json")]
            # Read each blob content and return a list of all the jsons
            print("FAILED HERE3!!!!!")
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