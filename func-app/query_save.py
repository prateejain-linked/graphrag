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
        status_code=200
    )

@query_functions.function_name('summarization')
@query_functions.route(route="summarize", auth_level=func.AuthLevel.FUNCTION)
def summarize_query(req: func.HttpRequest) -> func.HttpResponse:

    query_id = req.params['query_id']
    output = summarize(query_id=query_id, root_dir='settings')
    return func.HttpResponse(
        output,
        status_code=200
    )


@query_functions.function_name('rrf_app')
@query_functions.route(route="rrf", auth_level=func.AuthLevel.FUNCTION)
def rrf(req: func.HttpRequest) -> func.HttpResponse:

    query_ids = req.params['query_ids']
    output = rrf_scoring(query_ids=query_ids,root_dir='settings')
    json_res={'query_id':output}
    return func.HttpResponse(
        json.dumps(json_res),
        status_code=200
    )
