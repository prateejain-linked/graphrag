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

@app.function_name('query')
@app.route(route="query", auth_level=func.AuthLevel.FUNCTION)
def query(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    logging.info("Parameters: "+str(req.params))
    
    if 'context_id' in req.params:
        context_id=req.params['context_id']
        query=req.params['query']
        path=req.params['paths']
    else:
        return func.HttpResponse(
        "Must send context id and context operation",
        status_code=200
        )
    logging.info("Query start")
    result=run_local_search(
                None,
                None,
                '.\\exe',
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


@app.function_name('query&save')
@app.route(route="query_save", auth_level=func.AuthLevel.FUNCTION)
def query(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    logging.info("Parameters: "+str(req.params))
    
    if 'context_id' in req.params:
        context_id=req.params['context_id']
        query=req.params['query']
        path=req.params['paths']
    else:
        return func.HttpResponse(
        "Must send context id and context operation",
        status_code=200
        )
    logging.info("Query start")
    result=run_local_search(
                None,
                None,
                '.\\exe',
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
@app.route(route="summarize", auth_level=func.AuthLevel.FUNCTION)
def summarize_query(req: func.HttpRequest) -> func.HttpResponse:

    query_id = req.params['query_id']
    output = summarize(query_id=query_id, root_dir='.\\exe')
    return func.HttpResponse(
        output,
        status_code=200
    )


@app.function_name('rrf_app')
@app.route(route="rrf", auth_level=func.AuthLevel.FUNCTION)
def rrf(req: func.HttpRequest) -> func.HttpResponse:

    query_ids = req.params['query_ids']
    output = rrf_scoring(query_ids=query_ids,root_dir='.\\exe')
    return func.HttpResponse(
        str(output),
        status_code=200
    )
