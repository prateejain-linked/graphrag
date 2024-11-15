import azure.functions as func
import datetime
import json
import logging
import csv
import codecs
from graphrag.index.cli import index_cli
import os

from graphrag.query.cli import run_local_search, summarize,rrf_scoring,expand_node_graph,generate_graph, alert_search
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
                override=req.params['cube_id'] if 'cube_id' in req.params else None
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
                save_result=True,
                override=req.params['cube_id'] if 'cube_id' in req.params else None
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
    output = summarize(query_id=query_id, root_dir='settings', override=req.params['cube_id'] if 'cube_id' in req.params else None)
    return func.HttpResponse(
        json.dumps(output),
        status_code=200
    )


@query_functions.function_name('rrf_app')
@query_functions.route(route="rrf", auth_level=func.AuthLevel.FUNCTION)
def rrf(req: func.HttpRequest) -> func.HttpResponse:

    query_ids = req.params['query_ids']
    output = rrf_scoring(query_ids=query_ids,root_dir='settings',
                override=req.params['cube_id'] if 'cube_id' in req.params else None)
    json_res={'query_id':output}
    return func.HttpResponse(
        json.dumps(json_res),
        status_code=200
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

@query_functions.function_name('alerts')
@query_functions.route(route="alerts", auth_level=func.AuthLevel.FUNCTION)
def alerts(req: func.HttpRequest) -> func.HttpResponse:

    context_id = req.params['context_id']
    query = req.params['query']
    expand=req.params.get('target',None)
    with_keywords=False

    if not expand:
        wk = req.params.get('kw',None)
        if wk:
            with_keywords=True
    
    output = alert_search(root_dir = "settings",
                            context_id=context_id,
                            query=query,
                            with_keywords=with_keywords,
                            expand=expand)
    return func.HttpResponse(
        str(output),
        status_code=200
    )