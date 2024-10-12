import azure.functions as func
import logging
import os
import sys
from graphrag.index.cli import index_cli
from graphrag.query.cli import run_local_search, summarize

app = func.FunctionApp()
# Create a handler that writes log messages to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(handler)

@app.function_name('summarization')
@app.route(route="summarize", auth_level=func.AuthLevel.ANONYMOUS)
def summarize_query(req: func.HttpRequest) -> func.HttpResponse:
    query_id = req.params['query']
    artifacts_path = req.params['path']
    output = summarize(query_id,artifacts_path)
    return func.HttpResponse(
        output,
        status_code=200
    )

@app.function_name('csquery')
@app.route(route="query", auth_level=func.AuthLevel.FUNCTION)
def query(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    logging.info("Parameters1: "+str(req.params))
    if executing_correct_func_app(req, "csquery"):
        return func.HttpResponse(
        "Please trigger csquery Azure function for query",
        status_code=200
        )
    if 'context_id' in req.params:
        context_id=req.params['context_id']
        query=req.params['query']
        path=int(req.params['path'])
    else:
        return func.HttpResponse(
        "Must send context id and context operation",
        status_code=200
        )
    logging.info("Query start")
    output = run_local_search(
                None,
                None,
                "settings",
                community_level=2,
                response_type="",
                context_id=context_id,
                query=query,
                optimized_search=False,
                use_kusto_community_reports=False,
                path=path
            )

    return func.HttpResponse(
        "Query completed: "+ output,
        status_code=200
    )

def executing_correct_func_app(req: func.HttpRequest, route: str):
    return os.getenv("ENVIRONMENT") == "AZURE" and  os.getenv("APP_NAME")!= route
            
