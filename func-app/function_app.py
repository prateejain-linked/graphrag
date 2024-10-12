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


def executing_correct_func_app(req: func.HttpRequest, route: str):
    return os.getenv("ENVIRONMENT") == "AZURE" and  os.getenv("APP_NAME")!= route
            
