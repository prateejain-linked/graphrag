import azure.functions as func
import logging
import os
import sys
from graphrag.index.cli import index_cli
from graphrag.query.cli import run_local_search

app = func.FunctionApp()
# Create a handler that writes log messages to stdout
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
logging.getLogger().addHandler(handler)

@app.function_name('csindexer')
@app.route(route="index", auth_level=func.AuthLevel.FUNCTION)
def indexing(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    if executing_correct_func_app(req, "csindexer"):
        return func.HttpResponse(
        "Please trigger csindexer Azure function for indexing",
        status_code=200
        )
    
    input_base_dir=None
    if "input_base_dir" in req.params:
        input_base_dir = req.params['input_base_dir']
    
    output_base_dir=None
    if "output_base_dir" in req.params:
        output_base_dir = req.params['output_base_dir']
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
        context_id=None,
        context_operation=None,
        community_level=2,
        use_kusto_community_reports=None,
        optimized_search=None,
        input_base_dir=input_base_dir,
        output_base_dir=output_base_dir
    )
    return func.HttpResponse(
        "Wow this first HTTP Function works!!!!",
        status_code=200
    )

@app.function_name('QyeryPipeline')
@app.route(route="index", auth_level=func.AuthLevel.ANONYMOUS)
def indexing(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    index_cli(
        root = "",
        verbose=False,
        resume=False,
        memprofile=False,
        nocache=False,
        config=None,
        emit=None,
        dryrun=False,
        init=True,
        overlay_defaults=False,
        cli=True,
        context_id=None,
        context_operation=None,
        community_level=None,
        use_kusto_community_reports=None,
        optimized_search=None
    )
    return func.HttpResponse(
        "Wow this first HTTP Function works!!!!",
        status_code=200
    )
