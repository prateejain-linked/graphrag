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

@app.function_name('index')
@app.route(route="index", auth_level=func.AuthLevel.FUNCTION)    
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

