import azure.functions as func
import datetime
import json
import logging

from graphrag.index.cli import index_cli

app = func.FunctionApp()

@app.route(route="queryHttpTrigger", auth_level=func.AuthLevel.FUNCTION)
def queryHttpTrigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    index_cli(
        root="./..",
        verbose=False,
        resume=None,
        memprofile=False,
        nocache=False,
        reporter="none",
        config=None,
        emit=None,
        dryrun=False,
        init=False,
        overlay_defaults=False,
        cli=True,
        context_id="appcontext",
        context_operation="activate",
        community_level=2,
        use_kusto_community_reports=False,
        optimized_search=False
    )

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )