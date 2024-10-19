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
from query_save import query_functions
from index import index_functions

app = func.FunctionApp()

function_type = os.environ.get("FUNCTIONTYPE", default="indexing")
if(function_type == "query"):
    app.register_functions(query_functions)

if(function_type == "indexing"):
    app.register_functions(index_functions)


