import azure.functions as func
import datetime
import json
import logging
import csv
import codecs
from graphrag.index.cli import index_cli
import os 

import azure.functions as func
from context_switch_function import context_functions
from index import index_functions
from query_save import query_functions

app = func.FunctionApp()

function_type = os.environ.get("FUNCTIONTYPE", default="indexing")
if(function_type == "query"):
    app.register_functions(query_functions)

elif(function_type == "indexing"):
    app.register_functions(index_functions)

elif(function_type == "context"):
    app.register_functions(context_functions)

