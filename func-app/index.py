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
from query_save import query

index_functions = func.Blueprint()

@index_functions.function_name('index')
@index_functions.route(route="index", auth_level=func.AuthLevel.FUNCTION)    
def context_switch(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    logging.info("Parameters: "+str(req.params))
    
    if 'context_id' in req.params:
        context_id=req.params['context_id']
        context_command=req.params.get('context_operation',None)
        logging.info(f"Got context command: {context_id} {context_command}")
    else:
        return func.HttpResponse(
        "Must send context id and context operation",
        status_code=200
        )
    
    if context_command==None: #index
        batch_stat_f="batch_index.txt"
        if not os.path.exists(batch_stat_f):
            f=open(batch_stat_f,"w")
            f.close()
        
        f=open(batch_stat_f,"r+")
        f.seek(0,0)
        c=f.read()
        if len(c)>0:
            batch_index=int(c)
        else:
            batch_index=0
        f.close()


    index_cli(
        root = "exe",
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
        context_id=context_id,
        context_operation=context_command,
        community_level=2,
        use_kusto_community_reports=False,
        optimized_search=False
    )

    if context_command==None:
        logging.info("Indexer completed for batch "+str(batch_index))

    return func.HttpResponse(
        "Index: Command completed",
        status_code=200
    )



'''
@app.function_name('IndexingPipelineFunc')
@app.schedule(schedule="* */30 * * * *", arg_name="mytimer", run_on_startup=True, use_monitor=True)
def index(mytimer: func.TimerRequest) -> None:
    logging.info("TIMER trigger started")
    batch_stat_f="batch_index.txt"
    if not os.path.exists(batch_stat_f):
        f=open(batch_stat_f,"w")
        f.close()
    
    f=open(batch_stat_f,"r+")
    f.seek(0,0)
    c=f.read()
    if len(c)>0:
        batch_index=int(c)
    else:
        batch_index=0
    f.close()

   
    index_cli(
        root = "exe",
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
        use_kusto_community_reports=False,
        optimized_search=False
    )
 
    logging.info("Indexer completed for batch "+str(batch_index))

'''
