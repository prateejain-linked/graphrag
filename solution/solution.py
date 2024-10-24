import json
from requests import post,get
from sys import argv

if len(argv)<2:
    print("Usage: solution.py query <query string> <path number> <context id>")
    print("Usage: solution.py rrf <query string> <context id>")

    exit(-1)
    

qs_url = '<query-save url here>'
summ_url = '<summarize url here>'
rrf_url = '<rrf url here>'   
    
solution = (argv[1])

def run_query(query,path,context_id):
    params={
        "context_id":context_id,
        "path":path,
        "query":query
        
    }
    print('Q','->',params)
    r=get(qs_url,params)
    json_response = r.text

    return json_response
    
    
if solution.lower() == "query": 
    # query-save 
    # summarize
    
    print("Query solution ...\n")
    
    query = argv[2]
    path=argv[3]
    context_id = argv[4]
    
    
    json_response=run_query(query,path,context_id)
    qid = json.loads(json_response)['query_id']
    print (f"[>] {qid}")
    
    
    params={
        "context_id":context_id,
        "query_id":qid 
    }
    r=get(summ_url,params)
   
    print(r.text)
    
elif solution.lower() == "rrf":
    # query save fpr paths 0 to 3
    # rrf
    # summarize 
    
    print("RRF solution ...\n")
    
    query = argv[2]
    context_id = argv[3]
    
    q_ids = []
    for i in range(4):
        json_response=run_query(query,str(i),context_id)
        qid = json.loads(json_response)['query_id']
        print (f"[>] {qid}")
        q_ids.append(qid)
    
    q_ids_str = ",".join(q_ids)
    
    print("[>] Executing RRF...")
    params={
        "query_ids":q_ids_str 
    }
    r=get(rrf_url,params)
    json_response = r.text

    qid_rrf = json.loads(json_response)['query_id']
    
    print("RRF Q:",qid_rrf)
    
    params={
        "query_id":qid_rrf 
    }
    r=get(summ_url,params)
    
    print("Summarization result:\n\n")
    print(r.text)
    
    
elif solution.lower()== "hyde":
    # hyde
    
    print("Not implemented")
    
    