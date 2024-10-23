# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""Command line interface for the query module."""

import asyncio
import os
from pathlib import Path
from typing import cast
from io import BytesIO

from datashaper import VerbCallbacks
from graphrag.common.progress.rich import RichProgressReporter
from graphrag.common.storage import PipelineStorage, BlobPipelineStorage, FilePipelineStorage
from graphrag.common.utils.context_utils import get_files_by_contextid
from graphrag.config.enums import StorageType
from azure.core.exceptions import ResourceNotFoundError

import pandas as pd

from graphrag.config import (
    create_graphrag_config,
    GraphRagConfig,
)


from graphrag.common.progress import PrintProgressReporter
from graphrag.index.verbs.entities.extraction.strategies.graph_intelligence.run_graph_intelligence import run_gi
from graphrag.index.verbs.entities.extraction.strategies.typing import Document
from graphrag.model.entity import Entity
from graphrag.query.input.loaders.dfs import (
    store_entity_semantic_embeddings,
)
from graphrag.vector_stores import VectorStoreFactory, VectorStoreType
from graphrag.vector_stores.base import BaseVectorStore
from graphrag.vector_stores.lancedb import LanceDBVectorStore
from graphrag.vector_stores.kusto import KustoVectorStore
from .factories import get_global_search_engine, get_local_search_engine,get_summarizer
from .indexer_adapters import (
    read_indexer_covariates,
    read_indexer_entities,
    read_indexer_relationships,
    read_indexer_reports,
    kt_read_indexer_reports,
    read_indexer_text_units,
)

from common.graph_db_client import GraphDBClient
import json
import ast
import uuid

reporter = PrintProgressReporter("")

reporter = PrintProgressReporter("")

def __get_embedding_description_store(
    entities: list[Entity] = [],
    vector_store_type: str = VectorStoreType.LanceDB,
    config_args: dict | None = None,
    context_id: str = "",
):
    """Get the embedding description store."""
    if not config_args:
        config_args = {}

    collection_name = config_args.get(
        "query_collection_name", "entity_description_embeddings"
    )
    config_args.update({"collection_name": f"{collection_name}_{context_id}" if context_id else collection_name})
    vector_name = config_args.get(
        "vector_search_column", "description_embedding"
    )
    config_args.update({"vector_name": vector_name})
    config_args.update({"reports_name": f"reports_{context_id}" if context_id else "reports"})
    config_args.update({"text_units_name": f"text_units_{context_id}"})
    config_args.update({"docs_tbl_name": f"docs_{context_id}"})

    description_embedding_store = VectorStoreFactory.get_vector_store(
        vector_store_type=vector_store_type, kwargs=config_args
    )

    description_embedding_store.connect(**config_args)

    if vector_store_type == VectorStoreType.Kusto:
        return description_embedding_store

    elif config_args.get("overwrite", True):
        # this step assumps the embeddings where originally stored in a file rather
        # than a vector database

        # dump embeddings from the entities list to the description_embedding_store
        store_entity_semantic_embeddings(
            entities=entities, vectorstore=description_embedding_store
        )
    else:
        # load description embeddings to an in-memory lancedb vectorstore
        # to connect to a remote db, specify url and port values.
        description_embedding_store = LanceDBVectorStore(
            collection_name=collection_name
        )
        description_embedding_store.connect(
            db_uri=config_args.get("db_uri", "./lancedb")
        )

        # load data from an existing table
        description_embedding_store.document_collection = (
            description_embedding_store.db_connection.open_table(
                description_embedding_store.collection_name
            )
        )

    return description_embedding_store


def run_global_search(
    config_dir: str | None,
    data_dir: str | None,
    root_dir: str | None,
    community_level: int,
    response_type: str,
    context_id: str,
    query: str,
):
    """Run a global search with the given query."""
    data_dir, root_dir, config = _configure_paths_and_settings(
        data_dir, root_dir, config_dir
    )
    if config.graphdb.enabled:
        graph_db_client = GraphDBClient(config.graphdb)
    data_path = Path(data_dir)

    final_nodes: pd.DataFrame = pd.read_parquet(
        data_path / "create_final_nodes.parquet"
    )
    if config.graphdb.enabled:
        final_entities = graph_db_client.query_vertices()
    else:
        final_entities: pd.DataFrame = pd.read_parquet(
            data_path / "create_final_entities.parquet"
        )
    final_community_reports: pd.DataFrame = pd.read_parquet(
        data_path / "create_final_community_reports.parquet"
    )

    reports = read_indexer_reports(
        final_community_reports, final_nodes, community_level
    )
    entities = read_indexer_entities(final_nodes, final_entities, community_level)
    search_engine = get_global_search_engine(
        config,
        reports=reports,
        entities=entities,
        response_type=response_type,
    )

    result = search_engine.search(query=query)

    reporter.success(f"Global Search Response: {result.response}")
    return result.response

def cs_search(
    config_dir: str | None,
    data_dir: str | None,
    root_dir: str | None,
    community_level: int,
    response_type: str,
    context_id: str,
    query: str,
    optimized_search: bool = False,
    use_kusto_community_reports: bool = False,
    path=0,
    save_result=False
    ):

    """Run a local search with the given query."""
    data_dir, root_dir, config = _configure_paths_and_settings(
        data_dir, root_dir, config_dir
    )

    vector_store_args = (
        config.embeddings.vector_store if config.embeddings.vector_store else {}
    )

    reporter.info(f"Vector Store Args: {vector_store_args}")
    vector_store_type = vector_store_args.get("type", VectorStoreType.LanceDB)

    entities=[]
    text_units=[]
    covariates=[]
    reports=[]
    final_relationships=[]


    ##### LEGACY #######################

    if vector_store_type == VectorStoreType.LanceDB:
        # for the POC purpose input artifacts blob, output artifacts blob and input query blob storage are going to same.
        if(config.storage.type == StorageType.memory):
            ValueError("Memory storage is not supported")
        if(config.storage.type == StorageType.blob):
            if(config.storage.container_name is not None):
                input_storage_client: PipelineStorage = BlobPipelineStorage(connection_string=config.storage.connection_string,
                                                                            container_name=config.storage.container_name,
                                                                            storage_account_blob_url=config.storage.storage_account_blob_url)
            else:
                ValueError("Storage type is Blob but container name is invalid")
        if(config.storage.type == StorageType.file):
            input_storage_client: PipelineStorage = FilePipelineStorage(config.root_dir)


        data_paths = []
        data_paths = get_files_by_contextid(config, context_id)
        final_nodes = pd.DataFrame()
        final_community_reports = pd.DataFrame()
        final_text_units = pd.DataFrame()
        final_relationships = pd.DataFrame()
        final_entities = pd.DataFrame()
        final_covariates = pd.DataFrame()

        for data_path in data_paths:
            #check from the config for the ouptut storage type and then read the data from the storage.

            #GraphDB: we may need to make change below to read nodes data from Graph DB
            final_nodes = pd.concat([final_nodes, read_paraquet_file(input_storage_client, data_path + "/create_final_nodes.parquet")])
            final_community_reports = pd.concat([final_community_reports,read_paraquet_file(input_storage_client, data_path + "/create_final_community_reports.parquet")]) # KustoDB: Final_entities, Final_Nodes, Final_report should be merged and inserted to kusto
            final_text_units = pd.concat([final_text_units, read_paraquet_file(input_storage_client, data_path + "/create_final_text_units.parquet")]) # lance db search need it for embedding mapping. we have embeddings in entities we should use from there. KustoDB already must have sorted it.
            final_relationships = pd.concat([final_relationships,read_paraquet_file(input_storage_client, data_path + "/create_final_relationships.parquet")])

            if not optimized_search:
                final_covariates = pd.concat([final_covariates, read_paraquet_file(input_storage_client, data_path + "/create_final_covariates.parquet")])

            final_entities = pd.concat([final_entities, read_paraquet_file(input_storage_client, data_path + "/create_final_entities.parquet")])

        ############# End of for loop

        entities = read_indexer_entities(final_nodes, final_entities, community_level) # KustoDB: read Final nodes data and entities data and merge it.
        reports=read_indexer_reports(
            final_community_reports, final_nodes, community_level
        )

        final_relationships=read_indexer_relationships(final_relationships)

        covariates = (
            read_indexer_covariates(final_covariates)
            if final_covariates.empty is False
            else []
        )
        text_units=read_indexer_text_units(final_text_units)


    ########################################################################################

    if use_kusto_community_reports:
        raise ValueError("Using community reports is not supported.")

    description_embedding_store = __get_embedding_description_store(
        entities=entities,
        vector_store_type=vector_store_type,
        config_args=vector_store_args,
        context_id=context_id,
    )

    '''
        *** If KUSTO is enabled, both entities and final_relationships must be empty.
    '''
    search_engine = get_local_search_engine(
        config=config,
        reports=reports,
        text_units=text_units,
        entities=entities,
        relationships=final_relationships,
        covariates={"claims": covariates},
        description_embedding_store=description_embedding_store,
        response_type=response_type,
        context_id=context_id,
        is_optimized_search=optimized_search,
        use_kusto_community_reports=use_kusto_community_reports,
    )

    
    if optimized_search:
        result = search_engine.optimized_search(query=query)
    else:
        result = search_engine.search(query=query,path=path)


    raw_result=query + "\n__RAW_RESULT__:\n"+ json.dumps(result.context_data['raw_result'])
    
    if save_result:
        query_id= uuid.uuid4()
        blob_storage_client: PipelineStorage = BlobPipelineStorage(connection_string=None,
                                                                container_name=config.output_storage.container_name,
                                                                storage_account_blob_url=config.output_storage.storage_account_blob_url)
        asyncio.run(blob_storage_client.set(
                            f"query/{query_id}/output.json",raw_result
                        ) 
                    ) 
        return str(query_id)
    
    return raw_result



def path1(
    config_dir: str | None,
    data_dir: str | None,
    root_dir: str | None,
    community_level: int,
    response_type: str,
    context_id: str,
    query: str,
    optimized_search: bool = False,
    use_kusto_community_reports: bool = False,
    ):
    ValueError("Not implemented")

def path2(
    config_dir: str | None,
    data_dir: str | None,
    root_dir: str | None,
    community_level: int,
    response_type: str,
    context_id: str,
    query: str,
    optimized_search: bool = False,
    use_kusto_community_reports: bool = False,
    ):
    """Path 2
    Find all the emails sent to trader by Tim Belden
    a. Query -> LLM -> Entity Extracted -> 5 entities -> Set A [TimBelden1]
    b. Query -> LLM -> Embeddings -> Y [x1..... Xn]
    c. Run the query on Kusto for embedding Y [x1.....xn] for entitYid in [TimBelden1]
    4. Get the text units and get the response"""
    data_dir, root_dir, config = _configure_paths_and_settings(
        data_dir, root_dir, config_dir
    )

    
    exit(0)

def path3(
    config_dir: str | None,
    data_dir: str | None,
    root_dir: str | None,
    community_level: int,
    response_type: str,
    context_id: str,
    query: str,
    optimized_search: bool = False,
    use_kusto_community_reports: bool = False,
    ):
    ValueError("Not implemented")


def run_local_search(
    config_dir: str | None,
    data_dir: str | None,
    root_dir: str | None,
    community_level: int,
    response_type: str,
    context_id: str,
    query: str,
    optimized_search: bool = False,
    use_kusto_community_reports: bool = False,
    path = 0,
    save_result=False):
    """Run a local search with the given query."""
    
    return cs_search(config_dir, data_dir, root_dir, community_level, response_type, context_id, 
                     query, optimized_search, use_kusto_community_reports, path=path,save_result=save_result)

def blob_exists(container_client, blob_name):
    blob_client = container_client.get_blob_client(blob_name)
    try:
        # Attempt to get the blob properties
        blob_client.get_blob_properties()
        return True
    except ResourceNotFoundError:
        # Blob does not exist
        return False


def read_paraquet_file(storage: PipelineStorage, path: str):
    #create different enum for paraquet storage type
    file_data = asyncio.run(storage.get(path, True))
    if file_data is None:
        return pd.DataFrame()
    return pd.read_parquet(BytesIO(file_data), engine="pyarrow")

def _configure_paths_and_settings(
    data_dir: str | None,
    root_dir: str | None,
    config_dir: str | None,
) -> tuple[str, str | None, GraphRagConfig]:
    if data_dir is None and root_dir is None:
        msg = "Either data_dir or root_dir must be provided."
        raise ValueError(msg)
    if data_dir is None:
        data_dir = _infer_data_dir(cast(str, root_dir))
    config = _create_graphrag_config(root_dir, config_dir)
    return data_dir, root_dir, config


def _infer_data_dir(root: str) -> str:
    output = Path(root) / "output"
    # use the latest data-run folder
    if output.exists():
        folders = sorted(output.iterdir(), key=os.path.getmtime, reverse=True)
        if len(folders) > 0:
            folder = folders[0]
            return str((folder / "artifacts").absolute())
    msg = f"Could not infer data directory from root={root}"
    raise ValueError(msg)


def _create_graphrag_config(
    root: str | None,
    config_dir: str | None,
) -> GraphRagConfig:
    """Create a GraphRag configuration."""
    return _read_config_parameters(root or "./", config_dir)


def _read_config_parameters(root: str, config: str | None):
    _root = Path(root)
    settings_yaml = (
        Path(config)
        if config and Path(config).suffix in [".yaml", ".yml"]
        else _root / "settings.yaml"
    )
    if not settings_yaml.exists():
        settings_yaml = _root / "settings.yml"

    if settings_yaml.exists():
        reporter.info(f"Reading settings from {settings_yaml}")
        with settings_yaml.open(
            "rb",
        ) as file:
            import yaml

            data = yaml.safe_load(file.read().decode(encoding="utf-8", errors="strict"))
            return create_graphrag_config(data, root)

    settings_json = (
        Path(config)
        if config and Path(config).suffix == ".json"
        else _root / "settings.json"
    )
    if settings_json.exists():
        reporter.info(f"Reading settings from {settings_json}")
        with settings_json.open("rb") as file:
            import json

            data = json.loads(file.read().decode(encoding="utf-8", errors="strict"))
            return create_graphrag_config(data, root)

    reporter.info("Reading settings from environment variables")
    return create_graphrag_config(root_dir=root)


def summarize(query_id:str,
              root_dir,
              response_type="multiple paragraphs",
              community_level=2)->str:
    data_dir, root_dir, config = _configure_paths_and_settings(
        '', root_dir, None
    )
    blob_storage_client: PipelineStorage = BlobPipelineStorage(connection_string=None,
                                                                container_name=config.output_storage.container_name,
                                                                storage_account_blob_url=config.output_storage.storage_account_blob_url)
    
    index_storage_client = BlobPipelineStorage(connection_string=None,
                                                                container_name=config.storage.container_name,
                                                                storage_account_blob_url=config.storage.storage_account_blob_url)


    blob_data = asyncio.run(blob_storage_client.get(f"query/{query_id}/output.json"))

    if type(blob_data)!= str:
        return "Invalid query result"
    
    query,raw_json = split_raw_response(blob_data)

    list_json=json.loads(raw_json)
    
    blob_history={
        'loaded_entities':set(),
        'loaded_relationships':set(),
        'loaded_text_units':set()
    }
    
    entities=[]
    relationships=[]
    text_units=[]


    for dict_json in list_json:
        #one entity per json row
        # Entity -> text unit list, relationship list , document list [one entry]

        entity_id = dict_json['entity_id']
        

        doc = dict_json["document_ids"] # Exactly ONE doc for each list of text units
        if len(doc) != 1: 
            return "Invalid query file. Document ID configuration not supported"
        doc=doc[0]

        #list_relationships=dict_json.get("relationships",[])
        list_text_units=dict_json["text_unit_ids"]

        #LOAD everything we need from this document
        artifacts_path = f'artifacts/{doc}/version=0'

        if entity_id not in blob_history['loaded_entities']:
            blob_history['loaded_entities'].add(entity_id)
            
            _nodes = read_paraquet_file(index_storage_client, f"{artifacts_path}/create_final_nodes.parquet")
            _entities_df = read_paraquet_file(index_storage_client, f"{artifacts_path}/create_final_entities.parquet")
            _entities= _entities_df[_entities_df['id'].isin([entity_id])] #isolate the target
            entities += read_indexer_entities(_nodes, _entities, community_level) #append
        
        #########################################################

        # We do not have relationship text units
        # relationships not supported at summarization stage
        '''
        artifacts_path='unknown'
        rels_to_load=[]
        for r in list_relationships:
            if r['id'] not in blob_history['loaded_relationships']:
                blob_history['loaded_relationships'].add(r['id'])
                rels_to_load.append(r['id'])

        if rels_to_load != []:
            _relationships_df = read_paraquet_file(index_storage_client, f"{artifacts_path}/create_final_relationships.parquet")
            _relationships = _relationships_df[_relationships_df['id'].isin(rels_to_load)]
            relationships += read_indexer_relationships(_relationships)
        '''
        ##########################################################

        units_to_load = []
        for u in list_text_units:
            if u not in blob_history["loaded_text_units"]:
                blob_history["loaded_text_units"].add(u)
                units_to_load.append(u)
        
        if units_to_load != []:
            _text_units_df = read_paraquet_file(index_storage_client, f"{artifacts_path}/create_final_text_units.parquet")
            _text_units = _text_units_df[_text_units_df['id'].isin(units_to_load)]
            if len(_text_units)!=len(units_to_load):
                return "Invlaid parquet file"
            text_units += read_indexer_text_units(_text_units)
    
    summarizer = get_summarizer(
        config=config,
        response_type=response_type,
        external_entities=entities,
        external_relationships=relationships,
        external_text_units=text_units
    )
    result = summarizer.summarize(query)
    return result.response

def split_raw_response(data):
    delimiter="\n__RAW_RESULT__:\n"
    pdelim=data.find(delimiter)
    if pdelim==-1:
        print( "Invalid query result")
        exit(-1)

    query=data[:pdelim]

    raw_json=data[pdelim+len(delimiter):]

    return query,raw_json

def rrf_scoring(query_ids:str,root_dir:str,k=60,top_k=20):

    _, root_dir, config = _configure_paths_and_settings(
        '', root_dir, None
    )
    rrf_scores = {}
    docs={}
    blob_storage_client = BlobPipelineStorage(connection_string=None,
                                                                container_name=config.output_storage.container_name,
                                                                storage_account_blob_url=config.output_storage.storage_account_blob_url)
    
    query_ids=query_ids.split(',')
    print('RRF over',query_ids)

    query='' # we expect the query to be the same for given raw reponses
    for query_id in query_ids:
        blob_data = asyncio.run(blob_storage_client.get(f"query/{query_id}/output.json"))
        query,raw_json=split_raw_response(blob_data) # query should stay the same
        list_json=json.loads(raw_json)
        for entity_json in list_json:
            for text_unit_id in entity_json["text_unit_ids"]:
                entity_text_unit = f"{entity_json['entity_id']}:{text_unit_id}"
                doc_id = entity_json['document_ids']
                docs[entity_text_unit] = doc_id
                if entity_text_unit not in rrf_scores:
                    rrf_scores[entity_text_unit] = 0
                rrf_scores[entity_text_unit]+=1.0/(k+entity_json["rank"]) #assuming rank is stored as an integer


    result=[]
    for couple in rrf_scores:
        d=couple.find(":")
        entity_id = couple[:d]
        text_unit_id=couple[d+1:]
        result.append({'entity_id':entity_id,
                       'text_unit_ids':[text_unit_id],
                       'rank':rrf_scores[couple],
                       'document_ids': docs[couple] }
                      )
        


    result.sort(key=lambda x : x['rank'],reverse=True)
    result=result[:top_k]

    result= query + "\n__RAW_RESULT__:\n"+ json.dumps(result)

    new_query_id= uuid.uuid4()
    
    asyncio.run(blob_storage_client.set(
                            f"query/{new_query_id}/output.json",result
                        ) 
    ) 

    return str(new_query_id)