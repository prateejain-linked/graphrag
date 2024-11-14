# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""The Azure Kusto vector storage implementation package."""
import os
import typing
import ast
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.helpers import dataframe_from_result_table
from graphrag.model.community_report import CommunityReport
from graphrag.model.entity import Entity
from graphrag.model.types import TextEmbedder
from graphrag.model import TextUnit,Relationship
import logging
import numpy as np
import pandas as pd
from pathlib import Path

import json
from typing import Any, List, cast

from graphrag.query.input.loaders.utils import (
    to_list,
    to_optional_dict,
    to_optional_float,
    to_optional_int,
    to_optional_list,
    to_optional_str,
    to_str,
)

from .base import (
    BaseVectorStore,
    VectorStoreDocument,
    VectorStoreSearchResult,
)


class KustoVectorStore(BaseVectorStore):
    """The Azure Kusto vector storage implementation."""

    def connect(self, **kwargs: Any) -> Any:
        """
        Connect to the vector storage.

        Args:
            **kwargs: Arbitrary keyword arguments containing connection parameters.
                - cluster (str): The Kusto cluster URL.
                - database (str): The Kusto database name.
                - client_id (str): The client ID for AAD authentication.
                - client_secret (str): The client secret for AAD authentication.
                - authority_id (str): The authority ID (tenant ID) for AAD authentication.

        Returns:
            Any: The Kusto client instance.
        """
        cluster = kwargs.get("cluster")
        database = kwargs.get("database")
        client_id = kwargs.get("client_id")
        client_secret = kwargs.get("client_secret")
        authority_id = kwargs.get("authority_id")
        env = os.environ.get("ENVIRONMENT")
        if(env == "AZURE"):
            kcsb = KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(
                str(cluster), client_id=os.environ.get("AZURE_CLIENT_ID") )
        elif(env == "DEVELOPMENT"):
            #kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(str(cluster))
            logging.info("KUSTO DEVELPMENT MODE")
            #kcsb = KustoConnectionStringBuilder.with_interactive_login(str(cluster))
            kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
        else:
            kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(
            str(cluster), str(client_id), str(client_secret), str(authority_id))

        self.client = KustoClient(kcsb)
        self.database = database

    def load_documents(
        self, documents: List[VectorStoreDocument], overwrite: bool = True
    ) -> None:
        """
        Load documents into vector storage.

        Args:
            documents (List[VectorStoreDocument]): List of documents to be loaded.
            overwrite (bool): Whether to overwrite the existing table. Defaults to True.
        """
        data = [
            {
                "id": document.id,
                "name": document.text,
                "vector": document.vector,
                "attributes": json.dumps(document.attributes),
            }
            for document in documents
            if document.vector is not None
        ]

        if len(data) == 0:
            return

        # Convert data to DataFrame
        df = pd.DataFrame(data)

        # Create or replace table
        if overwrite:
            command = f".drop table {self.collection_name} ifexists"
            self.client.execute(self.database, command)
            command = f".create table {self.collection_name} (id: string, text: string, vector: dynamic, attributes: string)"
            self.client.execute(self.database, command)

        # Ingest data
        ingestion_command = f".ingest inline into table {self.collection_name} <| {df.to_csv(index=False, header=False)}"
        self.client.execute(self.database, ingestion_command)

    def filter_by_id(self, include_ids: List[str] | List[int]) -> Any:
        """
        Build a query filter to filter documents by id.

        Args:
            include_ids (List[str] | List[int]): List of document IDs to include in the filter.

        Returns:
            Any: The query filter string.
        """
        if len(include_ids) == 0:
            self.query_filter = None
        else:
            if isinstance(include_ids[0], str):
                id_filter = ", ".join([f"'{id}'" for id in include_ids])
                self.query_filter = f"id in ({id_filter})"
            else:
                self.query_filter = (
                    f"id in ({', '.join([str(id) for id in include_ids])})"
                )
        return self.query_filter

    def similarity_search_by_vector(
        self, query_embedding: List[float], k: int = 10, **kwargs: Any
    ) -> List[VectorStoreSearchResult]:
        """
        Perform a vector-based similarity search. A search to find the k nearest neighbors of the given query vector.

        Args:
            query_embedding (List[float]): The query embedding vector.
            k (int): The number of top results to return. Defaults to 10.
            **kwargs: Additional keyword arguments.

        Returns:
            List[VectorStoreSearchResult]: List of search results.
        """
        query = f"""
        let query_vector = dynamic({query_embedding});
        {self.collection_name}
        | extend similarity = series_cosine_similarity(query_vector, {self.vector_name})
        | top {k} by similarity desc
        """
        response = self.client.execute(self.database, query)
        df = dataframe_from_result_table(response.primary_results[0])
        print("Similarities of the search results:", [row["similarity"] for _, row in df.iterrows()])

        # Temporary to support the original entity_description_embedding
        return [
            VectorStoreSearchResult(
                document=VectorStoreDocument(
                    id=row["id"],
                    text=row["text"],
                    vector=row[self.vector_name],
                    attributes=row["attributes"],
                ),
                score= 1 + float(row["similarity"]), # 1 + similarity to make it a score between 0 and 2
            )
            for _, row in df.iterrows()
        ]

    def similarity_search_by_text(
        self, text: str, text_embedder: TextEmbedder, k: int = 10, **kwargs: Any
    ) -> list[VectorStoreSearchResult]:
        """
        Perform a similarity search using a given input text.

        Args:
            text (str): The input text to search for.
            text_embedder (TextEmbedder): The text embedder to convert text to vector.
            k (int): The number of top results to return. Defaults to 10.
            **kwargs: Additional keyword arguments.

        Returns:
            List[VectorStoreSearchResult]: List of search results.
        """
        query_embedding = text_embedder(text)
        if query_embedding:
            return self.similarity_search_by_vector(query_embedding, k)
        return []

    def get_extracted_entities(self, text: str, text_embedder: TextEmbedder, k: int = 10,
                               preselected_entities=[],
                               **kwargs: Any
    ) -> list[Entity]:

        query_embedding = text_embedder(text)

        if preselected_entities==[]:
            query = f"""
            let query_vector = dynamic({query_embedding});
            {self.collection_name}
            | extend similarity = series_cosine_similarity(query_vector, {self.vector_name})
            | top {k} by similarity desc
            """
        else:

            chosen_ids=", ".join(f"'{id}'" for id in preselected_entities )
            query = f"""
            let query_vector = dynamic({query_embedding});
            {self.collection_name}
            | where id in ({chosen_ids})
            | extend similarity = series_cosine_similarity(query_vector, {self.vector_name})
            | top {k} by similarity desc
            """


        response = self.client.execute(self.database, query)
        df = dataframe_from_result_table(response.primary_results[0])
        pt_enabled = os.environ.get("PROTOTYPE")

        return [
            Entity(
                id=row["id"],
                title=row["title"] if not pt_enabled else '',
                type=row["type"] if not pt_enabled else '',
                description=row["description"] if not pt_enabled else '',
                graph_embedding=row["graph_embedding"] if not pt_enabled else '',
                text_unit_ids=row["text_unit_ids"],
                description_embedding=row["description_embedding"],
                short_id="",
                community_ids=row["community_ids"] if not pt_enabled else '[]',
                document_ids=row["document_ids"] if not pt_enabled else '[]',
                rank=row["rank"],
                attributes=row["attributes"] if not pt_enabled else '',
                #score= 1 + float(row["similarity"]), #score not in Entity currently
            ) for _, row in df.iterrows()
        ]

    def unload_entities(self) -> None:
        self.client.execute(self.database,f".drop table {self.collection_name} ifexists")
        self.client.execute(self.database,f".drop table {self.text_units_name} ifexists")
        self.client.execute(self.database,f".drop table {self.reports_name} ifexists")

    def setup_entities(self) -> None:
        if self._check_if_table_exists(self.collection_name):
            return
        command = f".drop table {self.collection_name} ifexists	"
        self.client.execute(self.database, command)

        pt_enabled = os.environ.get("PROTOTYPE")

        if not pt_enabled:
            entity_table_schema = (f".create table {self.collection_name} (id: string, short_id: real, title: string, type: "
                                "string, description: string, description_embedding: dynamic, name_embedding: dynamic, "
                                "graph_embedding: dynamic, community_ids: dynamic, text_unit_ids: dynamic, document_ids: "
                                "dynamic, rank: real, attributes: dynamic)")
        else:
            #remove unwanted data (PROTOTYPE)
            entity_table_schema = (f".create table {self.collection_name} (id: string, short_id: real, "
                                " description_embedding: dynamic,"
                                " text_unit_ids: dynamic,"
                                " rank: real)")


        command = entity_table_schema
        self.client.execute(self.database, command)

        if not pt_enabled:
            command = f".alter column {self.collection_name}.graph_embedding policy encoding type = 'Vector16'"
            self.client.execute(self.database, command)

        command = f".alter column {self.collection_name}.description_embedding policy encoding type = 'Vector16'"
        self.client.execute(self.database, command)

    def load_entities(self, entities: list[Entity], overwrite: bool = False) -> None:
        # Convert data to DataFrame
        df = pd.DataFrame(entities)



        pt_enabled = os.environ.get("PROTOTYPE")

        if pt_enabled:
            #remove unwanted data (prototype)
            df.drop("title",axis=1,inplace=True)
            df.drop("description",axis=1,inplace=True)
            df.drop("type",axis=1,inplace=True)
            df.drop("name_embedding",axis=1,inplace=True)
            df.drop("graph_embedding",axis=1,inplace=True)
            df.drop("community_ids",axis=1,inplace=True)
            df.drop("document_ids",axis=1,inplace=True)
            df.drop("attributes",axis=1,inplace=True)

        #df['test_e'] = df['description_embedding'].apply(lambda x: np.ceil( int (np.array(x) * 10**9)) / 10**9)
        #dec_len=12
        #df['test_e'] = df['description_embedding'].apply(lambda x: ( ((np.array(x) * 10**(dec_len)).astype(np.int64)) / 10**(dec_len)).tolist() )
        #df['description_embedding']=df['test_e']
        # Create or replace table
        if overwrite:
            self.setup_entities()

        # Ingest data

        ingestion_command = f".ingest inline into table {self.collection_name} <| {df.to_csv(index=False, header=False)}"

        self.client.execute(self.database, ingestion_command)


    def setup_reports(self) -> None:
        # if self._check_if_table_exists(self.reports_name):
        #     return
        command = f".drop table {self.reports_name} ifexists"
        self.client.execute(self.database, command)
        command = f".create table {self.reports_name} (id: string, short_id: string, title: string, community_id: string, summary: string, full_content: string, rank: real, summary_embedding: dynamic, full_content_embedding: dynamic, attributes: dynamic)"
        self.client.execute(self.database, command)
        command = f".alter column {self.reports_name}.summary_embedding policy encoding type = 'Vector16'"
        self.client.execute(self.database, command)
        command = f".alter column {self.reports_name}.full_content_embedding policy encoding type = 'Vector16'"
        self.client.execute(self.database, command)

    def load_reports(self, reports: list[CommunityReport], overwrite: bool = False) -> None:
        # Convert data to DataFrame
        df = pd.DataFrame(reports)

        # Create or replace table
        if overwrite:
            self.setup_reports()

        # Ingest data
        ingestion_command = f".ingest inline into table {self.reports_name} <| {df.to_csv(index=False, header=False)}"
        self.client.execute(self.database, ingestion_command)

    def setup_text_units(self) -> None:
        if self._check_if_table_exists(self.text_units_name):
            return
        command = f".drop table {self.text_units_name} ifexists	"
        self.client.execute(self.database, command)

        pt_enabled = os.environ.get("PROTOTYPE")

        if not pt_enabled:
            command = f".create table {self.text_units_name} (id: string, short_id:string, \
                text: string, text_embedding:string, entity_ids: string, relationship_ids: \
                    string, covariate_ids:string, n_tokens: string, document_ids: string, \
                        attributes:string )"
        else:
            command=f".create table {self.text_units_name} (id: string, short_id:string,document_ids:string)"

        self.exe(command)


    def load_text_units(self, units: list[TextUnit], overwrite: bool = False) -> None:
        df = pd.DataFrame(units)



        if overwrite:
            self.setup_text_units()

        pt_enabled = os.environ.get("PROTOTYPE")

        if pt_enabled:
            #remove unwanted data (prototype)
            df.drop("text",axis=1,inplace=True)
            df.drop("text_embedding",axis=1,inplace=True)
            df.drop("entity_ids",axis=1,inplace=True)
            df.drop("relationship_ids",axis=1,inplace=True)
            df.drop("covariate_ids",axis=1,inplace=True)
            df.drop("n_tokens",axis=1,inplace=True)
            df.drop("attributes",axis=1,inplace=True)

        ingestion_command = f".ingest inline into table {self.text_units_name} <| {df.to_csv(index=False, header=False)}"
        self.client.execute(self.database, ingestion_command)

    def setup_docs(self) -> None: #Called by indexer
        command = f".drop table {self.docs_tbl_name} ifexists"
        self.client.execute(self.database, command)
        command = f".create table {self.docs_tbl_name} (id: string, in_path:string, \
            out_path: string)"

        self.exe(command)

    def load_doc_stats(self, rows) -> None: #called by indexer
        df = pd.DataFrame(rows)
        ingestion_command = f".ingest inline into table {self.docs_tbl_name} <| {df.to_csv(index=False, header=False)}"
        self.client.execute(self.database, ingestion_command)

    def exe(self,command):
        return self.client.execute(self.database,command)

    def retrieve_text_units(self, entities: list[Entity]):
        unit_ids=[]

        for e in entities:
            if e.text_unit_ids==None or e.text_unit_ids=='':
                continue
            id_list=ast.literal_eval(e.text_unit_ids)
            unit_ids.extend([id for id in id_list])
        return self.retrieve_text_units_by_id(unit_ids)
    def retrieve_text_units_by_id(self,unit_ids):
        unit_ids_str=", ".join(f"'{id}'" for id in unit_ids )

        command=f"{self.text_units_name} | where id in ({unit_ids_str})"
        r=self.exe(command)
        r=dataframe_from_result_table(r.primary_results[0])

        pt_enabled = os.environ.get("PROTOTYPE")



        res=[]
        cite_index=1
        for _,row in  r.iterrows():
            u=TextUnit(
                id=row['id'],
                short_id=str(cite_index),
                text=row['text'] if not pt_enabled else '',
                text_embedding=[],
                entity_ids=row['entity_ids'] if not pt_enabled else '[]',
                relationship_ids=row['relationship_ids']  if not pt_enabled else '[]' ,
                covariate_ids=[],
                n_tokens=row['n_tokens'] if not pt_enabled else '',
                document_ids=row['document_ids'],
                attributes={} #row['attributes'],
            )
            res.append(u)
            cite_index+=1

        return res

    def get_extracted_reports(
        self, community_ids: list[int], **kwargs: Any
    ) -> list[CommunityReport]:


        community_ids = ", ".join([str(id) for id in community_ids])
        query = f"""
        {self.reports_name}
        | where community_id in ({community_ids})
        """
        response = self.client.execute(self.database, query)
        df = dataframe_from_result_table(response.primary_results[0])

        return [
            CommunityReport(
                id=row["id"],
                short_id=row["short_id"],
                title=row["title"],
                community_id=row["community_id"],
                summary=row["summary"],
                full_content=row["full_content"],
                rank=row["rank"],
                summary_embedding=row["summary_embedding"],
                full_content_embedding=row["full_content_embedding"],
                attributes=row["attributes"],
            ) for _, row in df.iterrows()
        ]

    def _check_if_table_exists(self, table_name: str) -> bool:
        try:
            command = f".show tables | where TableName == '{table_name}'"
            response = self.client.execute(self.database, command)
            logging.info(f"The table {table_name} exists status: {str(len(response.primary_results))}")
            return response.primary_results[0].rows_count > 0
        except Exception as ex:
            logging.error(ex)
            raise
    
    def get_matching_relationships(self, query: str, text_embedder: TextEmbedder, k: int = 10,
                               relationship_ids=[], depth=1,
                               **kwargs: Any
    ):
        # Get top text units using similarity search
        query_embedding = text_embedder(query)

        if relationship_ids==[]:
            cmd = f"""
                let query_vector = dynamic({query_embedding});
                {self.relationships_name}
                | extend similarity = series_cosine_similarity(query_vector, text_unit_embedding)
                | top {k} by similarity desc
                """
        else:
            cmd = f"""
                let query_vector = dynamic({query_embedding});
                {self.relationships_name} | 
                where id in ({relationship_ids}) | 
                | extend similarity = series_cosine_similarity(query_vector, text_unit_embedding)
                | top {k} by similarity desc
                """
            
        response = self.exe( cmd)
        df = dataframe_from_result_table(response.primary_results[0])

        # Get all edges in retrieved rows    
        rels=[]
        for _,row in df.iterrows():
            txt_unit_l = row['text_unit_ids']
            if txt_unit_l == '' or txt_unit_l==None :
                print( "Unexpected relationship: missing text unit" )
                exit(-1)
            txt_unit_l = ast.literal_eval(txt_unit_l)

            if len(txt_unit_l) != 1:
                print( "Unexpected relationship: Zero/Multiple text units in one row" )
                exit(-1)

            r=Relationship(
                source=row['source'],
                target=row['target'],
                id=row['id'],
                short_id="0",
                source_id=row['source_id'],
                target_id=row['target_id'],
                weight=row['weight'],
                text_unit_ids=txt_unit_l,  #must have only one ID
                text_unit=row['text_unit']
            )
            rels.append(r)
        
        return rels
    
    def setup_relationships(self):
        command = f".drop table {self.relationships_name} ifexists"
        self.client.execute(self.database, command)

        rels_schema = (f".create table {self.relationships_name} (id: string, source: string, target:string,"
                                "weight: real ,"
                                "text_unit_ids: dynamic,"                               
                                "source_id:string,"
                                "target_id:string,"
                                "text_unit_embedding:dynamic,text_unit:string)"
                                )

        self.client.execute(self.database, rels_schema)

    def load_relationships(self, rels: list[Relationship], overwrite: bool = False):
        df = pd.DataFrame(rels)

        #df.drop("source",axis=1,inplace=True)
        #df.drop("target",axis=1,inplace=True)
        df.drop("short_id",axis=1,inplace=True)
        df.drop("description",axis=1,inplace=True)
        df.drop("description_embedding",axis=1,inplace=True) #####
        df.drop("document_ids",axis=1,inplace=True)
        df.drop("attributes",axis=1,inplace=True)

        ingestion_command = f".ingest inline into table {self.relationships_name} <| {df.to_csv(index=False, header=False)}"

        self.client.execute(self.database, ingestion_command)
    def get_expanding_edges_excluding_vertices(self,current_vertices,query,excluding_vertices,excluding_edges_ids,text_embedder):
        current_vertices_str =", ".join(f"'{id}'" for id in current_vertices )
        excluding_vertices_str =", ".join(f"'{id}'" for id in excluding_vertices )
        excluding_edges_str = ", ".join(f"'{id}'" for id in excluding_edges_ids )
        if len(excluding_edges_ids)==0:
            exclude_edge_id_filter = True
        else:
            exclude_edge_id_filter = f"id !in ({excluding_edges_str})"
        query_embedding = text_embedder.embed(query)
        kusto_query = f"""
        let query_vector = dynamic({query_embedding});
        {self.relationships_name}
        | where source_id in ({current_vertices_str})
        | where target_id !in ({excluding_vertices_str})
        | where ({exclude_edge_id_filter})
        | extend similarity = series_cosine_similarity(query_vector, text_unit_embedding)
        | sort by similarity desc
        """
        response = self.client.execute(self.database, kusto_query)
        df = dataframe_from_result_table(response.primary_results[0])
        return [
            Relationship(
                id=row['id'],
                source=row['source'],
                target=row['target'],
                short_id=row_index,
                source_id=row['source_id'],
                target_id=row['target_id'],
                text_unit_ids=row['text_unit_ids'],
                attributes={'similarity':row['similarity']}
            )
            for row_index, row in df.iterrows()
        ]
    
    def get_expanding_edges_including_vertices(self,current_vertices,query,including_vertices,excluding_edges_ids,text_embedder):
        current_vertices_str =", ".join(f"'{id}'" for id in current_vertices )
        including_vertices_str =", ".join(f"'{id}'" for id in including_vertices )
        excluding_edges_str = ", ".join(f"'{id}'" for id in excluding_edges_ids )
        if len(excluding_edges_ids)==0:
            exclude_edge_id_filter = True
        else:
            exclude_edge_id_filter = f"id !in ({excluding_edges_str})"
        query_embedding = text_embedder.embed(query)
        kusto_query = f"""
        let query_vector = dynamic({query_embedding});
        {self.relationships_name}
        | where source_id in ({current_vertices_str})
        | where target_id in ({including_vertices_str})
        | where ({exclude_edge_id_filter})
        | extend similarity = series_cosine_similarity(query_vector, text_unit_embedding)
        | sort by similarity desc
        """
        response = self.client.execute(self.database, kusto_query)
        df = dataframe_from_result_table(response.primary_results[0])
        return [
            Relationship(
                id=row['id'],
                source=row['source'],
                target=row['target'],
                short_id=row_index,
                source_id=row['source_id'],
                target_id=row['target_id'],
                text_unit_ids=row['text_unit_ids'],
                attributes={'similarity':row['similarity']}
            )
            for row_index, row in df.iterrows()
        ]
    
