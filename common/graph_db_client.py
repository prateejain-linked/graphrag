import os
import pandas as pd

from graphrag.config.models.graphdb_config import GraphDBConfig
import numpy as np

import ast

from gremlin_python.driver import client, serializer

import time
import os
import json

class GraphDBClient:
    def __init__(self,graph_db_params: GraphDBConfig|None):
        self._client=client.Client(
            url=f"wss://{graph_db_params.account_name}.gremlin.cosmos.azure.com:443/",
            traversal_source="g",
            username=graph_db_params.username,
            password=f"{graph_db_params.account_key}",
            message_serializer=serializer.GraphSONSerializersV2d0(),
        )

    def result_to_df(self,result) -> pd.DataFrame:
        json_data = []
        for row in result:
            json_row = row[0]
            properties_dict = json_row.pop('properties')
            formatted_properties={}
            for k,v in properties_dict.items():
                new_val=v
                if isinstance(v,list) and isinstance(v[0],dict):
                    new_val=v[0]['value']
                if k=='description_embedding' or k =='text_unit_ids' or k=='graph_embedding':
                    new_val=ast.literal_eval(new_val)
                if isinstance(new_val,list):
                    new_val=np.array(new_val)
                formatted_properties[k]=new_val
            json_row.update(formatted_properties)
            json_data.append(json_row)
        df = pd.DataFrame(json_data)
        return df

    def query_vertices(self) -> pd.DataFrame:
        result = self._client.submit(
            message=(
                "g.V()"
            ),
        )
        return self.result_to_df(result)

    def query_edges(self) -> pd.DataFrame:
        result = self._client.submit(
            message=(
                "g.E()"
            ),
        )
        return self.result_to_df(result)

    def write_vertices(self,data: pd.DataFrame)->None:
        for row in data.itertuples():
            print(row.id)
            self._client.submit(
                message=(
                    "g.addV('entity')"
                    ".property('id', prop_id)"
                    ".property('name', prop_name)"
                    ".property('type', prop_type)"
                    ".property('description','prop_description')"
                    ".property('human_readable_id', prop_human_readable_id)"
                    ".property('category', prop_partition_key)"
                    ".property(list,'description_embedding',prop_description_embedding)"
                    ".property(list,'graph_embedding',prop_graph_embedding)"
                    ".property(list,'text_unit_ids',prop_text_unit_ids)"
                ),
                bindings={
                    "prop_id": row.id,
                    "prop_name": row.name,
                    "prop_type": row.type,
                    "prop_description": row.description,
                    "prop_human_readable_id": row.human_readable_id,
                    "prop_partition_key": "entities",
                    "prop_description_embedding":json.dumps(row.description_embedding.tolist() if row.description_embedding is not None else []),
                    "prop_graph_embedding":json.dumps(row.graph_embedding.tolist() if row.graph_embedding is not None else []),
                    "prop_text_unit_ids":json.dumps(row.text_unit_ids if row.text_unit_ids is not None else []),
                },
            )
            time.sleep(5)


    def write_edges(self,data: pd.DataFrame)->None:
        for row in data.itertuples():
            print(row.source,row.target)
            self._client.submit(
                message=(
                    "g.V().has('name',prop_source_id)"
                    ".addE('connects')"
                    ".to(g.V().has('name',prop_target_id))"
                    ".property('weight',prop_weight)"
                    ".property(list,'text_unit_ids',prop_text_unit_ids)"
                    ".property('description',prop_description)"
                    ".property('id',prop_id)"
                    ".property('human_readable_id',prop_human_readable_id)"
                    ".property('source_degree',prop_source_degree)"
                    ".property('target_degree',prop_target_degree)"
                    ".property('rank',prop_rank)"
                    ".property('source',prop_source)"
                    ".property('target',prop_target)"
                ),
                bindings={
                    "prop_partition_key": "entities",
                    "prop_source_id": row.source,
                    "prop_target_id": row.target,
                    "prop_weight": row.weight,
                    "prop_text_unit_ids":json.dumps(row.text_unit_ids if row.text_unit_ids is not None else []),
                    "prop_description": row.description,
                    "prop_id": row.id,
                    "prop_human_readable_id": row.human_readable_id,
                    "prop_source_degree": row.source_degree,
                    "prop_target_degree": row.target_degree,
                    "prop_rank": row.rank,
                    "prop_source": row.source,
                    "prop_target": row.target,
                },
            )
            time.sleep(5)