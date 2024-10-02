import os
import pandas as pd

from graphrag.config.models.graphdb_config import GraphDBConfig
import numpy as np

import ast
import logging
from gremlin_python.driver import client, serializer
from azure.identity import ManagedIdentityCredential

import time
import os
import json




# Azure Cosmos DB Gremlin Endpoint and other constants
COSMOS_DB_SCOPE = "https://cosmos.azure.com/.default"  # The scope for Cosmos DB
class GraphDBClient:
    def __init__(self,graph_db_params: GraphDBConfig|None,context_id: str|None):
        self.username_prefix=graph_db_params.username
        token = f"{graph_db_params.account_key}"
        #if(os.environ.get("ENVIRONMENT") == "AZURE"):
        #    credential = ManagedIdentityCredential(client_id="295ce65c-28c6-4763-be6f-a5eb36c3ceb3")
        #    token = credential.get_token(COSMOS_DB_SCOPE)
        self._client=client.Client(
            url=f"{graph_db_params.gremlin_url}",
            traversal_source="g",
            username=self.username_prefix+"-contextid-"+context_id,
            password=token,
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

    def remove_graph(self):
        self._client.submit(message=("g.V().drop()"))

    def query_vertices(self,context_id:str) -> pd.DataFrame:
        result = self._client.submit(
            message=(
                "g.V()"
            ),
        )
        return self.result_to_df(result)

    def query_edges(self,context_id:str) -> pd.DataFrame:
        result = self._client.submit(
            message=(
                "g.E()"
            ),
        )
        return self.result_to_df(result)

    def element_exists(self,element_type:str,element_id:int,conditions:str="")->bool:
        result=self._client.submit(
                message=(
                        element_type+
                        ".has('id',prop_id)"+
                        conditions+
                        ".count()"
                ),
                bindings={
                        "prop_id":element_id,
                }
        )
        element_count=0
        for counts in result:
            element_count=counts[0]
        return element_count>0

    def write_vertices(self,data: pd.DataFrame,vmap)->None:
        step_df=50
        logging.info("start")
        split_dataframes=[data[i:i+step_df] for i in range(0,len(data),step_df)]
        
        for spilt_df in split_dataframes:
            q="g"
            query_bindings={}
            iter_row=0

            for row in spilt_df.itertuples():
                
                #if self.element_exists("g.V()",row.id):
                #    continue
                
                if row.id in vmap:
                    continue
                    
                vmap[row.id]=1

                q+=(
                    ".addV('entity')"
                    ".property('id', prop_id"+str(iter_row)+")"
                    ".property('name', prop_name"+str(iter_row)+")"
                    ".property('type', prop_type"+str(iter_row)+")"
                    ".property('description', prop_description"+str(iter_row)+")"
                    ".property('human_readable_id', prop_human_readable_id"+str(iter_row)+")"
                    ".property('category', 'entities')"
                )
                query_bindings.update({
                    ("prop_id"+str(iter_row)): row.id,
                    ("prop_name"+str(iter_row)): row.name,
                    ("prop_type"+str(iter_row)): row.type,
                    ("prop_description"+str(iter_row)): row.description,
                    ("prop_human_readable_id"+str(iter_row)): row.human_readable_id,
                })
                iter_row+=1

            ##########################################################################

            self._client.submit(
                message=q,
                bindings=query_bindings
            )

        logging.info("end")

    def write_edges(self,data: pd.DataFrame)->None:
        step_df=100
        split_dataframes=[data[i:i+step_df] for i in range(0,len(data),step_df)]
        for spilt_df in split_dataframes:
            q="g"
            query_bindings={}
            iter_row=0
            for row in spilt_df.itertuples():
                #if self.element_exists("g.E()",row.id):
                #    continue
                q+=(
                    ".V().has('name',prop_source_id"+str(iter_row)+")"
                    ".addE('connects')"
                    ".to(g.V().has('name',prop_target_id"+str(iter_row)+"))"
                    ".property('weight',prop_weight"+str(iter_row)+")"
                    ".property('description',prop_description"+str(iter_row)+")"
                    ".property('id',prop_id"+str(iter_row)+")"
                    ".property('human_readable_id',prop_human_readable_id"+str(iter_row)+")"
                    ".property('source_degree',prop_source_degree"+str(iter_row)+")"
                    ".property('target_degree',prop_target_degree"+str(iter_row)+")"
                    ".property('rank',prop_rank"+str(iter_row)+")"
                    ".property('source',prop_source"+str(iter_row)+")"
                    ".property('target',prop_target"+str(iter_row)+")"
                    ".property(list,'text_unit_ids',prop_text_unit_ids"+str(iter_row)+")"
                )
                query_bindings.update({
                    ("prop_source_id"+str(iter_row)): row.source,
                    ("prop_target_id"+str(iter_row)): row.target,
                    ("prop_weight"+str(iter_row)): row.weight,
                    ("prop_description"+str(iter_row)): row.description,
                    ("prop_id"+str(iter_row)): row.id,
                    ("prop_human_readable_id"+str(iter_row)): row.human_readable_id,
                    ("prop_source_degree"+str(iter_row)): row.source_degree,
                    ("prop_target_degree"+str(iter_row)): row.target_degree,
                    ("prop_rank"+str(iter_row)): row.rank,
                    ("prop_source"+str(iter_row)): row.source,
                    ("prop_target"+str(iter_row)): row.target,
                    ("prop_text_unit_ids"+str(iter_row)):json.dumps(row.text_unit_ids.tolist() if row.text_unit_ids is not None else []),
                })
                iter_row+=1


            self._client.submit(
                message=q,
                bindings=query_bindings
            )

    def get_top_related_unique_edges(self, entity_id: str, top: int) -> [dict[str, str]]:
        """Retrieve the top related unique edges for a given entity.
        This method queries the graph database to find the top related unique edges
        connected to the specified entity. The edges are sorted by weight in descending
        order, and the top N edges are returned.
        Args:
            entity_id (str): The ID of the entity for which to find related edges.
            top (int): The number of top related edges to retrieve.

        Returns
        -------
            A list of dictionaries containing the related entity IDs, weights, and text unit IDs.
        """
        env = os.environ.get("ENVIRONMENT")

        if env=='DEVELOPMENT':
            #Load relationships

            result = self._client.submit(
                message=(
                    f"""g.V().has('id', '{entity_id}')
                    .bothE('connects')
                      .project('id','source_id', 'target_id', 'weight','text_unit_ids','description','source','target','rank')
						.by('id')
                        .by(outV().values('id'))
                        .by(inV().values('id'))
                        .by('weight')
                        .by('text_unit_ids')
                        .by('description')
                        .by('source')
                        .by('target')
                        .by('rank')
                    .group()
                        .by(select('source_id', 'target_id'))
                        .by(fold())
                    .unfold()
                    .select(values)
                    .unfold()
                    .order().by(select('weight'), decr)
                    .dedup('source_id','target_id')
                    .limit({top})
                    """
                ),
            )

            json_data = []
            for rows in result:
                for row in rows:
                    id=row['id']
                    source_id = row['source_id']
                    target_id = row['target_id']
                    weight = row['weight']
                    text_unit_ids = row['text_unit_ids']
                    description = row['description']
                    source = row['source']
                    target = row['target']
                    rank=row['rank']
                    related_entity_id = source_id if source_id != entity_id else target_id
                    json_data.append({'id':id,'entity_id': related_entity_id, 'weight': weight, 'text_unit_ids': text_unit_ids,
                                      'description':description, 'source':source, 'target':target,'rank':rank})

        else:
            result = self._client.submit(
                message=(
                    f"""g.V().has('id', '{entity_id}')
                    .bothE('connects')
                    .project('source_id', 'target_id', 'weight','text_unit_ids')
                        .by(outV().values('id'))
                        .by(inV().values('id'))
                        .by('weight')
                        .by('text_unit_ids')
                    .group()
                        .by(select('source_id', 'target_id'))
                        .by(fold())
                    .unfold()
                    .select(values)
                    .unfold()
                    .order().by(select('weight'), decr)
                    .dedup('source_id','target_id')
                    .limit({top})
                    """
                ),
            )

            json_data = []
            for rows in result:
                for row in rows:
                    source_id = row['source_id']
                    target_id = row['target_id']
                    weight = row['weight']
                    text_unit_ids = row['text_unit_ids']
                    related_entity_id = source_id if source_id != entity_id else target_id
                    json_data.append({'entity_id': related_entity_id, 'weight': weight, 'text_unit_ids': text_unit_ids})

        return json_data