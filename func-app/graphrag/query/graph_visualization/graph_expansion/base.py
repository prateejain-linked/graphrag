# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

from abc import ABC, abstractmethod

from graphrag.query.llm.base import BaseTextEmbedding

class BaseGraphExpander(ABC):
    def __init__(self,kusto_client,graphdb_client,text_embedder: BaseTextEmbedding):
        self.kusto_client = kusto_client
        self.graphdb_client = graphdb_client
        self.text_embedder = text_embedder
    
    @abstractmethod
    def expand_node(graph,node,depth,top_k,query_embedding):
        """Expand the graph with paths starting at node"""