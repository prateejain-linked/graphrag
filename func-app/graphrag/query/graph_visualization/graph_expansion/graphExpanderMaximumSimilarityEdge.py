import ast
from .base import BaseGraphExpander
from graphrag.model import Relationship
import networkx as nx

class GraphExpanderMaximumSimilarityEdge(BaseGraphExpander):
    def expand_node(self,node,depth,top_k,query,excluding_edges_ids,use_kusto=False):
        graph = nx.Graph()
        if use_kusto:
            tree_edges,backtrack_edges,parents_dictionary = self.get_relevant_edges([node],[node],depth,top_k,query,excluding_edges_ids)
            top_k_edges = self.merge_edges_lists(tree_edges,backtrack_edges,top_k)
            top_k_paths_edges = self.build_paths_from_edges(top_k_edges,parents_dictionary,node)
            for top_k_path_edge in top_k_paths_edges:
                top_k_path_edge_source = top_k_path_edge[0]
                top_k_path_edge_target = top_k_path_edge[1]
                if not graph.has_node(top_k_path_edge_source):
                    graph.add_node(top_k_path_edge_source)
                if not graph.has_node(top_k_path_edge_target):
                    graph.add_node(top_k_path_edge_target)
                graph.add_edge(
                    top_k_path_edge_source,
                    top_k_path_edge_target,
                    text_unit = top_k_path_edge[2][0],
                )
        else:
            all_edges = self.get_all_edges_subtree(node,depth)
            all_edges_ids = [edge.id for edge in all_edges]
            ##Remove excluding_edges_ids
            top_k_edges = self.kusto_client.get_top_k_relationships_by_text_unit_similarity(all_edges_ids,top_k,query,self.text_embedder)
            for top_k_edge in top_k_edges:
                top_k_edge_source = top_k_edge.source_id
                top_k_edge_target = top_k_edge.target_id
                if not graph.has_node(top_k_edge_source):
                    graph.add_node(top_k_edge_source)
                if not graph.has_node(top_k_edge_target):
                    graph.add_node(top_k_edge_target)
                graph.add_edge(
                    top_k_edge_source,
                    top_k_edge_target,
                    text_unit = top_k_edge.text_unit_ids[0],
                )
        graphml = "".join(nx.generate_graphml(graph))
        return graphml.replace("<graphml",f"<graphml initial_node={node}")
            

    def build_paths_from_edges(self,top_k_edges,parents_dictionary,node):
        top_k_paths_edges=[]
        for top_k_edge in top_k_edges:
            parent=top_k_edge.source_id
            current_target = top_k_edge.target_id
            text_unit = ast.literal_eval(top_k_edge.text_unit_ids)
            top_k_paths_edges.append((parent,current_target,text_unit))
            while parent!=node:
                top_k_paths_edges.append((parent,current_target,text_unit))#replace this with relationship object
                current_target = parent
                parent,text_unit = parents_dictionary[parent]
        return top_k_paths_edges

    def get_relevant_edges(self,current_vertices,visited_vertices,depth,top_k,query,excluding_edges_ids):
        if depth==0:
            return ([],[],{})
        expanding_edges = self.get_outgoing_edges(current_vertices,visited_vertices,query,excluding_edges_ids)
        backtracking_edges = self.get_backtracking_edges(current_vertices,visited_vertices,query,excluding_edges_ids)
        expanding_vertices = set()
        parents_dictionary = {}
        for expanding_edge in expanding_edges:
            if expanding_edge.target_id not in expanding_vertices:
                expanding_vertices.add(expanding_edge.target_id)
                parents_dictionary[expanding_edge.target_id]={expanding_edge.source_id,expanding_edge.text_unit_ids[0]}
            else:
                backtracking_edges.append(expanding_edge)
        visited_vertices = visited_vertices.append(current_vertices)
        subtree_edges,subtree_backtrack_edges,subtree_parents_dictionary = self.get_relevant_edges(expanding_vertices,visited_vertices,depth-1,top_k,query,excluding_edges_ids)
        tree_edges = self.merge_edges_lists(subtree_edges,expanding_edges,top_k)
        tree_backtrack_edges = self.merge_edges_lists(subtree_backtrack_edges,backtracking_edges,top_k)
        parents_dictionary.update(subtree_parents_dictionary)
        return (tree_edges,tree_backtrack_edges,parents_dictionary)

    def get_outgoing_edges(self,current_vertices,visited_vertices,query,excluding_edges_ids):
        outgoing_edges = self.kusto_client.get_expanding_edges_excluding_vertices(current_vertices,query,visited_vertices,excluding_edges_ids,self.text_embedder)
        return outgoing_edges

    def get_backtracking_edges(self,current_vertices,visited_vertices,query,excluding_edges_ids):
        backtracking_edges = self.kusto_client.get_expanding_edges_including_vertices(current_vertices,query,visited_vertices,excluding_edges_ids,self.text_embedder)
        return backtracking_edges

    def merge_edges_lists(self,edge_list_a,edge_list_b,top_k):
        merged_edge_list=[]
        index_a, index_b = 0 , 0 
        while len(merged_edge_list)<top_k and (index_a<len(edge_list_a) or index_b<len(edge_list_b)):
            if index_a<len(edge_list_a) and (index_b>=len(edge_list_b) or edge_list_a[index_a].attributes['similarity'] > edge_list_b[index_b].attributes['similarity']):
                merged_edge_list.append(edge_list_a[index_a])
                index_a+=1
            else:
                merged_edge_list.append(edge_list_b[index_b])
                index_b+=1
        return merged_edge_list
    
    def get_all_edges_subtree(self,node,depth):
        json_response = self.graphdb_client.get_all_edges_within_depth(node,depth)
        relationship_response = []
        r_id=0
        for edge in json_response:
            r=Relationship(id=edge['id'], source=edge['source_id'],target=edge['target_id'],short_id=r_id,source_id=edge['source_id'],target_id=edge['target_id'])
            r_id+=1
            relationship_response.append(r)
        return relationship_response