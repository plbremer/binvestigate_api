import networkx as nx
import pathlib


from flask import Flask,request
from flask_restful import Api, Resource, reqparse
import json

PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("../datasets").resolve()

class RootDistanceResource(Resource):

    def post(self):
        '''
        step 1
        use networkx to determine what parent nodes to include
        step 2 
        modify the query string as necessary
        step 3
        execute the query, return results
        '''
        compound=request.json['compound']
        from_species=str(request.json['from_species'])
        from_organ=request.json['from_organ']
        from_disease=request.json['from_disease']
        to_species=str(request.json['to_species'])
        to_organ=request.json['to_organ']
        to_disease=request.json['to_disease']

        compound_networkx_path=DATA_PATH.joinpath("compounds_networkx.bin")
        compound_networkx=nx.readwrite.gpickle.read_gpickle(compound_networkx_path)
        species_networkx_path=DATA_PATH.joinpath("species_networkx.bin")
        species_networkx=nx.readwrite.gpickle.read_gpickle(species_networkx_path)

        if compound != "any":
            valid_compounds=nx.ancestors(compound_networkx,compound).add(compound)
        else:
            valid_compounds="no_restrictions"

        if (from_species != "any") and (to_species == "any"):
            valid_parents_species=nx.ancestors(species_networkx,from_species).add(from_species)
        elif (from_species == "any") and (to_species != "any"):
            valid_parents_species=nx.ancestors(species_networkx,to_species).add(to_species)
        elif (from_species != "any") and (to_species != "any"):
            # valid_parents_species_from=nx.ancestors(species_networkx,from_species).append(from_species)
            # valid_parents_species_to=nx.ancestors(species_networkx,to_species).append(to_species)
            lowest_parent=nx.lowest_common_ancestor(species_networkx,from_species,to_species)
            path_from=nx.algorithms.shortest_path(species_networkx,lowest_parent,from_species)
            path_to=nx.algorithms.shortest_path(species_networkx,lowest_parent,to_species)
            print(lowest_parent)
            print(path_from)
            print(path_to)
            combined_paths=path_from.union(path_to)
            print(combined_paths)
        
