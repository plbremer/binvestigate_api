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
        organ_networkx_path=DATA_PATH.joinpath("organ_networkx.bin")
        organ_networkx=nx.readwrite.gpickle.read_gpickle(organ_networkx_path)
        disease_networkx_path=DATA_PATH.joinpath("disease_networkx.bin")
        disease_networkx=nx.readwrite.gpickle.read_gpickle(disease_networkx_path)

        if compound != "any":
            valid_compounds=nx.ancestors(compound_networkx,compound).add(compound)
        else:
            valid_compounds="no_restrictions"

        if (from_species != "any") and (to_species == "any"):
            valid_species=nx.ancestors(species_networkx,from_species).add(from_species)
        elif (from_species == "any") and (to_species != "any"):
            valid_species=nx.ancestors(species_networkx,to_species).add(to_species)
        elif (from_species != "any") and (to_species != "any"):
            # valid_parents_species_from=nx.ancestors(species_networkx,from_species).append(from_species)
            # valid_parents_species_to=nx.ancestors(species_networkx,to_species).append(to_species)
            lowest_parent=nx.lowest_common_ancestor(species_networkx,from_species,to_species)
            path_from=nx.algorithms.shortest_path(species_networkx,lowest_parent,from_species)
            path_to=nx.algorithms.shortest_path(species_networkx,lowest_parent,to_species)
            # print(lowest_parent)
            # print(path_from)
            # print(path_to)
            valid_species=set(path_from).union(set(path_to))
        
        if (from_species != "any") and (to_species == "any"):
            valid_species=nx.ancestors(species_networkx,from_species).add(from_species)
        elif (from_species == "any") and (to_species != "any"):
            valid_species=nx.ancestors(species_networkx,to_species).add(to_species)
        elif (from_species != "any") and (to_species != "any"):
            # valid_parents_species_from=nx.ancestors(species_networkx,from_species).append(from_species)
            # valid_parents_species_to=nx.ancestors(species_networkx,to_species).append(to_species)
            lowest_parent=nx.lowest_common_ancestor(species_networkx,from_species,to_species)
            path_from=nx.algorithms.shortest_path(species_networkx,lowest_parent,from_species)
            path_to=nx.algorithms.shortest_path(species_networkx,lowest_parent,to_species)
            # print(lowest_parent)
            # print(path_from)
            # print(path_to)
            valid_species=set(path_from).union(set(path_to))

        if (from_organ != "any") and (to_organ == "any"):
            valid_organ=nx.ancestors(organ_networkx,from_organ).add(from_organ)
        elif (from_organ == "any") and (to_organ != "any"):
            valid_organ=nx.ancestors(organ_networkx,to_organ).add(to_organ)
        elif (from_organ != "any") and (to_organ != "any"):
            # valid_parents_organ_from=nx.ancestors(organ_networkx,from_organ).append(from_organ)
            # valid_parents_organ_to=nx.ancestors(organ_networkx,to_organ).append(to_organ)
            lowest_parent=nx.lowest_common_ancestor(organ_networkx,from_organ,to_organ)
            path_from=nx.algorithms.shortest_path(organ_networkx,lowest_parent,from_organ)
            path_to=nx.algorithms.shortest_path(organ_networkx,lowest_parent,to_organ)
            # print(lowest_parent)
            # print(path_from)
            # print(path_to)
            valid_organ=set(path_from).union(set(path_to))

        if (from_disease != "any") and (to_disease == "any"):
            valid_disease=nx.ancestors(disease_networkx,from_disease).add(from_disease)
        elif (from_disease == "any") and (to_disease != "any"):
            valid_disease=nx.ancestors(disease_networkx,to_disease).add(to_disease)
        elif (from_disease != "any") and (to_disease != "any"):
            # valid_parents_disease_from=nx.ancestors(disease_networkx,from_disease).append(from_disease)
            # valid_parents_disease_to=nx.ancestors(disease_networkx,to_disease).append(to_disease)
            lowest_parent=nx.lowest_common_ancestor(disease_networkx,from_disease,to_disease)
            path_from=nx.algorithms.shortest_path(disease_networkx,lowest_parent,from_disease)
            path_to=nx.algorithms.shortest_path(disease_networkx,lowest_parent,to_disease)
            # print(lowest_parent)
            # print(path_from)
            # print(path_to)
            valid_disease=set(path_from).union(set(path_to))