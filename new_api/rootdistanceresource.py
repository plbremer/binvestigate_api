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
            compound_path=nx.ancestors(compound_networkx,compound).union({str(compound)})
        else:
            compound_path=set()

        if (from_species != "any") and (to_species == "any"):
            species_path_from=nx.ancestors(species_networkx,from_species).add(from_species)
            species_path_to=set()
        elif (from_species == "any") and (to_species != "any"):
            species_path_from=set()
            species_path_to=nx.ancestors(species_networkx,to_species).add(to_species)
        elif (from_species != "any") and (to_species != "any"):
            lowest_parent=nx.lowest_common_ancestor(species_networkx,from_species,to_species)
            species_path_from=nx.algorithms.shortest_path(species_networkx,lowest_parent,from_species)
            species_path_to=nx.algorithms.shortest_path(species_networkx,lowest_parent,to_species)

        if (from_organ != "any") and (to_organ == "any"):
            organ_path_from=nx.ancestors(organ_networkx,from_organ).add(from_organ)
            organ_path_to=set()
        elif (from_organ == "any") and (to_organ != "any"):
            organ_path_from=set()
            organ_path_to=nx.ancestors(organ_networkx,to_organ).add(to_organ)
        elif (from_organ != "any") and (to_organ != "any"):
            lowest_parent=nx.lowest_common_ancestor(organ_networkx,from_organ,to_organ)
            organ_path_from=nx.algorithms.shortest_path(organ_networkx,lowest_parent,from_organ)
            organ_path_to=nx.algorithms.shortest_path(organ_networkx,lowest_parent,to_organ)

        if (from_disease != "any") and (to_disease == "any"):
            disease_path_from=nx.ancestors(disease_networkx,from_disease).add(from_disease)
            disease_path_to=set()
        elif (from_disease == "any") and (to_disease != "any"):
            disease_path_from=set()
            disease_path_to=nx.ancestors(disease_networkx,to_disease).add(to_disease)
        elif (from_disease != "any") and (to_disease != "any"):
            lowest_parent=nx.lowest_common_ancestor(disease_networkx,from_disease,to_disease)
            disease_path_from=nx.algorithms.shortest_path(disease_networkx,lowest_parent,from_disease)
            disease_path_to=nx.algorithms.shortest_path(disease_networkx,lowest_parent,to_disease)

        compound_path=list(compound_path)
        species_path_from=list(species_path_from)
        species_path_to=list(species_path_to)
        organ_path_from=list(organ_path_from)
        organ_path_to=list(organ_path_to)
        disease_path_from=list(disease_path_from)
        disease_path_to=list(disease_path_to)

        print(compound_path)
        print(species_path_from)
        print(species_path_to)
        print(organ_path_from)
        print(organ_path_to)
        print(disease_path_from)
        print(disease_path_to)