import networkx as nx
import pathlib

from rootdistancequery import RootDistanceQuery

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
        compound_dfr=request.json['compound_dfr']
        species_from_dfr=request.json['species_from_dfr']
        organ_from_dfr=request.json['organ_from_dfr']
        disease_from_dfr=request.json['disease_from_dfr']
        species_to_dfr=request.json['species_to_dfr']
        organ_to_dfr=request.json['organ_to_dfr']
        disease_to_dfr=request.json['disease_to_dfr']
        temp_limit=request.json['page_size']
        temp_offset=(request.json['page_size']*request.json['page_current'])        

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

        my_RootDistanceQuery=RootDistanceQuery()
        my_RootDistanceQuery.build_node_search_part_1_from(
            species_from_dfr,
            organ_from_dfr,
            disease_from_dfr,
            species_from_dfr,
            organ_from_dfr,
            disease_from_dfr
        )
        my_RootDistanceQuery.build_node_search_part_1_to(
            species_to_dfr,
            organ_to_dfr,
            disease_to_dfr,
            species_to_dfr,
            organ_to_dfr,
            disease_to_dfr
        )
        my_RootDistanceQuery.build_node_search_part_2()
        my_RootDistanceQuery.build_node_search_part_3(
            compound_dfr,
            compound_path
        )
        my_RootDistanceQuery.build_node_search_part_4()

        # #we delete previously existing views here so that we can execute whatever
        # my_RootDistanceQuery.build_delete_views()

        connection=my_engine.connect()
        connection.execute(
            my_RootDistanceQuery.build_node_search_part_1_from
        )
        connection.execute(
            my_RootDistanceQuery.build_node_search_part_1_to
        )
        connection.execute(
            my_RootDistanceQuery.build_node_search_part_2
        )
        connection.execute(
            my_RootDistanceQuery.build_node_search_part_3
        )
        connection.execute(
            my_RootDistanceQuery.build_node_search_part_4
        )
        connection.execute(
            my_RootDistanceQuery.build_node_search_part_5
        )
        temp_cursor=connection.execute(
            f'''
            select * from node_search_part_5
            limit {temp_limit} offset {temp_offset}
            '''
        )

        if (temp_cursor.rowcount <= 0):
            connection.close()
            #https://stackoverflow.com/questions/8645250/how-to-close-sqlalchemy-connection-in-mysql
            my_engine.dispose()
            print('row count of final result cursor less than 1')
            return 'fail'
        else:
            temp_result=json.dumps([dict(r) for r in temp_cursor])
            connection.close()
            #https://stackoverflow.com/questions/8645250/how-to-close-sqlalchemy-connection-in-mysql
            my_engine.dispose()
            return temp_result        