import networkx as nx
import pathlib
from pprint import pprint

from rootdistancequery import RootDistanceQuery

from sqlalchemy import create_engine

from flask import Flask,request
from flask_restful import Api, Resource, reqparse
import json

PATH = pathlib.Path(__file__).parent
DATA_PATH = PATH.joinpath("../datasets").resolve()

# my_server='localhost'
# my_database='binvestigate_first'
# my_dialect='postgresql'
# my_driver='psycopg2'
# my_username='rictuar'
# my_password='elaine123'
# my_connection=f'{my_dialect}+{my_driver}://{my_username}:{my_password}@{my_server}/{my_database}'
# my_engine=create_engine(my_connection)

my_server='fold-result-database.czbab8f7pgfj.us-east-2.rds.amazonaws.com:5430'
my_database='foldresults'
my_dialect='postgresql'
my_driver='psycopg2'
my_username='postgres'
my_password='elaine123'
my_connection=f'{my_dialect}+{my_driver}://{my_username}:{my_password}@{my_server}/{my_database}'
my_engine=create_engine(my_connection)#,echo=True)


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

        #################33
        print('##############################')
        pprint(request.json)

        compound=request.json['compound']
        from_species=request.json['from_species']
        from_organ=request.json['from_organ']
        from_disease=request.json['from_disease']
        to_species=request.json['to_species']
        to_organ=request.json['to_organ']
        to_disease=request.json['to_disease']
        # compound_dfr=request.json['compound_dfr']
        # species_from_dfr=request.json['species_from_dfr']
        # organ_from_dfr=request.json['organ_from_dfr']
        # disease_from_dfr=request.json['disease_from_dfr']
        # species_to_dfr=request.json['species_to_dfr']
        # organ_to_dfr=request.json['organ_to_dfr']
        # disease_to_dfr=request.json['disease_to_dfr']
        # temp_limit=request.json['page_size']
        # temp_offset=(request.json['page_size']*request.json['page_current'])        
        page_current=request.json['page_current']
        page_size=request.json['page_size']
        sort_by=request.json['sort_by']
        filter_query=request.json['filter_query']

        compound_networkx_path=DATA_PATH.joinpath("compounds_networkx.bin")
        compound_networkx=nx.readwrite.gpickle.read_gpickle(compound_networkx_path)
        species_networkx_path=DATA_PATH.joinpath("species_networkx.bin")
        species_networkx=nx.readwrite.gpickle.read_gpickle(species_networkx_path)
        organ_networkx_path=DATA_PATH.joinpath("organ_networkx.bin")
        organ_networkx=nx.readwrite.gpickle.read_gpickle(organ_networkx_path)
        disease_networkx_path=DATA_PATH.joinpath("disease_networkx.bin")
        disease_networkx=nx.readwrite.gpickle.read_gpickle(disease_networkx_path)

        if compound != None:
            compound_path=nx.ancestors(compound_networkx,int(compound)).union({str(compound)})
        else:
            compound_path=[str(temp_node) for temp_node in compound_networkx.nodes]

        if (from_species != None) and (to_species == None):
            species_path_from=nx.ancestors(species_networkx,from_species)
            species_path_from.add(from_species)
            species_path_to=species_networkx.nodes
        elif (from_species == None) and (to_species != None):
            species_path_from=species_networkx.nodes
            species_path_to=nx.ancestors(species_networkx,to_species)
            species_path_to.add(to_species)
        elif (from_species != None) and (to_species != None):
            lowest_parent=nx.lowest_common_ancestor(species_networkx,from_species,to_species)
            species_path_from=nx.algorithms.shortest_path(species_networkx,lowest_parent,from_species)
            species_path_to=nx.algorithms.shortest_path(species_networkx,lowest_parent,to_species)
        elif (from_species == None) and (to_species == None):
            species_path_from=species_networkx.nodes
            species_path_to=species_networkx.nodes

        if (from_organ != None) and (to_organ == None):
            organ_path_from=nx.ancestors(organ_networkx,from_organ)
            organ_path_from.add(from_organ)
            organ_path_to=organ_networkx.nodes
        elif (from_organ == None) and (to_organ != None):
            organ_path_from=organ_networkx.nodes
            organ_path_to=nx.ancestors(organ_networkx,to_organ)
            organ_path_to.add(to_organ)
        elif (from_organ != None) and (to_organ != None):
            lowest_parent=nx.lowest_common_ancestor(organ_networkx,from_organ,to_organ)
            organ_path_from=nx.algorithms.shortest_path(organ_networkx,lowest_parent,from_organ)
            organ_path_to=nx.algorithms.shortest_path(organ_networkx,lowest_parent,to_organ)
        elif (from_organ == None) and (to_organ == None):
            organ_path_from=organ_networkx.nodes
            organ_path_to=organ_networkx.nodes

        if (from_disease != None) and (to_disease == None):
            disease_path_from=nx.ancestors(disease_networkx,from_disease)
            disease_path_from.add(from_disease)
            disease_path_to=disease_networkx.nodes
        elif (from_disease == None) and (to_disease != None):
            disease_path_from=disease_networkx.nodes
            disease_path_to=nx.ancestors(disease_networkx,to_disease)
            disease_path_to.add(to_disease)
        elif (from_disease != None) and (to_disease != None):
            lowest_parent=nx.lowest_common_ancestor(disease_networkx,from_disease,to_disease)
            disease_path_from=nx.algorithms.shortest_path(disease_networkx,lowest_parent,from_disease)
            disease_path_to=nx.algorithms.shortest_path(disease_networkx,lowest_parent,to_disease)
        elif (from_disease == None) and (to_disease == None):
            disease_path_from=disease_networkx.nodes
            disease_path_to=disease_networkx.nodes

        print('~~~~~~~~~~~~~~~~')
        # print(to_species)
        print(species_path_from)
        # print(from_organ)
        # print(organ_path_from)
        compound_path=list(compound_path)
        species_path_from=list(species_path_from)
        species_path_to=list(species_path_to)
        organ_path_from=list(organ_path_from)
        organ_path_to=list(organ_path_to)
        disease_path_from=list(disease_path_from)
        disease_path_to=list(disease_path_to)

        # print(compound_path)
        # print('$$$$$$$$$$$$$$$$$$$$$$$$$$')

        my_RootDistanceQuery=RootDistanceQuery()
        my_RootDistanceQuery.build_node_search_part_1_from(
            # species_from_dfr,
            # organ_from_dfr,
            # disease_from_dfr,
            species_path_from,
            organ_path_from,
            disease_path_from
        )
        my_RootDistanceQuery.build_node_search_part_1_to(
            # species_to_dfr,
            # organ_to_dfr,
            # disease_to_dfr,
            species_path_to,
            organ_path_to,
            disease_path_to
        )
        my_RootDistanceQuery.build_node_search_part_2()
        my_RootDistanceQuery.build_node_search_part_3(
            # compound_dfr,
            compound_path
        )
        my_RootDistanceQuery.build_node_search_part_4()
        my_RootDistanceQuery.build_node_search_part_5()
        my_RootDistanceQuery.build_node_search_part_6(
            page_current,
            page_size,
            sort_by,
            filter_query
        )
        #we delete previously existing views here so that we can execute whatever
        # my_RootDistanceQuery.build_delete_views()
        
        connection=my_engine.connect()
        # connection.execute(
        #     my_RootDistanceQuery.string_delete_views
        # )
        connection.execute(
            my_RootDistanceQuery.string_node_search_part_1_from
        )
        connection.execute(
            my_RootDistanceQuery.string_node_search_part_1_to
        )
        connection.execute(
            my_RootDistanceQuery.string_node_search_part_2
        )
        connection.execute(
            my_RootDistanceQuery.string_node_search_part_3
        )
        connection.execute(
            my_RootDistanceQuery.string_node_search_part_4
        )
        connection.execute(
            my_RootDistanceQuery.string_node_search_part_5
        )
        temp_cursor=connection.execute(
            my_RootDistanceQuery.string_node_search_part_6
        )
        # temp_cursor=connection.execute(
        #     f'''
        #     select * from node_search_part_6
        #     limit {temp_limit} offset {temp_offset}
        #     '''
        # )
        # connection.execute(
        #     my_RootDistanceQuery.string_delete_views
        # )

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