from sqlalchemy import create_engine

from venntablequery import VennTableQuery

from flask import Flask,request
from flask_restful import Api, Resource, reqparse
import json

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


class VennTableResource(Resource):

    def post(self):
        '''
        '''
        print('********************************************')
        print(request.json)
        page_current=request.json["page_current"]
        page_size=request.json["page_size"]
        sort_by=request.json["sort_by"]
        filter_query=request.json["filter_query"]
        dropdown_triplet_selection_value=request.json["dropdown_triplet_selection_value"]
        slider_percent_present_value=request.json["slider_percent_present_value"]
        toggle_average_true_value=request.json["toggle_average_true_value"]
        radio_items_filter_value=request.json["radio_items_filter_value"]

        # compound=request.json['compound']
        # from_species=request.json['from_species']
        # from_organ=request.json['from_organ']
        # from_disease=request.json['from_disease']
        # to_species=request.json['to_species']
        # to_organ=request.json['to_organ']
        # to_disease=request.json['to_disease']
        # page_current=request.json['page_current']
        # page_size=request.json['page_size']
        # sort_by=request.json['sort_by']
        # filter_query=request.json['filter_query']

        print(request.json)

        my_VennTableQuery=VennTableQuery()
        my_VennTableQuery.build_query_1(
            #page_current,
            #page_size,
            #sort_by,
            #filter_query,
            dropdown_triplet_selection_value,
            slider_percent_present_value,
            toggle_average_true_value
        )
        #print
        my_VennTableQuery.build_query_2(
            page_current,
            page_size,
            sort_by,
            filter_query,
            radio_items_filter_value,
            dropdown_triplet_selection_value
        )
        # # my_CompoundQuery.build_delete_views()

        # #my_CompoundQuery.build_query_

        # #print(my_CompoundQuery.query)

        connection=my_engine.connect()
        connection.execute(
            my_VennTableQuery.query_1
        )
        temp_cursor=connection.execute(
            my_VennTableQuery.query_2
        )
        # # connection.execute(
        # #     my_CompoundQuery.string_delete_views
        # # )

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
            #print(temp_result)
            return temp_result   
