from compoundquery import CompoundQuery

from sqlalchemy import create_engine

from flask import Flask,request
from flask_restful import Api, Resource, reqparse
import json

my_server='localhost'
my_database='binvestigate_first'
my_dialect='postgresql'
my_driver='psycopg2'
my_username='rictuar'
my_password='elaine123'
my_connection=f'{my_dialect}+{my_driver}://{my_username}:{my_password}@{my_server}/{my_database}'
my_engine=create_engine(my_connection)

class CompoundResource(Resource):

    def post(self):
        '''
        '''
        compound=request.json['compound']
        from_species=request.json['from_species']
        from_organ=request.json['from_organ']
        from_disease=request.json['from_disease']
        to_species=request.json['to_species']
        to_organ=request.json['to_organ']
        to_disease=request.json['to_disease']
        page_current=request.json['page_current']
        page_size=request.json['page_size']
        sort_by=request.json['sort_by']
        filter_query=request.json['filter_query']

        print(request.json)

        my_CompoundQuery=CompoundQuery()
        my_CompoundQuery.build_query_1(
            compound,
            from_species,
            from_organ,
            from_disease,
            to_species,
            to_organ,
            to_disease
        )
        my_CompoundQuery.build_query_2(
            page_current,
            page_size,
            sort_by,
            filter_query
        )
        # my_CompoundQuery.build_delete_views()

        #my_CompoundQuery.build_query_

        #print(my_CompoundQuery.query)

        connection=my_engine.connect()
        connection.execute(
            my_CompoundQuery.query_1
        )
        temp_cursor=connection.execute(
            my_CompoundQuery.query_2
        )
        # connection.execute(
        #     my_CompoundQuery.string_delete_views
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
