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
        from_species=str(request.json['from_species'])
        from_organ=request.json['from_organ']
        from_disease=request.json['from_disease']
        to_species=str(request.json['to_species'])
        to_organ=request.json['to_organ']
        to_disease=request.json['to_disease']

        my_CompoundQuery=CompoundQuery()
        my_CompoundQuery.build_query(
            compound,
            from_species,
            from_organ,
            from_disease,
            to_species,
            to_organ,
            to_disease
        )

        print(my_CompoundQuery.query)

        connection=my_engine.connect()
        temp_cursor=connection.execute(
            my_CompoundQuery.query
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
