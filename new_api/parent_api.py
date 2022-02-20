from pprint import pprint
from flask import Flask,request
from flask_restful import Api, Resource, reqparse
import json

from sqlalchemy import create_engine
from sqlalchemy import Table, String
from sqlalchemy.dialects import postgresql

from leafquery import LeafQuery

app=Flask(__name__)
api=Api(app)

my_server='localhost'
my_database='binvestigate_first'
my_dialect='postgresql'
my_driver='psycopg2'
my_username='rictuar'
my_password='elaine123'
my_connection=f'{my_dialect}+{my_driver}://{my_username}:{my_password}@{my_server}/{my_database}'
my_engine=create_engine(my_connection)


class LeafResource(Resource):

    def post(self):
        '''
        should check if each species,organ,disease headnode in headnodes_to_triplets
        should verify types...
        should verify valid ranges (like p value between 0 and 1).....
        '''
        #print(request.json['from_species'])
        # "from_species":562,
        # "from_organ":"D27.720.470.305",
        # "from_disease":"No",
        # "to_species":33554,
        # "to_organ":"A12.207.152.846",
        # "to_disease":"No",
        # "include_known":"Yes",
        # "include_unknown":"Yes",
        # "fold_median_min":0,
        # "fold_average_min":0,
        # "p_welch_max":1,
        # "p_mann_max":1
        temp_LeafQuery=LeafQuery(
            request.json['from_species'],
            request.json['from_organ'],
            request.json['from_disease'],
            request.json['to_species'],
            request.json['to_organ'],
            request.json['to_disease'],
            request.json['include_known'],
            request.json['include_unknown'],
            request.json['fold_median_min'],
            request.json['fold_average_min'],
            request.json['p_welch_max'],
            request.json['p_welch_max']
        )
        print(temp_LeafQuery.query)
        

        #actually make the conneciton and the call
        connection=my_engine.connect()
        temp_cursor=connection.execute(temp_LeafQuery.query)

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


api.add_resource(LeafResource,'/leafresource/')

if __name__ == '__main__':
    app.run(debug=True)

