from pprint import pprint
from flask import Flask,request
from flask_restful import Api, Resource, reqparse
import json

from sqlalchemy import create_engine
from sqlalchemy import Table, String
from sqlalchemy.dialects import postgresql

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


class LeafQuery(Resource):

    def post(self):
        print(request.json['from_species'])


api.add_resource(LeafQuery,'/leafquery/')

if __name__ == '__main__':
    app.run(debug=True)

