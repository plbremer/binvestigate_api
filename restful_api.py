#https://www.youtube.com/watch?v=GMppyAPbLYk
from pprint import pprint
from flask import Flask,request
from flask_restful import Api, Resource, reqparse
import json

from ComplicatedQuery import *

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
engine=create_engine(my_connection)#,echo=True)
connection=engine.connect()



# def list_of_list_parser(temp_list_of_lists):
#     #if (type(temp_list_of_lists) != list) and :
#     #    raise
#     print(temp_list_of_lists)
#     print('----------------------------------------------')


# FoldChangeTable_put_args=reqparse.RequestParser()
# FoldChangeTable_put_args.add_argument('from_species',type=str, help= 'put help text here',required=True)
# FoldChangeTable_put_args.add_argument('from_organ',type=list, help= 'put help text here',required=True)
# FoldChangeTable_put_args.add_argument('from_disease',type=list, help= 'put help text here',required=True)
# FoldChangeTable_put_args.add_argument('to_species',type=list, help= 'put help text here',required=True)
# FoldChangeTable_put_args.add_argument('to_organ',type=list, help= 'put help text here',required=True)
# FoldChangeTable_put_args.add_argument('to_disease',type=list, help= 'put help text here',required=True)
# FoldChangeTable_put_args.add_argument('additional_slider_min_fold_change',type=list, help= 'put help text here',required=True)


class FoldChangeTable(Resource):

    def post(self):
        #pprint(Resource.args)
        #pprint(vars(self))
        #pprint(vars(request))
        pprint(request.json)
        print(request.json['from_organ'])
        print(request.json['from_organ'])
        print('----------------------------------------------------')
        # pprint(request.form.to_dict())        
        # print('----------------------------------------------------')
        # pprint(request.form.to_dict(flat=False))
        # print('-------------------------------------------------------')
        #print(request.form.getlist('name[]'))
        #get the information from the put
        #args=FoldChangeTable_put_args.parse_args()
        #print(args)
        #run some checks on that information to make sure that its legitimate
            #set of if statements and abort messages
        #return error messages if its bad
        #otherwise get a query string from the ComplicatedQuery class
        #run that query on the database
        #return the entire thing (next step add pagination)

        my_ComplicatedQuery=ComplicatedQuery()
        my_ComplicatedQuery.assign_rest_api_net_query(request.json)
        my_ComplicatedQuery.assign_from_to_metadata()
        my_ComplicatedQuery.create_from_to_query_string()
        print(my_ComplicatedQuery.full_query_string)


        temp_cursor=connection.execute(
            my_ComplicatedQuery.full_query_string
        )

        print(temp_cursor)



api.add_resource(FoldChangeTable,'/foldchangetable/')

if __name__ == '__main__':
    app.run(debug=True)