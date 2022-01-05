#https://www.youtube.com/watch?v=GMppyAPbLYk
from pprint import pprint
from flask import Flask,request
from flask_restful import Api, Resource, reqparse
import json

from ComplicatedQuery import *
from BasicTableQuery import *

from sqlalchemy import create_engine
from sqlalchemy import Table, String
from sqlalchemy.dialects import postgresql

app=Flask(__name__)
api=Api(app)



# my_server='localhost'
# my_database='binvestigate_first'
# my_dialect='postgresql'
# my_driver='psycopg2'
# my_username='rictuar'
# my_password='elaine123'
# my_connection=f'{my_dialect}+{my_driver}://{my_username}:{my_password}@{my_server}/{my_database}'
# my_engine=create_engine(my_connection)#,echo=True)
#connection=engine.connect()
my_server='fold-result-database.czbab8f7pgfj.us-east-2.rds.amazonaws.com:5430'
my_database='foldresults'
my_dialect='postgresql'
my_driver='psycopg2'
my_username='postgres'
my_password='elaine123'
my_connection=f'{my_dialect}+{my_driver}://{my_username}:{my_password}@{my_server}/{my_database}'
my_engine=create_engine(my_connection)#,echo=True)


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
        print(request.json['store_compound'])
        # print(request.json['from_organ'])
        # print(request.json['from_organ'])
        # print('----------------------------------------------------')
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

        # my_ComplicatedQuery=ComplicatedQuery()
        # my_ComplicatedQuery.assign_rest_api_net_query(request.json)
        # my_ComplicatedQuery.assign_from_to_metadata()
        # my_ComplicatedQuery.create_from_to_query_string()
        # print(my_ComplicatedQuery.full_query_string)

        temp_BasicTableQuery=BasicTableQuery()
        temp_BasicTableQuery.build_view_1(
            request.json['store_from_species']['species'],
            request.json['store_from_organ']['organ'],
            request.json['store_from_disease']['disease'],
            request.json['store_to_species']['species'],
            request.json['store_to_organ']['organ'],
            request.json['store_to_disease']['disease'],
            request.json['store_compound']['compounds'],
            request.json['store_result']['page_size'],
            (request.json['store_result']['page_size']*request.json['store_result']['page_current'])
        )
        #build_view_1(temp_from_species,temp_from_organ,temp_from_disease,temp_to_species,temp_to_organ,temp_to_disease,temp_compound)

        temp_BasicTableQuery.build_view_2()

        temp_BasicTableQuery.build_view_3(
            request.json['store_additional']['fold_change_input'],
            request.json['store_additional']['min_triplet_input'],
            request.json['store_additional']['min_count_input'],
            request.json['store_additional']['total_count_input'],
            request.json['store_additional']['max_root_dist_species'],
            request.json['store_additional']['min_leaf_dist_species'],
            request.json['store_additional']['max_root_dist_organs'],
            request.json['store_additional']['min_leaf_dist_organs'],
            request.json['store_additional']['max_root_dist_diseases'],
            request.json['store_additional']['min_leaf_dist_diseases'],
            request.json['store_additional']['max_root_dist_compounds'],
            request.json['store_additional']['min_leaf_dist_compounds'],         
        )

        temp_BasicTableQuery.build_delete_views()
        # temp_cursor=connection.execute(
        #     my_ComplicatedQuery.full_query_string
        # )

        # print(temp_cursor)

        print(temp_BasicTableQuery.view_1)
        print('--------------------------------------------------------------')
        print(temp_BasicTableQuery.view_2)
        print('--------------------------------------------------------------')
        print(temp_BasicTableQuery.view_3)
        print('--------------------------------------------------------------')





        connection=my_engine.connect()
        connection.execute(
            temp_BasicTableQuery.view_1
        )
        connection.execute(
            temp_BasicTableQuery.view_2
        )
        connection.execute(
            temp_BasicTableQuery.view_3
        )
        temp_cursor=connection.execute(
            '''
            select * from temp_view_3;
            '''
        )
        # connection.execute(
        #     temp_BasicTableQuery.delete_views
        # )

        # pprint(vars(temp_cursor))
        # print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')        
        print(temp_cursor.rowcount)
        # print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        # print([r for r in temp_cursor])
        # print('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')

        # if len(temp_cursor.fetchall()) == 0:
        #     print('row count of final result cursor less than 1')
        #     return 'fail'
        # else:
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



api.add_resource(FoldChangeTable,'/foldchangetable/')

if __name__ == '__main__':
    app.run(debug=True)