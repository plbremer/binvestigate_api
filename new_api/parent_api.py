from pprint import pprint
from flask import Flask,request
from flask_restful import Api, Resource, reqparse
import json

from sqlalchemy import create_engine
from sqlalchemy import Table, String
from sqlalchemy.dialects import postgresql

from volcanoquery import VolcanoQuery
from metadataquery import MetadataQuery

from rootdistanceresource import RootDistanceResource
from compoundresource import CompoundResource
from venntableresource import VennTableResource
from venndiagramresource import VennDiagramResource
from sunburstresource import SunburstResource

app=Flask(__name__)
api=Api(app)

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


class VolcanoResource(Resource):

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
        #pprint(request.json['column_sort'][0])
        #pprint(request.json['column_filter'])
        pprint(request.json['sort_by'])
        pprint(request.json['filter_query'])
        temp_VolcanoQuery=VolcanoQuery(
            request.json['from_species'],
            request.json['from_organ'],
            request.json['from_disease'],
            request.json['to_species'],
            request.json['to_organ'],
            request.json['to_disease'],
            #request.json['include_bins'],
            request.json['include_classes'],
            request.json['include_knowns'],
            request.json['include_unknowns'],
            request.json['page_current'],
            request.json['page_size'],
            request.json['sort_by'],
            request.json['filter_query']
        )
        pprint(temp_VolcanoQuery.query)
        

        #actually make the conneciton and the call
        connection=my_engine.connect()
        temp_cursor=connection.execute(temp_VolcanoQuery.query)

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


class MetadataResource(Resource):

    def post(self):
        '''
        should check if each species,organ,disease headnode in headnodes_to_triplets
        should verify types...
        '''
        temp_MetadataQuery=MetadataQuery(
            request.json['from_species'],
            request.json['from_organ'],
            request.json['from_disease'],
            request.json['to_species'],
            request.json['to_organ'],
            request.json['to_disease'],
        )
        pprint(temp_MetadataQuery.query)

        #actually make the conneciton and the call
        connection=my_engine.connect()
        temp_cursor=connection.execute(temp_MetadataQuery.query)

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



api.add_resource(VolcanoResource,'/volcanoresource/')
api.add_resource(MetadataResource,'/metadataresource/')
api.add_resource(RootDistanceResource,'/rootdistanceresource/')
api.add_resource(CompoundResource,'/compoundresource/')
api.add_resource(VennTableResource,'/venntableresource/')
api.add_resource(VennDiagramResource,'/venndiagramresource/')
api.add_resource(SunburstResource,'/sunburstresource/')

if __name__ == '__main__':
    app.run(debug=True,port=4999)

