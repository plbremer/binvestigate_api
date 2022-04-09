import networkx as nx

class LeafDistanceResource(Resource):

    def post(self):
        '''
        step 1
        use networkx to determine what parent nodes to include
        step 2 
        modify the query string as necessary
        step 3
        execute the query, return results
        '''

        compound_networkx=