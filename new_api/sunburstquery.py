class SunburstQuery():

    def construct_one_clause(self,one_specification):
        #this worked when there werent spaces in the names of the clumns
        #temp_list=one_specification.split(' ')
        temp_list=['','','']
        temp_list[0]=one_specification[one_specification.find('{'):one_specification.find('}')+1]
        temp_list[1]=one_specification.split(' ')[-2]
        temp_list[2]=one_specification.split(' ')[-1]
        if (temp_list[1]=='s>') or (temp_list[1]=='s>=') or (temp_list[1]=='s<') or (temp_list[1]=='s<=') or (temp_list[1]=='s='):
            temp_where_clause='(\"'+temp_list[0][1:-1]+'\" '+temp_list[1][1:]+' '+temp_list[2]+')'
            return temp_where_clause
        elif (temp_list[1]=='scontains'):
            temp_where_clause='(\"'+temp_list[0][1:-1]+'\" like \'%%'+temp_list[2]+'%%\')'
            print(temp_where_clause)
            return temp_where_clause

    def construct_filter_where(self,filter_string):
        '''
        unfortunately, dash doesnt have have a parser that allows for multiple conditions build in
        so we had to write our own
        If we have a single condition (the filter description doesnt have a ")
        Then we go to the simple parsing function
        If we do have multiple conditions, then we do a lot of coercing to get our condition
        to read like multiple other conditions
        '''
        print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~``')
        print(filter_string)
        if len(filter_string)==0:
            return ''
        filter_list=filter_string.split(' && ')
        where_clauses=list()
        for filter_statement in filter_list:
            if ('"' not in filter_statement):
                where_clauses.append(self.construct_one_clause(filter_statement))
            elif ('\"' in filter_statement):
                filter_statement=filter_statement.replace('"','')
                filter_statement=filter_statement.replace('<','< ')
                filter_statement=filter_statement.replace('<=','<= ')
                filter_statement=filter_statement.replace('>','> ')
                filter_statement=filter_statement.replace('>=','>= ')
                filter_statement=filter_statement.replace('=','= ')
                filter_statement=filter_statement.replace('  ',' ')
                filter_statement=filter_statement.replace(' <',' s<')
                filter_statement=filter_statement.replace(' <=',' s<=')
                filter_statement=filter_statement.replace(' >',' s>')
                filter_statement=filter_statement.replace(' >=',' s>=')
                filter_statement=filter_statement.replace(' =',' s=')
                sublist=filter_statement.split(' ')
                
                if (sublist[1]=='s>') or (sublist[1]=='s>=') or (sublist[1]=='s<') or (sublist[1]=='s<=') or (sublist[1]=='s='):
                    sub_sublist=list()
                    connector_sublist=list()
                    for i in range(1,len(sublist),3):
                        try:
                            connector_sublist.append(sublist[i+2])
                        except IndexError:
                            connector_sublist.append('')
                            
                        sub_sublist.append(sublist[0]+' '+sublist[i]+' '+sublist[i+1])
                    substring='('
                    for i, item in enumerate(sub_sublist):
                        substring=substring+self.construct_one_clause(item)+' '+connector_sublist[i]+' '
                    substring=substring+')'
                elif sublist[1]=='scontains':
                    sub_sublist=list()
                    connector_sublist=list()
                    for i in range(2,len(sublist),2):
                        try:
                            connector_sublist.append(sublist[i+1])
                        except IndexError:
                            connector_sublist.append('')
                        sub_sublist.append(sublist[0]+' '+sublist[1]+' '+sublist[i])
                    substring='('
                    for i, item in enumerate(sub_sublist):
                        substring=substring+self.construct_one_clause(item)+' '+connector_sublist[i]+' '
                    substring=substring+')'     
                where_clauses.append(substring)
        where_clause_string=' and '
        print(where_clauses)
        for where_clause in where_clauses:
            where_clause_string=where_clause_string+where_clause+' and '
        where_clause_string=where_clause_string[5:-5]
        #where_clause_string=' where '+where_clause_string
        return where_clause_string

    def construct_order_by(self,order_list):
        #[{'column_id': 'english_name', 'direction': 'asc'}, {'column_id': 'fold_average', 'direction': 'asc'}]
        if len(order_list)==0:
            return ''
        total_string='order by\n'
        for temp_dict in order_list:
            total_string = total_string+temp_dict['column_id']+' '+temp_dict['direction']+',\n'
        total_string=total_string[:-2]
        return total_string

    # def construct_compound_where(self,classes,knowns,unknowns):
    #     #nothing. basically for now we just cause query fail
    #     if (classes=='No') and (knowns=='No') and (unknowns=='No'):
    #         return 'error'
    #     #classes only
    #     elif (classes=='Yes') and (knowns=='No') and (unknowns=='No'):
    #         return 'where (oq.compound !~ \'^[0-9\.]+$\') '
    #     #all compounds only
    #     elif (classes=='No') and (knowns=='Yes') and (unknowns=='Yes'):
    #         return 'where (oq.compound ~ \'^[0-9\.]+$\') '
    #     #classes and knowns
    #     elif (classes=='Yes') and (knowns=='Yes') and (unknowns=='No'):
    #         return 'where (cp.english_name !~ \'^[0-9\.]+$\') '
    #     #classes and unknowns
    #     elif (classes=='Yes') and (knowns=='Yes') and (unknowns=='No'):
    #         return 'where (cp.english_name ~ \'^[0-9\.]+$\') or (oq.compound !~ \'^[0-9\.]+$\') '
    #     #all unknowns only
    #     elif (classes=='No') and (knowns=='No') and (unknowns=='Yes'):
    #         return 'where (cp.english_name ~ \'^[0-9\.]+$\') '
    #     #all knowns only
    #     elif (classes=='No') and (knowns=='Yes') and (unknowns=='No'):
    #         return 'where (cp.english_name !~ \'^[0-9\.]+$\') and (oq.compound ~ \'^[0-9\.]+$\') '
    #     #everything
    #     elif (classes=='Yes') and (knowns=='Yes') and (unknowns=='Yes'):
    #         return ''

    def construct_pagination(self, current_page,page_size):
        temp_offset=page_size*current_page
        temp_limit=page_size
        pagination_string=f'limit {temp_limit} offset {temp_offset}'
        return pagination_string


    def init():
        pass

    def build_query_1(
        self,
        #page_current,
        #page_size,
        #sort_by,
        #filter_query,
        compound
    ):
    
        # sod_parallel_list=[element.split(' - ') for element in dropdown_triplet_selection_value]
        # if toggle_average_true_value==True:
        #     intensity_type='intensity_average'
        # elif toggle_average_true_value==False:
        #     intensity_type='intensity_median'
        # slider_percent_present_value*=0.01

    
        # main_line_list=[
        #     f'case when (max(percent_present) filter (where "species"=\'{sod_parallel_list[i][0]}\' and "organ"=\'{sod_parallel_list[i][1]}\' and "disease"=\'{sod_parallel_list[i][2]}\')>{slider_percent_present_value}) then (max({intensity_type}) filter (where "species"=\'{sod_parallel_list[i][0]}\' and "organ"=\'{sod_parallel_list[i][1]}\' and "disease"=\'{sod_parallel_list[i][2]}\')) else null end as "{dropdown_triplet_selection_value[i]}",\n' for i in range(len(dropdown_triplet_selection_value))
        #     #f'max({intensity_type}) filter (where "species"=\'{sod_parallel_list[i][0]}\' and "organ"=\'{sod_parallel_list[i][1]}\' and "disease"=\'{sod_parallel_list[i][2]}\') as "{dropdown_triplet_selection_value[i]}",\n' for i in range(len(dropdown_triplet_selection_value))
        # ]

        # main_line=''.join(main_line_list)
        # main_line=main_line[:-2]+'\n'
        # for  i in range(len(main_line_list)):
        #     main_line+=main_line_list[i]

        self.query_1=f'''
            select 
            species,
            organ,
            disease,
            max(intensity_average) filter (where "bin"={compound}) as intensity_average,
            max(intensity_median) filter (where "bin"={compound}) as intensity_median,
            max(percent_present) filter (where "bin"={compound}) as percent_present 
            from 
            non_ratio_table nrt 
            group by
            species, organ, disease
            order by 
            species,organ,disease  
        '''
        # select * from (
        # select
        # bin,
        # max(intensity_average) filter (where "species"='Homo Sapiens' and "organ"='Urine' and "disease"='No') as "Homo_Sapiens-Urine-No",
        # max(intensity_average) filter (where "species"='Homo Sapiens' and "organ"='Plasma' and "disease"='No') as "Homo_Sapiens-Plasma-No"
        # from
        # non_ratio_table nrt
        # group by
        # bin
        # order by
        # bin
        # ) as foo
        # where
        # "Homo_Sapiens-Urine-No">0.04

        print(self.query_1)

    # def build_query_2(
    #     self,
    #     page_current,
    #     page_size,
    #     sort_by,
    #     filter_query,
    #     radio_items_filter_value,
    #     dropdown_triplet_selection_value
    # ):    

    #     where_string=self.construct_filter_where(filter_query)
    #     order_by_string=self.construct_order_by(sort_by)
    #     pagination_string=self.construct_pagination(page_current,page_size)          

    #     if radio_items_filter_value=='no_filter':
    #         radio_filter_string=''
    #     elif radio_items_filter_value=='common':
    #         radio_filter_string_list=[
    #             f'(\"{element}\" is not null) ' for element in dropdown_triplet_selection_value
    #         ]
    #         radio_filter_string=' and '.join(radio_filter_string_list)
        
    #     if len(filter_query)!=0 or len(radio_filter_string)!=0:
    #         where_string_beginning='where '
    #     else:
    #         where_string_beginning=''

    #     if len(radio_filter_string)!=0:
    #         where_string_middle=' and '
    #     else:
    #         where_string_middle=''

    #     self.query_2=f'''
    #     select * from temp_venn_1
    #     {where_string_beginning} {where_string} {where_string_middle} {radio_filter_string} {order_by_string} {pagination_string}
    #     '''
    #     print('++++++++++++++++++++++++++++++++++++++++++++++++++')
    #     print(self.query_2)