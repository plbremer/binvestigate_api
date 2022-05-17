class CompoundQuery():

    def construct_one_clause(self,one_specification):
        temp_list=one_specification.split(' ')
        if (temp_list[1]=='s>') or (temp_list[1]=='s>=') or (temp_list[1]=='s<') or (temp_list[1]=='s<=') or (temp_list[1]=='s='):
            temp_where_clause='('+temp_list[0][1:-1]+' '+temp_list[1][1:]+' '+temp_list[2]+')'
            return temp_where_clause
        elif (temp_list[1]=='scontains'):
            temp_where_clause='('+temp_list[0][1:-1]+' like \'%%'+temp_list[2]+'%%\')'
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
        for where_clause in where_clauses:
            where_clause_string=where_clause_string+where_clause+' and '
        where_clause_string=where_clause_string[5:-5]
        where_clause_string=' where '+where_clause_string
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
        compound,
        species_from,
        organ_from,
        disease_from,
        species_to,
        organ_to,
        disease_to
    ):

        print('^^^^^^^^^^^^^^^')
        #print(species_from)
        print(species_to)

        if species_from==None:
            join_type_species_from='left'
        else:
            join_type_species_from='inner'
        if organ_from==None:
            join_type_organ_from='left'
        else:
            join_type_organ_from='inner'
        
        if disease_from==None:
            join_type_disease_from='left'
        else:
            join_type_disease_from='inner'
        
        if species_to==None:
            join_type_species_to='left'
        else:
            join_type_species_to='inner'
        
        if organ_to==None:
            join_type_organ_to='left'
        else:
            join_type_organ_to='inner'
        if disease_to==None:
            join_type_disease_to='left'
        else:
            join_type_disease_to='inner'
        
        print(join_type_species_to)
        print(join_type_organ_to)
        
        self.query_1=f'''
            create temporary view temp_1 as
            select 
            species_headnode_from,
            organ_headnode_from,
            disease_headnode_from,
            species_headnode_to,
            organ_headnode_to,
            disease_headnode_to,
            compound,
            fold_average,
            fold_median,
            sig_mannwhit,
            sig_welch
            from (
                select
                species_headnode_from,
                organ_headnode_from,
                disease_headnode_from,
                species_headnode_to,
                organ_headnode_to,
                disease_headnode_to,
                from_triplets_inter_removed_if_nec,
                to_triplets_inter_removed_if_nec
                from(
                    select
                    species_headnode_from,
                    organ_headnode_from,
                    disease_headnode_from,
                    species_headnode_to,
                    organ_headnode_to,
                    disease_headnode_to,
                    from_triplets_inter_removed_if_nec,
                    to_triplets_inter_removed_if_nec
                    from(
                        select 
                        species_headnode_from,
                        organ_headnode_from,
                        disease_headnode_from,
                        species_headnode_to,
                        organ_headnode_to,
                        disease_headnode_to,
                        from_triplets_inter_removed_if_nec,
                        to_triplets_inter_removed_if_nec
                        from (
                            select
                            species_headnode_from,
                            organ_headnode_from,
                            disease_headnode_from,
                            species_headnode_to,
                            organ_headnode_to,
                            disease_headnode_to,
                            from_triplets_inter_removed_if_nec,
                            to_triplets_inter_removed_if_nec
                            from(
                                select
                                species_headnode_from,
                                organ_headnode_from,
                                disease_headnode_from,
                                species_headnode_to,
                                organ_headnode_to,
                                disease_headnode_to,
                                from_triplets_inter_removed_if_nec,
                                to_triplets_inter_removed_if_nec
                                from(
                                    select 
                                    species_headnode_from,
                                    organ_headnode_from,
                                    disease_headnode_from,
                                    species_headnode_to,
                                    organ_headnode_to,
                                    disease_headnode_to,
                                    from_triplets_inter_removed_if_nec,
                                    to_triplets_inter_removed_if_nec
                                    from(
                                        select 
                                        *
                                        from 
                                        headnode_pairs_to_triplet_list_pair hpttlp 
                                        where 
                                        basic_vs_basic = True
                                    ) foo_1
                                    {join_type_species_from} join 
                                    unnest(array['{species_from}'])
                                    on
                                    "unnest"=species_headnode_from
                                ) foo_2
                                {join_type_organ_from} join 
                                unnest(array['{organ_from}'])
                                on
                                "unnest"=organ_headnode_from
                            ) foo_3
                            {join_type_disease_from} join 
                            unnest(array['{disease_from}'])
                            on
                            "unnest"=disease_headnode_from 
                        ) foo_4
                        {join_type_species_to} join 
                        unnest(array['{species_to}'])
                        on
                        "unnest"=species_headnode_to
                    ) foo_5
                    {join_type_organ_to} join 
                    unnest(array['{organ_to}'])
                    on
                    "unnest"=organ_headnode_to
                ) foo_6
                {join_type_disease_to} join 
                unnest(array['{disease_to}'])
                on
                "unnest"=disease_headnode_to 
            ) foo_7
            inner join 
            combined_results cr 
            on
            (foo_7.from_triplets_inter_removed_if_nec=cr.from_triplets) and
            (foo_7.to_triplets_inter_removed_if_nec=cr.to_triplets) and
            (cr.compound='{compound}')        
        '''

    def build_query_2(
        self,
        page_current,
        page_size,
        sort_by,
        filter_query
    ):
        
        #total_compound_string=self.construct_compound_where(include_classes,include_knowns,include_unknowns)
        where_string=self.construct_filter_where(filter_query)
        order_by_string=self.construct_order_by(sort_by)
        pagination_string=self.construct_pagination(page_current,page_size)        
        
        self.query_2=f'''
        select 
        species_from,
        organ_from,
        disease_from,
        species_to,
        organ_to,
        disease_to,
        cp.english_name as compound,
        fold_average,
        fold_median,
        sig_mannwhit,
        sig_welch
        from (
            select 
            species_from,
            organ_from,
            disease_from,
            species_to,
            organ_to,
            dp2.english_name as disease_to,
            compound,
            fold_average,
            fold_median,
            sig_mannwhit,
            sig_welch
            from (
                select 
                species_from,
                organ_from,
                disease_from,
                species_to,
                op2.english_name as organ_to,
                disease_headnode_to,
                compound,
                fold_average,
                fold_median,
                sig_mannwhit,
                sig_welch
                from (
                    select 
                    species_from,
                    organ_from,
                    disease_from,
                    sp2.english_name as species_to,
                    organ_headnode_to,
                    disease_headnode_to,
                    compound,
                    fold_average,
                    fold_median,
                    sig_mannwhit,
                    sig_welch
                    from (
                        select 
                        species_from,
                        organ_from,
                        dp.english_name as disease_from,
                        species_headnode_to,
                        organ_headnode_to,
                        disease_headnode_to,
                        compound,
                        fold_average,
                        fold_median,
                        sig_mannwhit,
                        sig_welch
                        from (
                            select 
                            species_from,
                            op.english_name as organ_from,
                            disease_headnode_from,
                            species_headnode_to,
                            organ_headnode_to,
                            disease_headnode_to,
                            compound,
                            fold_average,
                            fold_median,
                            sig_mannwhit,
                            sig_welch
                            from (
                                select
                                sp.english_name as species_from,
                                organ_headnode_from,
                                disease_headnode_from,
                                species_headnode_to,
                                organ_headnode_to,
                                disease_headnode_to,
                                compound,
                                fold_average,
                                fold_median,
                                sig_mannwhit,
                                sig_welch
                                from temp_1
                                inner join
                                species_properties sp 
                                on
                                species_headnode_from = sp.identifier 
                            ) foo_1
                            inner join
                            organ_properties as op
                            on 
                            foo_1.organ_headnode_from=op.identifier 
                            ) foo_2
                        inner join 
                        disease_properties as dp 
                        on
                        foo_2.disease_headnode_from=dp.identifier
                    ) foo_3
                    inner join 
                    species_properties sp2 
                    on
                    foo_3.species_headnode_to=sp2.identifier
                    ) foo_4
                inner join 
                organ_properties op2
                on
                foo_4.organ_headnode_to=op2.identifier
                ) foo_5
            inner join 
            disease_properties dp2 
            on
            foo_5.disease_headnode_to=dp2.identifier 
            ) foo_6
        inner join 
        compound_properties cp 
        on
        foo_6.compound=cp.identifier 
        {where_string} {order_by_string} {pagination_string}
        '''
        print(self.query_2)

    # def build_delete_views(self):

    #     self.string_delete_views=f'''
    #     drop view temp_1 cascade;
    #     '''