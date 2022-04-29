from pprint import pprint
class RootDistanceQuery():

    def construct_filter_where(self,filter_string):
        print('$$$$$$$$$$$$$$$$$$$$$$$$')
        print(filter_string)
        if len(filter_string)==0:
            return ''
        filter_list=filter_string.split(' && ')
        print(filter_list)
        where_clauses=list()
        for filter_statement in filter_list:
            temp_list=filter_statement.split(' ')
            print(temp_list)
            if (temp_list[1]=='s>') or (temp_list[1]=='s>=') or (temp_list[1]=='s<') or (temp_list[1]=='s<=') or (temp_list[1]=='s='):
                temp_where_clause='('+temp_list[0][1:-1]+' '+temp_list[1][1:]+' '+temp_list[2]+')'
                where_clauses.append(temp_where_clause)
            elif (temp_list[1]=='scontains'):
                temp_where_clause='('+temp_list[0][1:-1]+' like \'%%'+temp_list[2]+'%%\')'
                where_clauses.append(temp_where_clause)
        where_clause_string=' and '
        print(where_clauses)
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

    def build_node_search_part_1_from(
        self,
        # species_distance_from_root,
        # organ_distance_from_root,
        # disease_distance_from_root,
        species_path_from,
        organ_path_from,
        disease_path_from
    ):
        '''
        explanation of the left/inner join option
        basically, if users dont specify then we want this
        https://stackoverflow.com/questions/15265146/inner-join-2-tables-but-return-all-if-1-table-empty
        so we either inner join over everything, or we switch to a left join
        '''

        if len(species_path_from)==0:
            join_type_species='left'
        else:
            join_type_species='inner'
        if len(organ_path_from)==0:
            join_type_organ='left'
        else:
            join_type_organ='inner'
        if len(disease_path_from)==0:
            join_type_disease='left'
        else:
            join_type_disease='inner'

        self.string_node_search_part_1_from=f'''
            create view node_search_part_1_from as
            select
                species_node_from,
                root_dist_species_from,
                leaf_dist_species_from,
                organ_node_from,
                root_dist_organ_from,
                leaf_dist_organ_from,
                disease_node_from,
                root_dist_disease_from,
                leaf_dist_disease_from
            from (
                select
                    species_node_from,
                    root_dist_species_from,
                    leaf_dist_species_from,
                    organ_node_from,
                    root_dist_organ_from,
                    leaf_dist_organ_from,
                    disease_node_from,
                    root_dist_disease_from,
                    leaf_dist_disease_from
                from (
                    select 
                        species_node_from,
                        root_dist_species_from,
                        leaf_dist_species_from,
                        organ_node_from,
                        root_dist_organ_from,
                        leaf_dist_organ_from,
                        disease_node_from,
                        root_dist_disease_from,
                        leaf_dist_disease_from
                    from(
                        select 
                            species_node_from,
                            root_dist_species_from,
                            leaf_dist_species_from,
                            organ_node_from,
                            root_dist_organ_from,
                            leaf_dist_organ_from,
                            hftd.node_id as disease_node_from,
                            hftd.distance_from_root as root_dist_disease_from,
                            hftd.distance_from_furthest_leaf as leaf_dist_disease_from
                        from (
                            select
                                hfts.node_id as species_node_from,
                                hfts.distance_from_root as root_dist_species_from,
                                hfts.distance_from_furthest_leaf as leaf_dist_species_from,
                                hfto.node_id as organ_node_from,
                                hfto.distance_from_root as root_dist_organ_from,
                                hfto.distance_from_furthest_leaf as leaf_dist_organ_from
                            from (
                                    hierarchy_filter_table_species hfts 
                                cross join
                                    hierarchy_filter_table_organ hfto
                            )
                        ) as cross_1
                        cross join
                            hierarchy_filter_table_disease hftd 
                    ) as temp_1 
                    {join_type_species} join 
                        unnest(array{species_path_from})
                    on 
                        "unnest"=species_node_from 
                ) as temp_2
                {join_type_organ} join 
                    unnest(array{organ_path_from})
                on
                    "unnest"=organ_node_from
            ) as temp_3
            {join_type_disease} join 
                unnest(array{disease_path_from})
            on
                "unnest"=disease_node_from            
        '''
        

    def build_node_search_part_1_to(
        self,
        # species_distance_from_root,
        # organ_distance_from_root,
        # disease_distance_from_root,
        species_path_to,
        organ_path_to,
        disease_path_to
    ):
        '''
        explanation of the left/inner join option
        basically, if users dont specify then we want this
        https://stackoverflow.com/questions/15265146/inner-join-2-tables-but-return-all-if-1-table-empty
        so we either inner join over everything, or we switch to a left join
        '''

        if len(species_path_to)==0:
            join_type_species='left'
        else:
            join_type_species='inner'
        if len(organ_path_to)==0:
            join_type_organ='left'
        else:
            join_type_organ='inner'
        if len(disease_path_to)==0:
            join_type_disease='left'
        else:
            join_type_disease='inner'

        self.string_node_search_part_1_to=f'''
            create view node_search_part_1_to as
            select
                species_node_to,
                root_dist_species_to,
                leaf_dist_species_to,
                organ_node_to,
                root_dist_organ_to,
                leaf_dist_organ_to,
                disease_node_to,
                root_dist_disease_to,
                leaf_dist_disease_to
            from (
                select
                    species_node_to,
                    root_dist_species_to,
                    leaf_dist_species_to,
                    organ_node_to,
                    root_dist_organ_to,
                    leaf_dist_organ_to,
                    disease_node_to,
                    root_dist_disease_to,
                    leaf_dist_disease_to
                from (
                    select 
                        species_node_to,
                        root_dist_species_to,
                        leaf_dist_species_to,
                        organ_node_to,
                        root_dist_organ_to,
                        leaf_dist_organ_to,
                        disease_node_to,
                        root_dist_disease_to,
                        leaf_dist_disease_to
                    from(
                        select 
                            species_node_to,
                            root_dist_species_to,
                            leaf_dist_species_to,
                            organ_node_to,
                            root_dist_organ_to,
                            leaf_dist_organ_to,
                            hftd.node_id as disease_node_to,
                            hftd.distance_from_root as root_dist_disease_to,
                            hftd.distance_from_furthest_leaf as leaf_dist_disease_to
                        from (
                            select
                                hfts.node_id as species_node_to,
                                hfts.distance_from_root as root_dist_species_to,
                                hfts.distance_from_furthest_leaf as leaf_dist_species_to,
                                hfto.node_id as organ_node_to,
                                hfto.distance_from_root as root_dist_organ_to,
                                hfto.distance_from_furthest_leaf as leaf_dist_organ_to
                            from (
                                    hierarchy_filter_table_species hfts 
                                cross join
                                    hierarchy_filter_table_organ hfto
                            )
                        ) as cross_1
                        cross join
                            hierarchy_filter_table_disease hftd 
                    ) as temp_1 
                    {join_type_species} join 
                        unnest(array{species_path_to})
                    on 
                        "unnest"=species_node_to 
                ) as temp_2
                {join_type_organ} join 
                    unnest(array{organ_path_to})
                on
                    "unnest"=organ_node_to
            ) as temp_3
            {join_type_disease} join 
                unnest(array{disease_path_to})
            on
                "unnest"=disease_node_to            
        '''

    def build_node_search_part_2(
        self
    ):

        self.string_node_search_part_2=f'''
            create view node_search_part_2 as
            select 
                species_node_from,
                root_dist_species_from,
                leaf_dist_species_from,
                organ_node_from,
                root_dist_organ_from,
                leaf_dist_organ_from,
                disease_node_from,
                root_dist_disease_from,
                leaf_dist_disease_from,
                species_node_to,
                root_dist_species_to,
                leaf_dist_species_to,
                organ_node_to,
                root_dist_organ_to,
                leaf_dist_organ_to,
                disease_node_to,
                root_dist_disease_to,
                leaf_dist_disease_to,
                from_triplets_inter_removed_if_nec,
                to_triplets_inter_removed_if_nec
            from (
                select 
                    *
                from 
                    node_search_part_1_from nspf 
                inner join
                    headnode_pairs_to_triplet_list_pair hpttlp 
                on
                    (species_node_from = hpttlp.species_headnode_from) and
                    (organ_node_from = hpttlp.organ_headnode_from) and
                    (disease_node_from = hpttlp.disease_headnode_from)
            ) as temp_1
            inner join 
                node_search_part_1_to nspt 
            on
                (species_node_to = temp_1.species_headnode_to) and
                (organ_node_to = temp_1.organ_headnode_to) and
                (disease_node_to = temp_1.disease_headnode_to)
            '''


    def build_node_search_part_3(
        self,
        #compound_distance_from_root,
        compound_path,
    ):
        if len(compound_path)==0:
            join_type_compound='left'
        else:
            join_type_compound='inner'

        self.string_node_search_part_3=f'''
            create view node_search_part_3 as
            select
            node_id,
            root_dist_compound,
            leaf_dist_compound
            from(
                select
                    node_id,
                    distance_from_root as root_dist_compound,
                    distance_from_furthest_leaf as leaf_dist_compound
                from
                    hierarchy_filter_table_compound hftc 
            ) as temp_1
            {join_type_compound} join 
                unnest(array{compound_path})
            on
                "unnest"=node_id
        '''

    def build_node_search_part_4(self):

        self.string_node_search_part_4=f'''
        create view node_search_part_4 as
        select
            *
        from 
            node_search_part_2 nsp_2
            cross join
            node_search_part_3 nsp_3    
        '''

    def build_node_search_part_5(self):

        self.string_node_search_part_5=f'''
        create view node_search_part_5 as
        select 
            species_node_from,
            root_dist_species_from,
            leaf_dist_species_from,
            organ_node_from,
            root_dist_organ_from,
            leaf_dist_organ_from,
            disease_node_from,
            root_dist_disease_from,
            leaf_dist_disease_from,
            species_node_to,
            root_dist_species_to,
            leaf_dist_species_to,
            organ_node_to,
            root_dist_organ_to,
            leaf_dist_organ_to,
            disease_node_to,
            root_dist_disease_to,
            leaf_dist_disease_to,
            compound,
            root_dist_compound,
            leaf_dist_compound,            
            fold_average,
            fold_median,
            sig_mannwhit,
            sig_welch
        from
            node_search_part_4 nsp_4
        inner join
            combined_results cr 
        on
            (nsp_4.from_triplets_inter_removed_if_nec = cr.from_triplets) and 
            (nsp_4.to_triplets_inter_removed_if_nec = cr.to_triplets) and
            (nsp_4.node_id = cr.compound)
        '''
    def build_node_search_part_6(
        self,
        page_current,
        page_size,
        sort_by,
        filter_query
    ):


        where_string=self.construct_filter_where(filter_query)
        order_by_string=self.construct_order_by(sort_by)
        pagination_string=self.construct_pagination(page_current,page_size)  

        self.string_node_search_part_6=f'''
        select 
        species_from,
        root_dist_species_from,
        leaf_dist_species_from,
        organ_from,
        root_dist_organ_from,
        leaf_dist_organ_from,
        disease_from,
        root_dist_disease_from,
        leaf_dist_disease_from,
        species_to,
        root_dist_species_to,
        leaf_dist_species_to,
        organ_to,
        root_dist_organ_to,
        leaf_dist_organ_to,
        disease_to,
        root_dist_disease_to,
        leaf_dist_disease_to,
        cp.english_name as compound,
        root_dist_compound,
        leaf_dist_compound,            
        fold_average,
        fold_median,
        sig_mannwhit,
        sig_welch
        from (
            select 
            species_from,
            root_dist_species_from,
            leaf_dist_species_from,
            organ_from,
            root_dist_organ_from,
            leaf_dist_organ_from,
            disease_from,
            root_dist_disease_from,
            leaf_dist_disease_from,
            species_to,
            root_dist_species_to,
            leaf_dist_species_to,
            organ_to,
            root_dist_organ_to,
            leaf_dist_organ_to,
            dp2.english_name as disease_to,
            root_dist_disease_to,
            leaf_dist_disease_to,
            compound,
            root_dist_compound,
            leaf_dist_compound,            
            fold_average,
            fold_median,
            sig_mannwhit,
            sig_welch
            from (
                select 
                species_from,
                root_dist_species_from,
                leaf_dist_species_from,
                organ_from,
                root_dist_organ_from,
                leaf_dist_organ_from,
                disease_from,
                root_dist_disease_from,
                leaf_dist_disease_from,
                species_to,
                root_dist_species_to,
                leaf_dist_species_to,
                op2.english_name as organ_to,
                root_dist_organ_to,
                leaf_dist_organ_to,
                disease_node_to,
                root_dist_disease_to,
                leaf_dist_disease_to,
                compound,
                root_dist_compound,
                leaf_dist_compound,            
                fold_average,
                fold_median,
                sig_mannwhit,
                sig_welch
                from (
                    select 
                    species_from,
                    root_dist_species_from,
                    leaf_dist_species_from,
                    organ_from,
                    root_dist_organ_from,
                    leaf_dist_organ_from,
                    disease_from,
                    root_dist_disease_from,
                    leaf_dist_disease_from,
                    sp2.english_name as species_to,
                    root_dist_species_to,
                    leaf_dist_species_to,
                    organ_node_to,
                    root_dist_organ_to,
                    leaf_dist_organ_to,
                    disease_node_to,
                    root_dist_disease_to,
                    leaf_dist_disease_to,
                    compound,
                    root_dist_compound,
                    leaf_dist_compound,            
                    fold_average,
                    fold_median,
                    sig_mannwhit,
                    sig_welch
                    from (
                        select 
                        species_from,
                        root_dist_species_from,
                        leaf_dist_species_from,
                        organ_from,
                        root_dist_organ_from,
                        leaf_dist_organ_from,
                        dp.english_name as disease_from,
                        root_dist_disease_from,
                        leaf_dist_disease_from,
                        species_node_to,
                        root_dist_species_to,
                        leaf_dist_species_to,
                        organ_node_to,
                        root_dist_organ_to,
                        leaf_dist_organ_to,
                        disease_node_to,
                        root_dist_disease_to,
                        leaf_dist_disease_to,
                        compound,
                        root_dist_compound,
                        leaf_dist_compound,            
                        fold_average,
                        fold_median,
                        sig_mannwhit,
                        sig_welch
                        from (
                            select 
                            species_from,
                            root_dist_species_from,
                            leaf_dist_species_from,
                            op.english_name as organ_from,
                            root_dist_organ_from,
                            leaf_dist_organ_from,
                            disease_node_from,
                            root_dist_disease_from,
                            leaf_dist_disease_from,
                            species_node_to,
                            root_dist_species_to,
                            leaf_dist_species_to,
                            organ_node_to,
                            root_dist_organ_to,
                            leaf_dist_organ_to,
                            disease_node_to,
                            root_dist_disease_to,
                            leaf_dist_disease_to,
                            compound,
                            root_dist_compound,
                            leaf_dist_compound,            
                            fold_average,
                            fold_median,
                            sig_mannwhit,
                            sig_welch
                            from (
                                select
                                sp.english_name as species_from,
                                root_dist_species_from,
                                leaf_dist_species_from,
                                organ_node_from,
                                root_dist_organ_from,
                                leaf_dist_organ_from,
                                disease_node_from,
                                root_dist_disease_from,
                                leaf_dist_disease_from,
                                species_node_to,
                                root_dist_species_to,
                                leaf_dist_species_to,
                                organ_node_to,
                                root_dist_organ_to,
                                leaf_dist_organ_to,
                                disease_node_to,
                                root_dist_disease_to,
                                leaf_dist_disease_to,
                                compound,
                                root_dist_compound,
                                leaf_dist_compound,            
                                fold_average,
                                fold_median,
                                sig_mannwhit,
                                sig_welch
                                from node_search_part_5
                                inner join
                                species_properties sp 
                                on
                                species_node_from = sp.identifier 
                            ) foo_1
                            inner join
                            organ_properties as op
                            on 
                            foo_1.organ_node_from=op.identifier 
                            ) foo_2
                        inner join 
                        disease_properties as dp 
                        on
                        foo_2.disease_node_from=dp.identifier
                    ) foo_3
                    inner join 
                    species_properties sp2 
                    on
                    foo_3.species_node_to=sp2.identifier
                    ) foo_4
                inner join 
                organ_properties op2
                on
                foo_4.organ_node_to=op2.identifier
                ) foo_5
            inner join 
            disease_properties dp2 
            on
            foo_5.disease_node_to=dp2.identifier 
            ) foo_6
        inner join 
        compound_properties cp 
        on
        foo_6.compound=cp.identifier 
        {where_string} {order_by_string}
        {pagination_string}
        '''
        # {where_string} {order_by_string}
        # {pagination_string}   
        # print(where_string)
        # print(order_by_string)
        # print(pagination_string)
        # print('&&&&&&&&&&&&&&&&&&&&&&&')
        # pprint(self.string_node_search_part_6)

    def build_delete_views(self):

        self.string_delete_views=f'''
        drop view node_search_part_1_from cascade;
        drop view node_search_part_1_to cascade;
        drop view node_search_part_3 cascade;
        '''