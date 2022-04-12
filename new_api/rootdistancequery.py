class BasicTableQuery():

    def init():
        pass

    def build_node_search_part_1_from(
        self,
        species_distance_from_root,
        organ_distance_from_root,
        disease_distance_from_root,
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
                organ_node_from,
                disease_node_from 
            from (
                select
                    species_node_from,
                    organ_node_from,
                    disease_node_from 
                from (
                    select 
                        species_node_from,
                        organ_node_from,
                        disease_node_from 
                    from(
                        select 
                            species_node_from,
                            organ_node_from,
                            hftd.node_id as disease_node_from
                        from (
                            select
                                hfts.node_id as species_node_from,
                                hfto.node_id as organ_node_from
                            from (
                                    hierarchy_filter_table_species hfts 
                                cross join
                                    hierarchy_filter_table_organ hfto
                            )
                            where (
                                hfts.distance_from_root < {species_distance_from_root} and
                                hfto.distance_from_root < {organ_distance_from_root}
                            )
                        ) as cross_1
                        cross join
                            hierarchy_filter_table_disease hftd 
                        where (
                            hftd.distance_from_root < {disease_distance_from_root}
                        )
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
        species_distance_from_root,
        organ_distance_from_root,
        disease_distance_from_root,
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
                species_node_from,
                organ_node_from,
                disease_node_from 
            from (
                select
                    species_node_from,
                    organ_node_from,
                    disease_node_from 
                from (
                    select 
                        species_node_from,
                        organ_node_from,
                        disease_node_from 
                    from(
                        select 
                            species_node_from,
                            organ_node_from,
                            hftd.node_id as disease_node_from
                        from (
                            select
                                hfts.node_id as species_node_from,
                                hfto.node_id as organ_node_from
                            from (
                                    hierarchy_filter_table_species hfts 
                                cross join
                                    hierarchy_filter_table_organ hfto
                            )
                            where (
                                hfts.distance_from_root < {species_distance_from_root} and
                                hfto.distance_from_root < {organ_distance_from_root}
                            )
                        ) as cross_1
                        cross join
                            hierarchy_filter_table_disease hftd 
                        where (
                            hftd.distance_from_root < {disease_distance_from_root}
                        )
                    ) as temp_1 
                    {join_type_species} join 
                        unnest(array{species_path_to})
                    on 
                        "unnest"=species_node_from 
                ) as temp_2
                {join_type_organ} join 
                    unnest(array{organ_path_to})
                on
                    "unnest"=organ_node_from
            ) as temp_3
            {join_type_disease} join 
                unnest(array{disease_path_to})
            on
                "unnest"=disease_node_from            
        '''

    def build_node_search_part_2(
        self
    ):

        self.node_search_part_2=f'''
            create view node_search_part_2 as
            select 
                species_node_from,
                organ_node_from,
                disease_node_from,
                species_node_to,
                organ_node_to,
                disease_node_to,
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
        compound_distance_from_root,
        compound_path,
    ):
        if len(compound_path)==0:
            join_type_compound='left'
        else:
            join_type_compound='inner'

        self.node_search_part_3=f'''
            create view node_search_part_3 as
            select
            *
            from(
                select
                    node_id
                from
                    hierarchy_filter_table_compound hftc 
                where 
                    (hftc.distance_from_root < {compound_distance_from_root}) and 
                    --currently, unknown compounds are connected to the root node
                    --so, what we want to do is avoid the situation where we get 
                    --compounds when we are really trying to get aggregations
                    (hftc.we_map_to = 'No')
            ) as temp_1
            {join_type_compound} join 
                unnest(array{compound_path})
            on
                "unnest"=node_id
        '''

    def build_node_search_part_4(self):

        self.node_search_part_4=f'''
        create view node_search_part_4 as
        select
            *
        from 
            node_search_part_2 nsp_2
            cross join
            node_search_part_3 nsp_3    
        '''

    def build_node_search_part_5(self):

        self.node_search_part_5=f'''
        select 
            species_node_from,
            organ_node_from,
            disease_node_from,
            species_node_to,
            organ_node_to,
            disease_node_to,
            compound,
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
            (nsp_4.to_triplets_inter_removed_if_nec = cr.to_triplets)
        '''
    def build_delete_views(self):

        self.delete_views=f'''
        drop view node_search_part_1_from cascade;
        drop view node_search_part_1_to cascade;
        drop view node_search_part_3;
        '''