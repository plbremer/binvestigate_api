
#select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, fold_results_fold_change_matrix_median.results as results_fold_median from (


# first_results
#                 inner join
#                 fold_results_fold_change_matrix_median
#                 on
#                 fold_results_fold_change_matrix_average.from_triplets = fold_results_fold_change_matrix_median.from_triplets AND
#                 fold_results_fold_change_matrix_average.to_triplets = fold_results_fold_change_matrix_median.to_triplets AND
#                 fold_results_fold_change_matrix_average.compound = fold_results_fold_change_matrix_median.compound     
class BasicTableQuery():

    def init():
        pass

    def build_view_1(self,temp_from_species,temp_from_organ,temp_from_disease,temp_to_species,temp_to_organ,temp_to_disease,temp_compound):
        self.view_1=f'''
            create temp view temp_view_1 as                                                                                                                                                                                                                                                             
            select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, result_3.comp, result_3.from_triplets, result_3.to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results as results_sig_welch from
            (           
            select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, result_2.comp, result_2.from_triplets, result_2.to_triplets, results_fold_average, results_fold_median, results as results_sig_mannwhitney from
            (
            select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, result_1.comp, result_1.from_triplets, result_1.to_triplets, results_fold_average, results as results_fold_median from
            (
            select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results as results_fold_average from 
                (
                    select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets_inter_removed_if_nec, to_triplets_inter_removed_if_nec from 
                    (
                        (
                            (
                                select from_head_spec,from_head_org,from_head_dis,to_head_spec,to_head_org,to_head_dis from (
                                    (
                                        unnest(array{temp_from_species}) as from_head_spec 
                                        cross join 
                                        unnest(array{temp_from_organ}) as from_head_org
                                        cross join
                                        unnest(array{temp_from_disease}) as from_head_dis
                                    ) unnest_result_from
                                    inner join 
                                    
                                    headnodes_to_triplets AS htt_from
                                        on
                                        from_head_spec=htt_from.species_headnode
                                        AND
                                        from_head_org=htt_from.organ_headnode
                                        AND
                                        from_head_dis=htt_from.disease_headnode
                                ) trips_from
                                cross join 
                                (
                                    (
                                        unnest(array{temp_to_species}) as to_head_spec 
                                        cross join 
                                        unnest(array{temp_to_organ}) as to_head_org
                                        cross join
                                        unnest(array{temp_to_disease}) as to_head_dis
                                    ) unnest_result_to 
                                    inner join 
                                    headnodes_to_triplets AS htt_to
                                        on
                                        to_head_spec=htt_to.species_headnode
                                        AND
                                        to_head_org=htt_to.organ_headnode
                                        AND
                                        to_head_dis=htt_to.disease_headnode
                                ) trips_to
                            ) triplets_from_and_to
                            cross join
                            unnest(array{temp_compound}) as comp
                        ) from_and_to_and_comp
                        
                        inner join
                        
                        headnode_pairs_to_triplet_list_pair 
                        on
                        species_headnode_from = from_head_spec AND
                        organ_headnode_from = from_head_org AND
                        disease_headnode_from = from_head_dis AND
                        species_headnode_to = to_head_spec AND
                        organ_headnode_to = to_head_org AND
                        disease_headnode_to = to_head_dis
                    ) 
                ) from_and_to_and_comp_to_triplets
                inner join
                fold_results_fold_change_matrix_average
                on
                from_triplets_inter_removed_if_nec = fold_results_fold_change_matrix_average.from_triplets AND
                to_triplets_inter_removed_if_nec = fold_results_fold_change_matrix_average.to_triplets AND
                comp = fold_results_fold_change_matrix_average.compound
        ) as result_1
                inner join
                fold_results_fold_change_matrix_median
                on
                result_1.from_triplets = fold_results_fold_change_matrix_median.from_triplets AND
                result_1.to_triplets = fold_results_fold_change_matrix_median.to_triplets AND
                result_1.comp = fold_results_fold_change_matrix_median.compound 
        ) as result_2
                inner join
                fold_results_signifigance_matrix_mannwhitney
                on
                result_2.from_triplets = fold_results_signifigance_matrix_mannwhitney.from_triplets AND
                result_2.to_triplets = fold_results_signifigance_matrix_mannwhitney.to_triplets AND
                result_2.comp = fold_results_signifigance_matrix_mannwhitney.compound
        ) as result_3
                inner join
                fold_results_signifigance_matrix_welch
                on
                result_3.from_triplets = fold_results_signifigance_matrix_welch.from_triplets AND
                result_3.to_triplets = fold_results_signifigance_matrix_welch.to_triplets AND
                result_3.comp = fold_results_signifigance_matrix_welch.compound


                ;
        '''

    def build_view_2(self):
        self.view_2='''
        create temp view temp_view_2 as
        select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results_sig_welch, triplet_count_from, sample_count_list_from, min_sample_count_from, sum_sample_count_from, from_triplet_list, triplet_count_to, sample_count_list_to, min_sample_count_to, sum_sample_count_to, to_triplet_list, distance_from_root_s_from, distance_from_furthest_leaf_s_from, distance_from_root_s_to, distance_from_furthest_leaf_s_to, distance_from_root_o_from, distance_from_furthest_leaf_o_from, distance_from_root_o_to, distance_from_furthest_leaf_o_to, distance_from_root_d_from, distance_from_furthest_leaf_d_from, distance_from_root_d_to, distance_from_furthest_leaf_d_to, distance_from_root AS distance_from_root_comp, distance_from_furthest_leaf AS distance_from_furthest_leaf_comp from (
            select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results_sig_welch, triplet_count_from, sample_count_list_from, min_sample_count_from, sum_sample_count_from, from_triplet_list, triplet_count_to, sample_count_list_to, min_sample_count_to, sum_sample_count_to, to_triplet_list, distance_from_root_s_from, distance_from_furthest_leaf_s_from, distance_from_root_s_to, distance_from_furthest_leaf_s_to, distance_from_root_o_from, distance_from_furthest_leaf_o_from, distance_from_root_o_to, distance_from_furthest_leaf_o_to, distance_from_root_d_from, distance_from_furthest_leaf_d_from, distance_from_root AS distance_from_root_d_to, distance_from_furthest_leaf AS distance_from_furthest_leaf_d_to from (
                select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results_sig_welch, triplet_count_from, sample_count_list_from, min_sample_count_from, sum_sample_count_from, from_triplet_list, triplet_count_to, sample_count_list_to, min_sample_count_to, sum_sample_count_to, to_triplet_list, distance_from_root_s_from, distance_from_furthest_leaf_s_from, distance_from_root_s_to, distance_from_furthest_leaf_s_to, distance_from_root_o_from, distance_from_furthest_leaf_o_from, distance_from_root_o_to, distance_from_furthest_leaf_o_to, distance_from_root AS distance_from_root_d_from, distance_from_furthest_leaf AS distance_from_furthest_leaf_d_from from (
                    select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results_sig_welch, triplet_count_from, sample_count_list_from, min_sample_count_from, sum_sample_count_from, from_triplet_list, triplet_count_to, sample_count_list_to, min_sample_count_to, sum_sample_count_to, to_triplet_list, distance_from_root_s_from, distance_from_furthest_leaf_s_from, distance_from_root_s_to, distance_from_furthest_leaf_s_to, distance_from_root_o_from, distance_from_furthest_leaf_o_from, distance_from_root AS distance_from_root_o_to, distance_from_furthest_leaf AS distance_from_furthest_leaf_o_to from (
                        select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results_sig_welch, triplet_count_from, sample_count_list_from, min_sample_count_from, sum_sample_count_from, from_triplet_list, triplet_count_to, sample_count_list_to, min_sample_count_to, sum_sample_count_to, to_triplet_list, distance_from_root_s_from, distance_from_furthest_leaf_s_from, distance_from_root_s_to, distance_from_furthest_leaf_s_to, distance_from_root AS distance_from_root_o_from, distance_from_furthest_leaf AS distance_from_furthest_leaf_o_from from (
                            select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results_sig_welch, triplet_count_from, sample_count_list_from, min_sample_count_from, sum_sample_count_from, from_triplet_list, triplet_count_to, sample_count_list_to, min_sample_count_to, sum_sample_count_to, to_triplet_list, distance_from_root_s_from, distance_from_furthest_leaf_s_from, distance_from_root AS distance_from_root_s_to, distance_from_furthest_leaf AS distance_from_furthest_leaf_s_to from (
                                select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results_sig_welch, triplet_count_from, sample_count_list_from, min_sample_count_from, sum_sample_count_from, from_triplet_list, triplet_count_to, sample_count_list_to, min_sample_count_to, sum_sample_count_to,  to_triplet_list, distance_from_root AS distance_from_root_s_from, distance_from_furthest_leaf AS distance_from_furthest_leaf_s_from from (
                                    select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results_sig_welch, triplet_count_from, sample_count_list_from, min_sample_count_from, sum_sample_count_from, from_triplet_list, triplet_count AS triplet_count_to, sample_count_list AS sample_count_list_to, min_sample_count AS min_sample_count_to, sum_sample_count AS sum_sample_count_to, unique_triplet_list_real AS to_triplet_list from 
                                    (
                                        select from_head_spec, from_head_org, from_head_dis, to_head_spec, to_head_org, to_head_dis, comp, from_triplets, to_triplets, results_fold_average, results_fold_median, results_sig_mannwhitney, results_sig_welch, triplet_count AS triplet_count_from, sample_count_list AS sample_count_list_from, min_sample_count AS min_sample_count_from, sum_sample_count AS sum_sample_count_from, unique_triplet_list_real AS from_triplet_list from (
                                                temp_view_1
                                                inner join
                                                    unique_reduced_trip_list_to_properties
                                                    on from_triplets = unique_triplets
                                            ) AS temp_1
                                    ) AS temp_2
                                    inner join 
                                        unique_reduced_trip_list_to_properties
                                        on to_triplets = unique_reduced_trip_list_to_properties.unique_triplets
                                ) AS temp_3
                                inner join
                                    hierarchy_filter_table_species
                                    on from_head_spec=node_id
                            ) AS temp_4
                            inner join
                                hierarchy_filter_table_species
                                on to_head_spec=node_id
                        ) AS temp_5
                        inner join
                            hierarchy_filter_table_organ
                            on from_head_org=node_id
                    ) AS temp_6
                    inner join
                        hierarchy_filter_table_organ
                        on to_head_org=node_id
                ) AS temp_7
                inner join
                    hierarchy_filter_table_disease
                    on from_head_dis=node_id
            ) AS temp_8
            inner join
                hierarchy_filter_table_disease
                on to_head_dis=node_id
        ) AS temp_9
        inner join
            hierarchy_filter_table_compound
            on comp=node_id
        ;
        '''


    #plb 2-8-2022
    #this is where we want to add some sort of filter for 
    #the signifigance values
    def build_view_3(
        self,
        fold_change_input,
        min_triplet_input,
        min_count_input,
        total_count_input,
        max_root_dist_species,
        min_leaf_dist_species,
        max_root_dist_organs,
        min_leaf_dist_organs,
        max_root_dist_diseases,
        min_leaf_dist_diseases,
        max_root_dist_compounds,
        min_leaf_dist_compounds
    ):
        self.view_3=f'''
        create temp view temp_view_3 as
        select * from temp_view_2
            where
                ( (results_fold_median >= {fold_change_input}) OR (results_fold_median <= -{fold_change_input}) ) AND
                ( (results_fold_average >= {fold_change_input}) OR (results_fold_average <= -{fold_change_input}) ) AND
                ( triplet_count_from >= {min_triplet_input}) AND
                ( triplet_count_to >= {min_triplet_input}) AND
                ( min_sample_count_from >= {min_count_input}) AND
                ( sum_sample_count_from >= {total_count_input}) AND
                ( min_sample_count_to >= {min_count_input}) AND
                ( sum_sample_count_to >= {total_count_input}) AND
                ( distance_from_root_s_from < {max_root_dist_species}) AND
                ( distance_from_furthest_leaf_s_from  > {min_leaf_dist_species}) AND
                ( distance_from_root_s_to  < {max_root_dist_species}) AND
                ( distance_from_furthest_leaf_s_to > {min_leaf_dist_species}) AND
                ( distance_from_root_o_from  < {max_root_dist_organs}) AND
                ( distance_from_furthest_leaf_o_from  > {min_leaf_dist_organs}) AND
                ( distance_from_root_o_to  < {max_root_dist_organs}) AND
                ( distance_from_furthest_leaf_o_to  > {min_leaf_dist_organs}) AND
                ( distance_from_root_d_from  < {max_root_dist_diseases}) AND
                ( distance_from_furthest_leaf_d_from  > {min_leaf_dist_diseases}) AND
                ( distance_from_root_d_to  < {max_root_dist_diseases}) AND
                ( distance_from_furthest_leaf_d_to > {min_leaf_dist_diseases}) AND
                ( distance_from_root_comp  < {max_root_dist_compounds}) AND
                ( distance_from_furthest_leaf_comp > {min_leaf_dist_compounds})                
                ;
        '''

    def build_delete_views(self):
        self.delete_views='''
        drop view temp_view_1 cascade;
        '''