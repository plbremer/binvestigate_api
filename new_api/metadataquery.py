

class MetadataQuery:


    def __init__(self,
        from_species,
        from_organ,
        from_disease,
        to_species,
        to_organ,
        to_disease,
    ):

        self.query=f'''
        
            select 
                triplet_count as triplet_count_to,
                sample_count_list as sample_count_list_to,
                min_sample_count as min_sample_count_to,
                sum_sample_count as sum_sample_count_to,
                unique_triplet_list_real as unique_triplet_list_real_to,
                triplet_count as triplet_count_from,
                sample_count_list as sample_count_list_from,
                min_sample_count as min_sample_count_from,
                sum_sample_count as sum_sample_count_from,
                unique_triplet_list_real as unique_triplet_list_real_from
            from (
                select
                    to_triplets_inter_removed_if_nec,
                    unique_triplets as triplet_id_from,
                    triplet_count as triplet_count_from,
                    sample_count_list as sample_count_list_from,
                    min_sample_count as min_sample_count_from,
                    sum_sample_count as sum_sample_count_from,
                    unique_triplet_list_real as unique_triplet_list_real_from
                from 
                    (
                    select 
                    from_triplets_inter_removed_if_nec,
                    to_triplets_inter_removed_if_nec
                    from 
                    headnode_pairs_to_triplet_list_pair hpttlp 
                    where 
                    hpttlp.species_headnode_from = '{from_species}' and
                    hpttlp.organ_headnode_from = '{from_organ}' and
                    hpttlp.disease_headnode_from = '{from_disease}' and
                    hpttlp.species_headnode_to = '{to_species}' and
                    hpttlp.organ_headnode_to = '{to_organ}' and
                    hpttlp.disease_headnode_to = '{to_disease}'
                    ) as extract_triplet_codes
                inner join 
                unique_reduced_trip_list_to_properties urtltp 
                on
                extract_triplet_codes.from_triplets_inter_removed_if_nec=urtltp.unique_triplets 
            ) as from_info
            inner join 
            unique_reduced_trip_list_to_properties urtltp
            on
            from_info.to_triplets_inter_removed_if_nec=urtltp.unique_triplets 

        '''