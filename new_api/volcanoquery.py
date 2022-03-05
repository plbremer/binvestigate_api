

class VolcanoQuery():

    def __init__(self,
        from_species,
        from_organ,
        from_disease,
        to_species,
        to_organ,
        to_disease,
        include_known,
        include_unknown,
        fold_median_min,
        fold_average_min,
        p_welch_max,
        p_mann_max
    ):
        '''
            for the moment we dont use 
                include_known,
                include_unknown,
                fold_median_min,
                fold_average_min,
                p_welch_max,
                p_mann_max
            for the moment we also dont include pagination. want to populate the volcano plot
            opted to call these for the moment
            extract_triplet_codes.species_headnode_from,
            extract_triplet_codes.organ_headnode_from,
            extract_triplet_codes.disease_headnode_from,
            extract_triplet_codes.species_headnode_to,
            extract_triplet_codes.organ_headnode_to,
            extract_triplet_codes.disease_headnode_from,
        '''

        self.query=f'''
            select 
                oq.compound,
                cp.english_name,
                oq.fold_average,
                oq.fold_median,
                oq.sig_mannwhit,
                oq.sig_welch    
            from (
                select
                    cr.compound,
                    cr.fold_average,
                    cr.fold_median,
                    cr.sig_mannwhit,
                    cr.sig_welch 
                from
                    (
                        select 
                        *
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
                combined_results cr 
                on
                extract_triplet_codes.from_triplets_inter_removed_if_nec=cr.from_triplets and 
                extract_triplet_codes.to_triplets_inter_removed_if_nec=cr.to_triplets
            ) as oq
            inner join
            compound_properties cp 
            on
            oq.compound=cp.identifier 
        '''


