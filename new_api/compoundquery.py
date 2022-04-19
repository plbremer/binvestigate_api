class CompoundQuery():

    def init():
        pass

    def build_query(
        self,
        compound,
        species_from,
        organ_from,
        disease_from,
        species_to,
        organ_to,
        disease_to
    ):
        if species_from=='any':
            join_type_species_from='left'
        else:
            join_type_species_from='inner'
        if organ_from=='any':
            join_type_organ_from='left'
        else:
            join_type_organ_from='inner'
        if disease_from=='any':
            join_type_disease_from='left'
        else:
            join_type_disease_from='inner'
        if species_to=='any':
            join_type_species_to='left'
        else:
            join_type_species_to='inner'
        if organ_to=='any':
            join_type_organ_to='left'
        else:
            join_type_organ_to='inner'
        if disease_to=='any':
            join_type_disease_to='left'
        else:
            join_type_disease_to='inner'
        
        
        self.query=f'''
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


