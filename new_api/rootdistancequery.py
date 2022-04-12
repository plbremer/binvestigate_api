class BasicTableQuery():

    def init():
        pass

    def build_node_search_part_1(

    ):
        string_node_search_part_1=f'''
            create view node_search_part_1_from as
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
                    hfts.distance_from_root < 8 and
                    hfto.distance_from_root < 8
                )
            ) as cross_1
            cross join
                hierarchy_filter_table_disease hftd 
            where (
                hftd.distance_from_root < 8
            )
        '''
        