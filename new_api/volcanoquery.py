

class VolcanoQuery():

    def construct_filter_where(self,filter_string):
        if len(filter_string)==0:
            return ''
        filter_list=filter_string.split('&&')
        where_clauses=list()
        for filter_statement in filter_list:
            temp_list=filter_statement.split(' ')
            if (temp_list[1]=='s>') or (temp_list[1]=='s>=') or (temp_list[1]=='s<') or (temp_list[1]=='s<=') or (temp_list[1]=='s='):
                temp_where_clause='('+temp_list[0][1:-1]+' '+temp_list[1][1:]+' '+temp_list[2]+')'
                where_clauses.append(temp_where_clause)
            elif (temp_list[1]=='scontains'):
                temp_where_clause='('+temp_list[0][1:-1]+' like \'%%'+temp_list[2]+'%%\')'
                where_clauses.append(temp_where_clause)
        where_clause_string=' and '
        for where_clause in where_clauses:
            where_clause_string=where_clause_string+where_clause+' and \n'
        where_clause_string=where_clause_string[:-6]
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

    def construct_compound_where(self,classes,knowns,unknowns):
        #nothing. basically for now we just cause query fail
        if (classes=='No') and (knowns=='No') and (unknowns=='No'):
            return 'error'
        #classes only
        elif (classes=='Yes') and (knowns=='No') and (unknowns=='No'):
            return 'where (oq.compound !~ \'^[0-9\.]+$\') '
        #all compounds only
        elif (classes=='No') and (knowns=='Yes') and (unknowns=='Yes'):
            return 'where (oq.compound ~ \'^[0-9\.]+$\') '
        #classes and knowns
        elif (classes=='Yes') and (knowns=='Yes') and (unknowns=='No'):
            return 'where (cp.english_name !~ \'^[0-9\.]+$\') '
        #classes and unknowns
        elif (classes=='Yes') and (knowns=='Yes') and (unknowns=='No'):
            return 'where (cp.english_name ~ \'^[0-9\.]+$\') or (oq.compound !~ \'^[0-9\.]+$\') '
        #all unknowns only
        elif (classes=='No') and (knowns=='No') and (unknowns=='Yes'):
            return 'where (cp.english_name ~ \'^[0-9\.]+$\') '
        #all knowns only
        elif (classes=='No') and (knowns=='Yes') and (unknowns=='No'):
            return 'where (cp.english_name !~ \'^[0-9\.]+$\') and (oq.compound ~ \'^[0-9\.]+$\') '
        #everything
        elif (classes=='Yes') and (knowns=='Yes') and (unknowns=='Yes'):
            return ''

    def construct_pagination(self, current_page,page_size):
        temp_offset=page_size*current_page
        temp_limit=page_size
        pagination_string=f'limit {temp_limit} offset {temp_offset}'
        return pagination_string

    def __init__(self,
        from_species,
        from_organ,
        from_disease,
        to_species,
        to_organ,
        to_disease,
        #include_bins,
        include_classes,
        include_knowns,
        include_unknowns,
        current_page,
        page_size,
        column_sort,
        column_filter
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

            ignore above 4-25-22
        '''

        #this chunk of code is trying to filter out compound that are not requested by the user
        #by default everything is requested
        #so if something is not requested, then we tag a line onto a where clause
        # total_compound_string=''
        # if (include_classes=='No') or (include_knowns=='No') or (include_knowns=='No'):
        #     total_compound_string=total_compound_string+' where \n'
        # else:
        #     total_compound_string=''
        # if include_classes=='Yes':
        #     class_string=('')
        # elif include_classes=='No':
        #     class_string=('(oq.compound ~ \'^[0-9\.]+$\') and \n')
        # if include_knowns=='Yes':
        #     known_string=('')
        # elif include_knowns=='No':
        #     known_string=('(cp.english_name !~ \'^[0-9\.]+$\') and \n')
        # if include_unknowns=='Yes':
        #     unknown_string=('')
        # elif include_unknowns=='No':
        #     unknown_string=('(cp.english_name !~ \'^[0-9\.]+$\') and \n')
        # total_compound_string=total_compound_string+class_string+known_string+unknown_string
        # total_compound_string=total_compound_string[:-5]



        total_compound_string=self.construct_compound_where(include_classes,include_knowns,include_unknowns)
        where_string=self.construct_filter_where(column_filter)
        order_by_string=self.construct_order_by(column_sort)
        pagination_string=self.construct_pagination(current_page,page_size)

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
            {total_compound_string} {where_string} {order_by_string}
            {pagination_string}
        '''


