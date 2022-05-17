class VennDiagramQuery():
    def init():
        pass


    def build_query_1(
        self,
        #page_current,
        #page_size,
        #sort_by,
        #filter_query,
        dropdown_triplet_selection_value,
        slider_percent_present_value,
        #toggle_average_true_value
    ):
    
        sod_parallel_list=[element.split(' - ') for element in dropdown_triplet_selection_value]
        # if toggle_average_true_value==True:
        #     intensity_type='intensity_average'
        # elif toggle_average_true_value==False:
        #     intensity_type='intensity_median'
        slider_percent_present_value*=0.01

    
        main_line_list=[
            f'case when (max(percent_present) filter (where "species"=\'{sod_parallel_list[i][0]}\' and "organ"=\'{sod_parallel_list[i][1]}\' and "disease"=\'{sod_parallel_list[i][2]}\')>{slider_percent_present_value}) then (bin) else null end as "{dropdown_triplet_selection_value[i]}",\n' for i in range(len(dropdown_triplet_selection_value))
            #f'max({intensity_type}) filter (where "species"=\'{sod_parallel_list[i][0]}\' and "organ"=\'{sod_parallel_list[i][1]}\' and "disease"=\'{sod_parallel_list[i][2]}\') as "{dropdown_triplet_selection_value[i]}",\n' for i in range(len(dropdown_triplet_selection_value))
        ]

        main_line=''.join(main_line_list)
        main_line=main_line[:-2]+'\n'
        # for  i in range(len(main_line_list)):
        #     main_line+=main_line_list[i]

        self.query_1=f'''
            select
            bin,\n'''+main_line+'''from
            non_ratio_table nrt
            group by
            bin
            order by
            bin    
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
        print('DDDDDDiagram query')
        print(self.query_1)
