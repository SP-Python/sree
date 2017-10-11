

##############################################################
# wrapper function to run for all of the above and get out the RPV Calc   
def calculate_segments(df, visitor_id, segment_being_used, group_by):

    def visitors_logic(df, visitor_id):
        """  taking out the agent vid for counts"""
        df['eg'] = df.eg.str.lower()
        df['visits'] = np.where((df.eg == "impression") & (df.eg != "kl_challenger") & (df.agent != 1.0), df[visitor_id], np.NaN)
        df['visits'] = np.where((df.eg == 'control') & (df.eg != "kl_challenger"), df.vid, df.visits)

    # segment and control level agg for visits and revenue
    def aggregate_to_segments(df,subset_fields):
        """aggregate based on the chosen fields for metics"""
        agg = (df
           .groupby(subset_fields)
           .agg({"visits" : pd.Series.nunique, "e_tr" : np.sum, "e_ti" : pd.Series.nunique})
           .unstack(-1)
           .reset_index())
        return agg

    def flatten_aggregate(df, metric,naming) :
        """  Splits each out into a column for each metric """
        f_df = df[metric]
        f_df.columns = f_df.columns.values + str(naming)
        f_df = f_df.fillna(0)
        return f_df

    def calculate_rpv_metrics(df):
        # if no visits then divide by 1 else use visits
        # same as tableau
        df['control_rpv'] = df['control_revenue'] / np.where(df['control_visitors'] == 0, 1, df['control_visitors']) 
        df['impression_rpv'] = df['impression_revenue'] / np.where(df['impression_visitors'] == 0, 1, df['impression_visitors'])
        df['rpv_uplift'] = df['impression_rpv'] - df['control_rpv']
        df['incremental_revenue'] = df['rpv_uplift'] * np.where(df['impression_visitors'] == 0, 1, df['impression_visitors'])

    def calculate_ratios(df):
        """ Calculate additonal metrics show in Tableau"""
        df['control_conversions'] = df['control_orders'] / df['control_visitors']
        df['impression_conversions'] = df['impression_orders'] / df['impression_visitors']
        df['impression_aov'] = df['impression_revenue'] / df['impression_orders']
        df['control_aov'] = df['control_revenue'] /df['control_orders']  

    def fill_and_rename(df,segment_being_used):
        """ fill zeros and fix segment naming"""
        df.fillna(0, inplace=True)
        df.rename(columns={segment_being_used : "Segment"}, inplace=True)
        #df.set_index("Segment", inplace=True)

    def cleanup_colnames(df):
        """ Will fix columns to no longer be snake case and have capitals at front"""
        cols = df.columns.str.title()
        cols = cols.str.replace("_", " ")
        df.columns = cols

    """ Aggregate metrics to the Incremental by Segment type of View"""
    # fix the agents
    visitors_logic(df, visitor_id)

    # setup the fields to group
    subset_fields = [segment_being_used] + group_by #fields you want
    #print subset_fields

    # aggregate the results by the fields
    agg = aggregate_to_segments(df, subset_fields)

    #flatten aggregate fields
    visits = flatten_aggregate(agg, 'visits','_visitors')
    revenue = flatten_aggregate(agg, 'e_tr','_revenue')
    orders = flatten_aggregate(agg, 'e_ti', '_orders')

    # make into single aggregate
    agg2 = pd.concat([agg[segment_being_used], visits, revenue, orders], axis = 1)
    # fill zeros
    agg2.fillna(0, inplace=True)

    # fill and renmae segment
    fill_and_rename(agg2, segment_being_used)

    # Calculate dervied metrics
    calculate_rpv_metrics(agg2)
    calculate_ratios(agg2)

    # replace infinte values with nan
    agg2.replace([np.inf, -np.inf], np.nan, inplace=True)

    # cleaning up columns
    cleanup_colnames(agg2)

    agg2 = agg2[['Segment', 'Control Visitors',   'Control Orders', 'Control Conversions',
                 'Control Revenue', 'Control Aov', 'Control Rpv',
                 'Impression Visitors',   'Impression Orders',
            'Impression Conversions','Impression Revenue', 'Impression Aov',
                 'Impression Rpv', 'Rpv Uplift','Incremental Revenue']]
    return agg2

inc = calculate_segments(df=df
                  , visitor_id="vid"
                  , segment_being_used ="b_segm_final"
                  , group_by = (["eg"]))

execfile("sub_aggregation.py")
execfile("output.py")

#output = export(output_nm='ksp_may2016_01_28_inc')

#export(output_nm="ksp_may2016_01_28_inc.xlsx")