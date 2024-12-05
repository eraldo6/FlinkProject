
from draw_graphs import *
DEBUG= False 
WRITE_FILTER=   False
WRITE_GROUPBy=  False 

def main():

    # DIR_NAME= "/home/ffaghih/op_flink_analysis/metrics_SSB/filter/"
    DIR_NAME= "/home/ffaghih/op_flink_analysis/metrics_SSB/aggr_change/"
    for filename in os.listdir(DIR_NAME):                   # for every file in the directory.
        if filename.endswith(".json"):
            print(f"\n'*******'\n'*******'\n'*******'\n'*******'\n'File_Name': {filename}")
            input_json_path = os.path.join(DIR_NAME, filename) 
            df_metrics = parse_job_json(input_json_path) 

    # draw_graphs(f.VERTEX_NAME, cons.SRC_KEYWORDS, cons.SRC_OUTPUT, cons.SRC_GRAPH) 
    draw_graph_filter(x_col= "pred_cnt", y_col_1="rate_tuple_S", y_col_2= "rate_MB_S", input_file= cons.FILTER_OUT_FILE, graph_name= cons.FILTER_GRAPH)
    draw_graph_groupByAggregate(y_col_1="rate_tuple_S", y_col_2= "rate_MB_S", input_file= cons.GROUP_AGGREGATE_OUT_FILE, graph_name= cons.GROUP_AGGREGATE_GRAPH)


def parse_job_json(json_path):

    with open(json_path, 'r') as file:
        job_data= json.load(file)
    vertices= job_data.get("vertices", [])
    job_plan= job_data.get("plan", {})

    df= get_job_metrics(json_path, vertices)
    if DEBUG:   print(f"'DF':\n", tabulate(df, headers='keys', tablefmt='psql', showindex=False), sep="")
    df= add_job_plan(job_plan, df)
    print(f"\n'DF after adding the job_plan':\n", tabulate(df, headers='keys', tablefmt='psql', showindex=False), sep="") 
    if WRITE_FILTER:    write_to_csv(df, f.VERTEX_NAME, ["Calc"], cons.FILTER_OUT_FILE )
    if WRITE_GROUPBy:   write_to_csv(df, f.VERTEX_NAME, ["GroupAggregate"], cons.GROUP_AGGREGATE_OUT_FILE)


def get_job_metrics(json_path, vertices):

    metrics_list = []
    for vertex in vertices:
        vertex_data = {
            f.VERTEX_NAME: vertex["name"],
            f.READ_RECORDS: None,
            f.WRITE_RECORDS: None,
            f.VERTEX_ID: vertex["id"],
            # "Parallelism": vertex["parallelism"],
            # "Status": vertex["status"],
            "Duration_ms": vertex["duration"]
        } 
        vertex_metrics = vertex.get("metrics", {})
        for metric in f.metrics_to_extract:
            metric_value = vertex_metrics.get(metric, None)
            # vertex_data[metric.replace("-", "_").title()] = metric_value 
            vertex_data[metric] = metric_value 
        metrics_list.append(vertex_data) 

    df = pd.DataFrame(metrics_list) 
    # print(f"\nFile input DF:\n", tabulate(df, headers='keys', tablefmt='psql', showindex=False), sep="")
    df.insert(1, 'rate_MB_S', None) 
    df.insert(1, 'rate_tuple_S', None) 
    for index, row in df.iterrows(): 
        # print(row[f.VERTEX_NAME]) 
        if row[f.BUSY_TIME] == 0:
            print(f"{f.BUSY_TIME} equals_to_zero: {row[f.VERTEX_NAME]}")
            df.loc[index, 'rate_MB_S'] = -1 
            df.loc[index, 'rate_tuple_S'] = -1 
            continue 
        if "Source" in row[f.VERTEX_NAME]:
            df.loc[index, 'rate_MB_S'] = (row[f.WRITE_BYTES]/(2**20)) / (row[f.BUSY_TIME]/(10**3))
            df.loc[index, 'rate_tuple_S'] = row[f.WRITE_RECORDS] / (row[f.BUSY_TIME]/(10**3))             
        else:
            df.loc[index, 'rate_MB_S'] = (row[f.READ_BYTES]/(2**20)) / (row[f.BUSY_TIME]/(10**3))
            df.loc[index, 'rate_tuple_S'] = row[f.READ_RECORDS] / (row[f.BUSY_TIME]/(10**3)) 

    file_name = os.path.basename(json_path)
    match= re.search(r"SF_\d+", file_name) 
    matched_pattern= "" 

    print(f"json_path: {json_path}")
    print(f"file_name: {file_name}")
    if match:
        matched_pattern = match.group() 
        # if DEBUG:   
        print(f"SF_pattern_found: {matched_pattern}") 

    df[cons.SF_SSB]= matched_pattern
    # write_to_csv(df, f.VERTEX_NAME, cons.SRC_KEYWORDS, cons.SRC_OUTPUT)
    
    return df 


def add_job_plan(job_plan, df_in):
    df= df_in.copy()
    df.insert(3, f.DESCRIPTION, "")
    df.insert(3, f.VERTEX_OP, "")
    df.insert(3, f.OP_FEATURE, None);   df[f.OP_FEATURE] = df[f.OP_FEATURE].astype(object)
    df.insert(3, f.SUCCESSORS, "")
    df.insert(3, f.PREDECESSORS, "")

    p_nodes= pd.DataFrame(job_plan.get("nodes", []))
    # print(f"'p_nodes':\n", tabulate(p_nodes, headers='keys', tablefmt='psql', showindex=False), sep="")
    for i, row in df.iterrows():
        vertex_id= row[f.VERTEX_ID] 
        # print(f"\nvertex_id: {vertex_id}")
        n= p_nodes[p_nodes["id"]==vertex_id]
        n_inputs= n["inputs"].iloc[0]
        
        if isinstance(n_inputs, list):
            if pd.isna(n_inputs).all():
                print(f"'No Input nodes ' {row[f.VERTEX_NAME]}") 
                continue
        elif pd.isna(n_inputs):
            if DEBUG:   print(f"'No_Inputs_nodes', should be Source: {row[f.VERTEX_NAME]}")
            df.loc[i, f.VERTEX_OP] = "Source"
            continue
        
        df.at[i, f.DESCRIPTION]= n["description"].iloc[0]
        vertex_op, op_dict= parse_description(n["description"].iloc[0])
        df.loc[i, f.VERTEX_OP] = vertex_op 
        # print(op_dict)
        # print(type(op_dict))
        # print(df.dtypes)

        # df.loc[i, f.OP_FEATURE] = op_dict 
        df.at[i, f.OP_FEATURE] = op_dict

        # add predecessors and successors: 
        preds= []
        for j, d in enumerate(n_inputs):
            in_id= d["id"]
            in_name=  df.loc[df[f.VERTEX_ID] == in_id, f.VERTEX_NAME].values[0] 
            # print(f"in_id_name: {in_name}, in_node:", d, )
            preds.append(in_name)
            # update the sucessors: 
            current_val= df.loc[df[f.VERTEX_NAME] == in_name, f.SUCCESSORS].values[0]
            if current_val == "":
                df.loc[df[f.VERTEX_NAME] == in_name, f.SUCCESSORS] = row[f.VERTEX_NAME]
            else:
                df.loc[df[f.VERTEX_NAME] == in_name, f.SUCCESSORS] = current_val + "," + row[f.VERTEX_NAME]

        df.at[i, f.PREDECESSORS] = ",".join(preds)          # update the predecessors

    return df


def parse_description(descript_txt):
    vertex_op= ""
    op_dict= {}
    if DEBUG:   print(f"'descript_txt': {descript_txt}")

    match = re.search(r":\s*([^\(]+)", descript_txt)
    vertex_op = match.group(1) if match else None
    if DEBUG:   print(f"vertex_op: {vertex_op}")

    if vertex_op == "Calc":
        predicate_cnt= parse_where(descript_txt)
        sel_cols, expr_cols= parse_select(descript_txt) 
        op_dict= {"pred_cnt": predicate_cnt, "sel_cols": sel_cols, "sel_expr_cols":expr_cols}
    if vertex_op == "GroupAggregate":
        sel_cols, expr_cols= parse_select(descript_txt)
        groupBy_cnt= parse_groupBy(descript_txt)
        op_dict= {"groupBy_cnt": groupBy_cnt, "sel_cols": sel_cols, "sel_expr_cols":expr_cols}
    return vertex_op, op_dict


def parse_where(descript_txt):

    predicate_cnt= None
    where_match = re.search(r"where=\[(.*)\]", descript_txt)  # Extract the content inside where=[...]
    where_clause = where_match.group(1) if where_match else None

    if where_clause:
        # Split the where clause by AND/OR logical operators (you can adjust if more operators are used)
        predicates = re.split(r"\s+(AND|OR)\s+", where_clause)
        predicate_cnt = len([p for p in predicates if p.strip() and p.strip() not in {"AND", "OR"}])
    else:
        predicate_cnt = 0

    if predicate_cnt is None:
        print("no Predicate s")

    if DEBUG:   print(f"predicate_cnt:{predicate_cnt}")
    return predicate_cnt 

def parse_select(descript_txt):

    match = re.search(r"select=\[(.*?)\]", descript_txt) 
    if not match:
        return 0, 0  
    
    select_clause = match.group(1) 
    items = [item.strip() for item in select_clause.split(",")]
    total_cols = len(items)
    expr_cols = sum(
        1 for item in items if re.search(r"[()\*+/=-]", item)
    )

    return total_cols, expr_cols


def parse_groupBy(descript_txt):

    groupBy_cnt= None 

    groupby_match = re.search(r"groupBy=\[(.*?)\]", descript_txt)
    if groupby_match:
        groupby_clause = groupby_match.group(1)
        groupby_columns = [col.strip() for col in groupby_clause.split(",")]
        groupBy_cnt = len(groupby_columns)
    else:
        groupBy_cnt = 0  
    
    return groupBy_cnt

# if __name__ == "__main__":
main()
