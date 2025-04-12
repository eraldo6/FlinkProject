

class Analyze_Constants():
    def __init__(self): 
        GRAPH_FILES_DIR= "graph_files/"
        self.SRC_KEYWORDS= ["catalog_p", "catalog_r", "catalog_s", "customer_d", "date_dim", "item", "promotion", "store_r", "store_s", "store", "web_r", "web_s", "web_site"]
        self.COLOR_MAPPING = { 'SF_1': 'red', 'SF_5': 'blue', 'SF_10': 'green', 'SF_20': 'purple'} 
        self.SF_SSB= "SF_SSB"
        self.SRC_OUTPUT= GRAPH_FILES_DIR + "src_output.csv"
        self.SRC_GRAPH= GRAPH_FILES_DIR + "src_op_str"
        # 
        self.FILTER_OUT_FILE= GRAPH_FILES_DIR + "out_filter.csv"
        self.FILTER_GRAPH= GRAPH_FILES_DIR + "op_filter"
        # self.FILTER_SELECTED_COL= ["Vertex_Name"]
        #
        self.GROUP_AGGREGATE_OUT_FILE= GRAPH_FILES_DIR + "out_groupByAggregate.csv"
        self.GROUP_AGGREGATE_GRAPH= GRAPH_FILES_DIR + "op_groupBy"
        #
        #
        self.GRAPH_AXIS_NAMES= {"sel_expr_cols": "Number of Aggregation"}

# class SSB_Constans():
#     def __init__(self):
#         self.

class Flink_Job_Info():
    def __init__(self): 
        self.READ_BYTES=    "read-bytes" 
        self.READ_RECORDS=  "read-records"
        self.BUSY_TIME=     "accumulated-busy-time"
        self.WRITE_RECORDS= "write-records"
        self.WRITE_BYTES=   "write-bytes"
        self.metrics_to_extract = [
            self.READ_RECORDS,  self.WRITE_RECORDS, 
            self.BUSY_TIME,
            self.READ_BYTES,    self.WRITE_BYTES, 
            "accumulated-idle-time", 
            "accumulated-backpressured-time"
        ]
        # 
        #
        self.VERTEX_NAME= "Vertex_Name"
        self.DURATION= "Duration_ms"
        self.VERTEX_ID= "Vertex_ID"
        self.SUCCESSORS = "Successors"
        self.PREDECESSORS= "Predecessors"
        self.VERTEX_OP= "Vertex_OP"
        self.DESCRIPTION= "description"
        self.OP_FEATURE= "op_feature"
        #
        #



