import json 
import pandas as pd 
from tabulate import tabulate 
import matplotlib.pyplot as plt
import os 
import re 
from experiment_params import *
import csv

cons= Analyze_Constants() 
f= Flink_Job_Info() 

def write_to_csv(df, column_name, keywords, out_file):

    print(f"write_into_file. column_name:{column_name}, keywords:{keywords}, out_file:{out_file}")
    mask= []
    for name in df[column_name]:
        found = False
        for keyword in keywords:
            if keyword in name:
                found = True
                break
        mask.append(found)

    mask_series = pd.Series(mask)
    filtered_df = df[mask_series]
    if "src" in out_file:
        filtered_df.to_csv(out_file, mode='a', index=False, header= False)
        # filtered_df.to_csv(out_file, mode='a', index=False)
    elif "Calc" in keywords:
        expand_write_to_csv(filtered_df, out_file)
    elif "GroupAggregate" in keywords:
        expand_write_to_csv(filtered_df, out_file) 


def expand_write_to_csv(filtered_df, out_file):

    df_copy= filtered_df.copy().reset_index()
    expanded_columns = pd.json_normalize(df_copy['op_feature'])
    df_copy = pd.concat([df_copy, expanded_columns], axis=1)
    df_copy = df_copy.drop(columns=['op_feature'])

    print(f"\n'DF after expanding', write to {out_file} \n", tabulate(df_copy, headers='keys', tablefmt='psql', showindex=False), sep="") 
    df_copy.to_csv(out_file, mode='a', index=False, header= False)
    # df_copy.to_csv(out_file, mode='a', index=False)


################################################################################################################################
################################################################################################################################

def draw_graphs(column_name, keywords, input_file, graph_name):

    print(f"\n'Draw_Graphs' column_name: {column_name}, keywords:{keywords}, input_file:{input_file}, graph_name:{graph_name}")
    df= pd.read_csv(input_file) 
    df['color'] = df[cons.SF_SSB].map(cons.COLOR_MAPPING) 

    fig, axes = plt.subplots(1, 2, figsize=(15, 8)) 
    for keyword in keywords:
        filtered_df = df[df[column_name].str.contains(keyword, case=False)]
        print(f"keyword:{keyword}, len:{len(filtered_df['color'])}") 
        axes[0].scatter([keyword] * len(filtered_df), filtered_df['rate_tuple_S'], c=filtered_df['color'], label=keyword, alpha=0.4, marker= '*')
        axes[1].scatter([keyword] * len(filtered_df), filtered_df['rate_MB_S'], c=filtered_df['color'], label=keyword, alpha=0.4, marker= '*')

    axes[0].set_xlabel('Table Name', fontsize=14);      axes[0].set_ylabel('rate_tuple_S', fontsize=14)
    axes[1].set_xlabel('Table Name', fontsize=14);      axes[1].set_ylabel('rate_MB_S', fontsize=14)
    axes[0].tick_params(axis='both', which='major', labelsize=14)  
    axes[1].tick_params(axis='both', which='major', labelsize=14)
    axes[0].set_yscale('log') 
    axes[1].set_yscale('log') 

    fig.suptitle(f"SF_1: {cons.COLOR_MAPPING['SF_1']}, SF_10: {cons.COLOR_MAPPING['SF_10']}", fontsize=16)
    fig.tight_layout()
    fig.savefig(f"{graph_name}.png")


def draw_graph_filter(x_col, y_col_1, y_col_2, input_file, graph_name):

    df = pd.read_csv(input_file) 
    print(f"df_length befor filter {len(df)}")
    df = df[df['rate_MB_S'] >= 0]
    df['color'] = df[cons.SF_SSB].map(cons.COLOR_MAPPING) 
    print(f"\n'draw df filter' {len(df)}:\n", tabulate(df, headers='keys', tablefmt='psql', showindex=False), sep="") 

    fig, axes = plt.subplots(2, 2, figsize=(14, 6)) 
    axes[0,0].scatter(df[x_col], df[y_col_1], c=df['color'], alpha=0.4, marker= '*') 
    axes[0,1].scatter(df[x_col], df[y_col_2], c=df['color'], alpha=0.4, marker= '*') 

    for y_col in [y_col_1, y_col_2]:
        print(f"\n'y_col': {y_col}") 
        stat_l= [];     outlier_rmv= []; 
        for x_val in df[x_col].unique():
            f_df= df[ df[x_col] == x_val ] 
            stat_dict, no_outlier_dict= get_statistics(f_df, y_col, x_val, "pred_count")
            stat_l.append(stat_dict);   outlier_rmv.append(no_outlier_dict) 
        stat_df= pd.DataFrame(stat_l) 
        no_outlier_df= pd.DataFrame(outlier_rmv)
        print(f"'stat_df':\n", tabulate(stat_df, headers='keys', tablefmt='psql', showindex=False), sep="") 
        print(f"'stat = no_outlier_df':\n", tabulate(no_outlier_df, headers='keys', tablefmt='psql', showindex=False), sep="") 

    axes[0,0].set_xlabel(x_col, fontsize=14);     axes[0,0].set_ylabel(y_col_1, fontsize=14) 
    axes[0,1].set_xlabel(x_col, fontsize=14);     axes[0,1].set_ylabel(y_col_2, fontsize=14) 
    axes[0,0].tick_params(axis='both', which='major', labelsize=14);  
    axes[0,1].tick_params(axis='both', which='major', labelsize=14); 
    axes[0,0].set_yscale('log');      axes[0,1].set_yscale('log'); 


    x_col_2= "read-records"
    scat_1_0= axes[1,0].scatter(df[x_col_2], df[y_col_1], c=df[x_col], cmap='plasma', alpha=0.4, marker= '*') 
    axes[1,1].scatter(df[x_col_2], df[y_col_2], c=df[x_col], cmap='viridis', alpha=0.4, marker= '*') 
    cbar = fig.colorbar(scat_1_0, ax=axes[1, 0], orientation='vertical');   cbar.set_label('pred_cnt')
    
    axes[1,0].set_xlabel(x_col_2, fontsize=14);     axes[1,0].set_ylabel(y_col_1, fontsize=14) 
    axes[1,1].set_xlabel(x_col_2, fontsize=14);     axes[1,1].set_ylabel(y_col_2, fontsize=14) 
    axes[1,0].tick_params(axis='both', which='major', labelsize=14);  
    axes[1,1].tick_params(axis='both', which='major', labelsize=14); 
    axes[1,0].set_yscale('log');      axes[1,1].set_yscale('log'); 

    fig.suptitle(f"SF_1: {cons.COLOR_MAPPING['SF_1']}, SF_10: {cons.COLOR_MAPPING['SF_10']}", fontsize=16) 
    fig.tight_layout() 
    fig.savefig(f"{graph_name}.png") 


def get_statistics(f_df, y_col, x_val, identifier):

    max_v =     f_df[y_col].max();      min_v =  f_df[y_col].min()
    median_v =  f_df[y_col].median();   mean_v = f_df[y_col].mean()
    std_dev =   f_df[y_col].std()
    tmp_dict=   {identifier: x_val, "min": min_v, "max": max_v, "median": median_v, "mean_v":mean_v, "std_dev": std_dev}

    Q1 = f_df[y_col].quantile(0.25)  
    Q3 = f_df[y_col].quantile(0.75)  
    IQR = Q3 - Q1

    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    df_no_outliers = f_df[(f_df[y_col] >= lower_bound) & (f_df[y_col] <= upper_bound)]

    max_v =     df_no_outliers[y_col].max();      min_v =  df_no_outliers[y_col].min()
    median_v =  df_no_outliers[y_col].median();   mean_v = df_no_outliers[y_col].mean()
    std_dev =   df_no_outliers[y_col].std()
    no_outlier_dict=   {identifier: x_val, "min": min_v, "max": max_v, "median": median_v, "mean_v":mean_v, "std_dev": std_dev}

    return tmp_dict, no_outlier_dict




def draw_graph_groupByAggregate(y_col_1, y_col_2, input_file, graph_name):
    df = pd.read_csv(input_file) 
    print(f"df_length befor filter {len(df)}")
    df = df[df['rate_MB_S'] >= 0]
    print(f"\n'draw df filter' {len(df)}:\n", tabulate(df, headers='keys', tablefmt='psql', showindex=False), sep="") 

    AGGR_CNT= "sel_expr_cols";      x_col= AGGR_CNT; 
    fig, axes = plt.subplots(2, 2, figsize=(14, 6)) 

    axes[0,0].scatter(df[x_col], df[y_col_1], c='red', alpha=0.4, marker= '*') 
    axes[0,1].scatter(df[x_col], df[y_col_2], c='red', alpha=0.4, marker= '*') 
    axes[0,0].set_xlabel(cons.GRAPH_AXIS_NAMES[x_col], fontsize=14);     axes[0,0].set_ylabel(y_col_1, fontsize=14) 
    axes[0,1].set_xlabel(cons.GRAPH_AXIS_NAMES[x_col], fontsize=14);     axes[0,1].set_ylabel(y_col_2, fontsize=14) 
    axes[0,0].tick_params(axis='both', which='major', labelsize=14);  
    axes[0,1].tick_params(axis='both', which='major', labelsize=14); 
    axes[0,0].set_yscale('log');      axes[0,1].set_yscale('log'); 
    
    x_col_2= "read-records"
    axes[1,0].scatter(df[x_col_2], df[y_col_1], c='red', alpha=0.4, marker= '*') 
    axes[1,1].scatter(df[x_col_2], df[y_col_2], c='red', alpha=0.4, marker= '*') 
    axes[1,0].set_xlabel(x_col_2, fontsize=14);     axes[1,0].set_ylabel(y_col_1, fontsize=14) 
    axes[1,1].set_xlabel(x_col_2, fontsize=14);     axes[1,1].set_ylabel(y_col_2, fontsize=14) 
    axes[1,0].tick_params(axis='both', which='major', labelsize=14);  
    axes[1,1].tick_params(axis='both', which='major', labelsize=14); 
    axes[1,0].set_yscale('log');      axes[1,1].set_yscale('log'); 
    
    fig.tight_layout() 
    fig.savefig(f"{graph_name}.png") 


