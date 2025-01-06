import numpy as np
import pandas as pd
import os
import datetime as dt
import gc
from glob import glob
from multiprocessing import Process


def read_file_paths(input_dir):
    return sorted(glob(input_dir))

def read_data(file_paths, start_file_idx=0, max_rows=3*10**6):
    all_dfs = []
    row_counter = 0
    file_counter = 0
    
    for f in file_paths[start_file_idx:]:
        df = pd.read_csv(f)
        all_dfs.append(df)
        row_counter += df.shape[0]
        file_counter += 1
        
        if row_counter >= max_rows:
            break
    
    df = pd.concat(all_dfs).reset_index(drop=True)
    next_file_idx = start_file_idx + file_counter
    
    del all_dfs
    gc.collect()

    return df, next_file_idx

def add_origin_asn(df, prev_asn_by_prefix=None):
    df = df.copy()
    df['origin_asn'] = df.as_path.apply(lambda x: x.split(' ')[-1] if type(x)==str else None)
    
    origin_asn_by_prefix = df.groupby('prefix')['origin_asn'].last().to_frame()
    if prev_asn_by_prefix is not None:
        origin_asn_by_prefix = pd.concat([prev_asn_by_prefix, origin_asn_by_prefix]).groupby('prefix').last()
    
    df_origin_asn_fill = df[df.origin_asn.isna()].join(origin_asn_by_prefix, on='prefix', rsuffix='_fill')
    df.loc[df_origin_asn_fill.index, 'origin_asn'] = df_origin_asn_fill['origin_asn']

    return df, origin_asn_by_prefix

def save_asn_data(df, origin_asn, output_dir, file_name):
    asn_path = os.path.join(output_dir, origin_asn)
    file_path = os.path.join(asn_path, file_name)

    print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '|', 'Started:', file_path)
    
    if not os.path.exists(asn_path): 
        os.makedirs(asn_path)

    df_asn = df.loc[df.origin_asn==origin_asn]

    if df_asn.shape[0] > 0:
        df_asn.to_csv(file_path, index=False)
    
    print(dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), '|', 'Successfully saved:', file_path)

def split_asn_data(input_dir='./data/updates/*', output_dir='./data/asn_updates', asn_sample_size=100, n_processes=4):
    file_paths = read_file_paths(input_dir)
    n_files = len(file_paths)
    next_file_idx = 0
    origin_asn_by_prefix = None
    asn_sample = None

    while next_file_idx < n_files:
        df, next_file_idx = read_data(file_paths, start_file_idx=next_file_idx)
        df, origin_asn_by_prefix = add_origin_asn(df, prev_asn_by_prefix=origin_asn_by_prefix) 
        min_time = pd.to_datetime(df.time.min()*10**9).strftime('%Y-%m-%d %H:%M:%S')
        max_time = pd.to_datetime(df.time.max()*10**9).strftime('%Y-%m-%d %H:%M:%S')
        file_name = f'{min_time} - {max_time}.csv'

        if asn_sample is None:
            asn_sample = np.random.choice(df.origin_asn.dropna().unique(), 
                                          size=asn_sample_size, 
                                          replace=False)
        
        args_list = [(df, origin_asn, output_dir, file_name) 
                     for origin_asn in asn_sample]

        while len(args_list) > 0:
            processes = []

            for i in range(n_processes):
                process_args = args_list.pop(0)
                process = Process(target=save_asn_data, args=process_args)
                process.start()
                processes.append(process)

            for process in processes:
                process.join()

            
if __name__ == '__main__':
    split_asn_data()



