import numpy as np
import pandas as pd
import os
from glob import glob
import gc


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


def split_data(input_dir='./data/updates/*', output_dir='./data/splitted'):
    file_paths = read_file_paths(input_dir)
    n_files = len(file_paths)
    next_file_idx = 0
    origin_asn_by_prefix = None

    while next_file_idx < n_files:
        df, next_file_idx = read_data(file_paths, start_file_idx=next_file_idx)
        df, origin_asn_by_prefix = add_origin_asn(df, prev_asn_by_prefix=origin_asn_by_prefix)
        
        min_time = pd.to_datetime(df.time.min()*10**9).strftime('%Y-%m-%d %H:%M:%S')
        max_time = pd.to_datetime(df.time.max()*10**9).strftime('%Y-%m-%d %H:%M:%S')

        for origin_asn in df.origin_asn.unique():
            df_asn = df.loc[df.origin_asn==origin_asn]
            for prefix in df_asn.prefix.unique():
                prefix_path = os.path.join(output_dir, origin_asn, prefix.replace('/', '|'))
                file_path = os.path.join(prefix_path, f'{min_time} - {max_time}.csv')
                
                if not os.path.exists(prefix_path):
                    os.makedirs(prefix_path)
    
                df_asn.loc[df.prefix==prefix].to_csv(file_path, index=False)


if __name__ == '__main__':
    split_data()



