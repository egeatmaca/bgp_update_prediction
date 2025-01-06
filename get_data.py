import pybgpstream
import pandas as pd
import datetime as dt
import os
from time import time
import gc


def get_bgp_updates(from_time, until_time, max_rows=10**6, data_dir='./data/', verbose=True):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    stream = pybgpstream.BGPStream(
        from_time=from_time, until_time=until_time,
        collectors=["route-views.sg", "route-views.eqix"],
        record_type="updates"
    )

    update_list = []
    columns=['record_type', 'type', 'time', 'project', 'collector', 'router', 'router_ip', 'peer_asn', 'peer_address', 'prefix', 'next_hop', 'as_path', 'communities', 'old_state', 'new_state']
    row_counter = 0
    for elem in stream:
        update = (elem.record_type, elem.type, elem.time, elem.project, elem.collector, elem.router, elem.router_ip,
                      elem.peer_asn, elem.peer_address, elem._maybe_field("prefix"), elem._maybe_field("next-hop"), 
                      elem._maybe_field("as-path"), ' '.join(elem.fields["communities"]) if "communities" in elem.fields else None,
                      elem._maybe_field("old-state"), elem._maybe_field("new-state"))
        update_list.append(update)
        row_counter += 1

        if row_counter == max_rows:
            updates = pd.DataFrame(update_list, columns=columns)
            min_time = pd.to_datetime(updates.time.min()*10**9).strftime('%Y-%m-%d %H:%M:%S')
            max_time = pd.to_datetime(updates.time.max()*10**9).strftime('%Y-%m-%d %H:%M:%S')
            file_path = os.path.join(data_dir, f'{min_time} - {max_time}.csv')
            updates.to_csv(file_path, index=False)
        
            if verbose:
                print(f'{dt.datetime.now()} | Data saved successfully: {file_path}')

            update_list = []
            row_counter = 0
            del updates
            gc.collect()


if __name__ == '__main__':
    start_time = time()
    get_bgp_updates(from_time="2024-07-08 09:00:00", until_time="2024-07-21 00:00:00")
    print(f'Finished in {time() - start_time}s!')






