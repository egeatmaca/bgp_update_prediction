import pybgpstream
import pandas as pd
import datetime as dt
import os
from time import time
import gc


def get_bgp_updates(from_time, until_time):    
    stream = pybgpstream.BGPStream(
        from_time=from_time, until_time=until_time,
        collectors=["route-views.sg", "route-views.eqix"],
        record_type="updates"
    )

    update_list = []
    for elem in stream:
        update = (elem.record_type, elem.type, elem.time, elem.project, elem.collector, elem.router, elem.router_ip,
                      elem.peer_asn, elem.peer_address, elem._maybe_field("prefix"), elem._maybe_field("next-hop"), 
                      elem._maybe_field("as-path"), ' '.join(elem.fields["communities"]) if "communities" in elem.fields else None,
                      elem._maybe_field("old-state"), elem._maybe_field("new-state"))
        update_list.append(update)

    columns=['record_type', 'type', 'time', 'project', 'collector', 'router', 'router_ip', 'peer_asn', 'peer_address', 'prefix', 'next-hop', 'as-path', 'communities', 'old_state', 'new_state']
    updates = pd.DataFrame(update_list, columns=columns)
    
    return updates


def save_bgp_updates(from_time, until_time, iter_mins=15, data_dir='./data/', verbose=True):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    
    from_time = dt.datetime.strptime(from_time, '%Y-%m-%d %H:%M:%S')
    until_time = dt.datetime.strptime(until_time, '%Y-%m-%d %H:%M:%S')
    
    microsecond = dt.timedelta(microseconds=1)
    time_delta = dt.timedelta(minutes=iter_mins) - microsecond
    from_time_iter = from_time
    until_time_iter = from_time_iter + time_delta

    while from_time_iter <= until_time:
        until_time_iter = until_time_iter if until_time_iter <= until_time else until_time_iter
        
        from_time_iter_str = from_time_iter.strftime('%Y-%m-%d %H:%M:%S')
        until_time_iter_str = until_time_iter.strftime('%Y-%m-%d %H:%M:%S')

        updates = get_bgp_updates(from_time_iter_str, until_time_iter_str)
        updates.to_csv(os.path.join(data_dir, f'{from_time_iter_str} - {until_time_iter_str}'), index=False)
        
        if verbose:
            print(f'Data saved successfully: {from_time_iter_str} - {until_time_iter_str}')

        from_time_iter = until_time_iter + microsecond
        until_time_iter = from_time_iter + time_delta

        del updates
        gc.collect()


if __name__ == '__main__':
    start_time = time()
    df = save_bgp_updates(from_time="2024-07-08 09:00:00", until_time="2024-07-21 00:00:00")
    print(f'Finished in {time() - start_time}s!')






