"""Validate a CSV completely, then publish stable measurement event identities."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import re

def records(path,dataset):
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,24}',dataset):raise ValueError('Invalid dataset identifier')
    data=Path(path).read_bytes()
    result=[];seen=set()
    for row in csv.DictReader(data.decode('utf-8-sig').splitlines()):
        sensor=dataset+'_'+row['sensor']
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}',sensor):raise ValueError('Invalid sensor')
        event_id=dataset+'_'+hashlib.sha256((dataset+'\0'+row['sample_id']+'\0'+row['sensor']).encode()).hexdigest()
        value=float(row['value'])
        if not math.isfinite(value) or abs(value)>1e9:raise ValueError('Invalid numeric measurement')
        if event_id in seen:raise ValueError('Duplicate CSV identity')
        seen.add(event_id)
        result.append({'sensor':sensor,'event_id':event_id,'value':value,'unit':row.get('unit',''),'source':row.get('source','')})
    if not result:raise ValueError('Empty CSV')
    return result

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv',required=True,type=Path)
    parser.add_argument('--dataset',default='iris_demo_v1')
    parser.add_argument('--config',type=Path,default=Path('/etc/data-platform-analytics/config.json'))
    parser.add_argument('--dry-run',action='store_true')
    args=parser.parse_args();values=records(args.csv,args.dataset)
    if args.dry_run:
        print(json.dumps({'records':len(values),'sensors':sorted({x['sensor'] for x in values})}));return
    import consumer
    config=json.loads(args.config.read_text());client=consumer.open_client(config)
    try:
        producer=client.create_producer(config['input_topic'],batching_enabled=False)
        for value in values:producer.send(json.dumps(value,allow_nan=False).encode(),properties={'event_id':value['event_id']})
    finally:client.close()
    print(json.dumps({'published':len(values),'dataset':args.dataset}))

if __name__=='__main__':main()
