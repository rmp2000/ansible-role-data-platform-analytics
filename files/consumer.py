"""Pulsar analytics subscriber. Acknowledge only after synchronous durable storage."""
import base64
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import re
import signal
import ssl
import time
import urllib.parse
import urllib.request

CONFIG=Path('/etc/data-platform-analytics/config.json')
STATUS=Path('/var/lib/data-platform-analytics/status.json')
STOP=False
logging.basicConfig(level=logging.WARNING,format='%(levelname)s %(message)s')
LOG=logging.getLogger('analytics')
logging.getLogger('pulsar').setLevel(logging.CRITICAL)

def normalize(payload):
    value=json.loads(payload)
    if not isinstance(value,dict) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}',value.get('event_id','')):
        raise ValueError('invalid_event_id')
    kind=value.get('record_type')
    if kind=='measurement_summary':
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,64}',value.get('sensor','')):raise ValueError('invalid_sensor')
        if type(value.get('count')) is not int or not 1<=value['count']<2**63:raise ValueError('invalid_count')
        if any(type(value.get(k)) not in (int,float) or not math.isfinite(value[k]) for k in ('sum','mean')):raise ValueError('invalid_summary')
    elif kind=='rejected':
        if not isinstance(value.get('reason'),str) or len(value['reason'])>4096:raise ValueError('invalid_rejection')
    else:raise ValueError('unknown_record_type')
    names=('event_id','record_type','reason') if kind=='rejected' else ('event_id','record_type','sensor','count','sum','mean')
    row={k:value[k] for k in names if k in value}
    row.update(payload_sha256=hashlib.sha256(payload).hexdigest(),payload=payload.decode('utf-8'))
    return row

class ClickHouse:
    def __init__(self,config):
        self.config=config
        self.context=ssl.create_default_context(cafile=config['clickhouse_ca'])
    def query(self,sql,**parameters):
        args={'wait_end_of_query':'1',**{'param_'+k:str(v) for k,v in parameters.items()}}
        req=urllib.request.Request(self.config['clickhouse_url']+'/?'+urllib.parse.urlencode(args),data=sql.encode(),
            headers={'X-ClickHouse-User':self.config.get('clickhouse_user','platform_writer'),'X-ClickHouse-Key':self.config['clickhouse_password']})
        with urllib.request.urlopen(req,context=self.context,timeout=20) as response:value=response.read().decode()
        if value.startswith('Code:'):raise RuntimeError('Target query failed')
        return value
    def store(self,payload,message_key):
        digest=hashlib.sha256(payload).hexdigest()
        try:row=normalize(payload)
        except (ValueError,TypeError,UnicodeError,OverflowError,RecursionError):
            self.reject(message_key,digest,payload,'invalid_processed_event')
            return 'quarantined'
        previous=self.query('SELECT payload_sha256 FROM analytics.events FINAL WHERE event_id={id:String} LIMIT 1',id=row['event_id']).strip()
        if previous:
            if previous==digest:return 'duplicates'
            self.reject(row['event_id'],digest,payload,'conflicting_event_id')
            return 'quarantined'
        self.query('INSERT INTO analytics.events FORMAT JSONEachRow\n'+json.dumps(row,allow_nan=False))
        return 'stored'
    def reject(self,event_id,digest,payload,reason):
        row={'event_id':event_id,'payload_sha256':digest,'reason':reason,'payload_base64':base64.b64encode(payload).decode()}
        self.query('INSERT INTO analytics.rejected FORMAT JSONEachRow\n'+json.dumps(row))

def open_client(config):
    import pulsar
    token=Path(config['pulsar_token_file']).read_text().strip()
    return pulsar.Client(config['broker_url'],authentication=pulsar.AuthenticationToken(token),
        tls_trust_certs_file_path=config['pulsar_ca'],tls_allow_insecure_connection=False,tls_validate_hostname=True,
        operation_timeout_seconds=15,connection_timeout_ms=5000,logger=logging.getLogger('pulsar'))

def save(status):
    status['updated_at']=time.time()
    temporary=STATUS.with_suffix('.tmp')
    with temporary.open('w') as handle:
        os.chmod(temporary,0o600);json.dump(status,handle);handle.flush();os.fsync(handle.fileno())
    temporary.replace(STATUS)

def main():
    import pulsar
    global STOP
    signal.signal(signal.SIGTERM,lambda *_:globals().__setitem__('STOP',True))
    config=json.loads(CONFIG.read_text())
    previous=json.loads(STATUS.read_text()) if STATUS.exists() else {}
    status={**{'stored':0,'duplicates':0,'quarantined':0,'failures':0},**previous,'state':'starting'}
    target=ClickHouse(config)
    while not STOP:
        client=None
        try:
            save(status)
            client=open_client(config)
            subscriber=client.subscribe(config['topic'],config['subscription'],
                consumer_type=pulsar.ConsumerType.Exclusive,initial_position=pulsar.InitialPosition.Earliest,
                negative_ack_redelivery_delay_ms=2000,receiver_queue_size=100)
            status['state']='ready';save(status)
            while not STOP:
                try:message=subscriber.receive(timeout_millis=1000)
                except pulsar.Timeout:
                    save(status);continue
                payload=message.data()
                key='raw_'+hashlib.sha256(config['topic'].encode()+message.message_id().serialize()).hexdigest()
                try:
                    result=target.store(payload,key)
                    subscriber.acknowledge(message)
                    status[result]+=1;status['last_event_id']=key;status['state']='ready';save(status)
                except Exception as error:
                    subscriber.negative_acknowledge(message)
                    status['failures']+=1;status['state']='retrying';status['last_error']=type(error).__name__;save(status)
                    LOG.warning('Target write will retry: %s',type(error).__name__)
                    time.sleep(2)
        except Exception as error:
            status['failures']+=1;status['state']='reconnecting';status['last_error']=type(error).__name__;save(status)
            LOG.warning('Consumer reconnect: %s',type(error).__name__)
            time.sleep(3)
        finally:
            if client is not None:client.close()

if __name__=='__main__':main()
