"""Root-only live analytics checks. Credentials never leave protected files."""
import hashlib
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys
import time
import urllib.request
import uuid
sys.path.insert(0,'/usr/local/lib/data-platform-analytics')
import consumer
CONFIG=consumer.CONFIG
def wait_for(test,label,timeout=300):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            if test():return
        except Exception:pass
        time.sleep(1)
    raise RuntimeError('Timed out: '+label)
def service(name,action):
    subprocess.run(['systemctl',action,name],check=True,capture_output=True)
def main():
    mode=sys.argv[1];config=json.loads(CONFIG.read_text());target=consumer.ClickHouse(config)
    admin=Path('/etc/apache-pulsar/admin.token').read_text().strip()
    context=ssl.create_default_context(cafile=config['pulsar_ca'])
    def pending():
        request=urllib.request.Request(config['admin_url']+'/admin/v2/persistent/'+config['topic'].removeprefix('persistent://')+'/stats',headers={'Authorization':'Bearer '+admin})
        with urllib.request.urlopen(request,context=context,timeout=10) as response:value=json.load(response)['subscriptions'][config['subscription']]
        return value['msgBacklog']+value.get('unackedMessages',0)
    def status():return json.loads(consumer.STATUS.read_text())
    def publish(payloads):
        client=consumer.open_client(config)
        try:
            producer=client.create_producer(config['topic'],batching_enabled=False)
            for payload in payloads:producer.send(payload)
        finally:client.close()
    def has(identifier,payload):
        digest=hashlib.sha256(payload).hexdigest()
        value=target.query('SELECT count(),any(payload_sha256) FROM analytics.events FINAL WHERE event_id={id:String}',id=identifier).strip()
        return value=='1\t'+digest
    def payload(identifier,value=30):
        return json.dumps({'record_type':'measurement_summary','event_id':identifier,'sensor':identifier,'count':2,'sum':value,'mean':value/2}).encode()
    wait_for(lambda:status()['state']=='ready' and pending()==0,'ready independent analytics subscription')
    tag='verify_'+uuid.uuid4().hex
    original=payload(tag);conflict=payload(tag,99);invalid=b'\x00invalid-'+uuid.uuid4().bytes
    publish([original,original,conflict,invalid])
    wait_for(lambda:has(tag,original),'exact analytic row with logical uniqueness')
    for digest in [hashlib.sha256(x).hexdigest() for x in (conflict,invalid)]:
        wait_for(lambda:target.query('SELECT count() FROM analytics.rejected FINAL WHERE payload_sha256={digest:String}',digest=digest).strip()=='1','durable quarantine record')
    wait_for(lambda:pending()==0,'acknowledged duplicate/conflict/invalid fixtures')
    print('DATA_PLATFORM_ANALYTICS_OK',flush=True)
    if mode!='recovery':return
    identifier='verify_'+uuid.uuid4().hex;event=payload(identifier)
    original_config=CONFIG.read_bytes();metadata=CONFIG.stat();failed=status().get('failures',0)
    def replace(content):
        path=CONFIG.with_name('.recovery-'+uuid.uuid4().hex);path.write_bytes(content);os.chown(path,metadata.st_uid,metadata.st_gid);path.chmod(metadata.st_mode & 0o777);path.replace(CONFIG)
    try:
        bad=dict(config,clickhouse_password='invalid-'+uuid.uuid4().hex)
        replace(json.dumps(bad).encode());service('data-platform-analytics','restart');publish([event])
        wait_for(lambda:status().get('failures',0)>failed and pending()>0,'target failure retained in Pulsar')
        if has(identifier,event):raise RuntimeError('Target wrote despite invalid credentials')
    finally:
        replace(original_config);service('data-platform-analytics','restart')
    wait_for(lambda:has(identifier,event) and pending()==0,'delivery after restoring target credentials')
    print('DATA_PLATFORM_ANALYTICS_TARGET_RETRY_OK',flush=True)
    identifier='verify_'+uuid.uuid4().hex;event=payload(identifier)
    service('data-platform-analytics','stop')
    try:
        publish([event]);wait_for(lambda:pending()>0,'durable pending analytic event')
        service('apache-pulsar','restart')
        wait_for(lambda:pending()>0,'pending analytic event after broker restart')
    finally:service('data-platform-analytics','start')
    wait_for(lambda:has(identifier,event) and pending()==0,'delivery after broker restart')
    print('DATA_PLATFORM_ANALYTICS_BROKER_RECOVERY_OK',flush=True)
if __name__=='__main__':
    try:main()
    except Exception as error:
        print('ANALYTICS_CHECK_FAILED: '+(str(error) if type(error) is RuntimeError else type(error).__name__),file=sys.stderr);sys.exit(1)
