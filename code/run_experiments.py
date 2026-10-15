"""Run final experiments, preserving console logs and stopping on failures."""
import argparse
import json
import subprocess
import sys
import time
from rainfall import ROOT,ART

parser=argparse.ArgumentParser()
parser.add_argument('--from-stage',choices=['prepare','train','report'],default='prepare')
parser.add_argument('--stride',choices=[30,60],type=int,default=30)
parser.add_argument('--training-fraction',type=float,default=1.)
args=parser.parse_args()
stages=['prepare','train','report'];stages=stages[stages.index(args.from_stage):]
ART.mkdir(exist_ok=True);logs=ROOT/'logs';logs.mkdir(exist_ok=True)
status={}
for stage in stages:
    command=[sys.executable,str(__import__('pathlib').Path(__file__).parent/'rainfall.py'),stage]
    if stage=='train':command+=['--stride',str(args.stride),'--training-fraction',str(args.training_fraction)]
    start=time.time();print('Starting',stage,flush=True)
    with (logs/(stage+'.log')).open('w',encoding='utf-8') as f:
        result=subprocess.run(command,stdout=f,stderr=subprocess.STDOUT)
    status[stage]={'returncode':result.returncode,'seconds':time.time()-start}
    (ART/'run_status.json').write_text(json.dumps(status,indent=2),encoding='utf-8')
    if result.returncode:
        print('Failed:',stage,'See',logs/(stage+'.log'));sys.exit(result.returncode)
    print('Finished',stage,flush=True)
