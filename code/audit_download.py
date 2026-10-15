"""Record checksums and the observed schema/unit of downloaded official files."""
import csv
import hashlib
from rainfall import ROOT,save_json

manifest=[]
for p in sorted((ROOT/'data'/'raw').glob('*.csv')):
    digest=hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):digest.update(chunk)
    with p.open(encoding='utf-8-sig',newline='') as f:first=next(csv.DictReader(f))
    manifest.append({'dataset_id':p.stem,'bytes':p.stat().st_size,'sha256':digest.hexdigest(),
        'first_timestamp':first['timestamp'],'reading_type':first['reading_type'],'reading_unit':first['reading_unit']})
    print(p.name,flush=True)
save_json('download_manifest.json',manifest)
