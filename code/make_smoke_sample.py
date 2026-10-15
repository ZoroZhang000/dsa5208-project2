"""Extract one station, January per year; kept outside final artifacts."""
import csv
from pathlib import Path
from rainfall import ROOT

target=ROOT/'smoke'/'data'/'raw'
target.mkdir(parents=True,exist_ok=True)
for source in sorted((ROOT/'data'/'raw').glob('*.csv')):
    if source.stat().st_size<1_000_000: continue
    with source.open(encoding='utf-8-sig',newline='') as f:
        reader=csv.DictReader(f)
        output=target/source.name
        with output.open('w',encoding='utf-8',newline='') as g:
            writer=csv.DictWriter(g,fieldnames=reader.fieldnames);writer.writeheader();n=0
            for row in reader:
                if row['date'][5:7]>'01':break
                if row['station_id']=='S77':writer.writerow(row);n+=1
        print(output.name,n)
