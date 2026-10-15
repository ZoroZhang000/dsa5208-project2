"""Plot aggregated CSVs and generate an honest results report."""
import csv
import json
import os
from pathlib import Path
from xml.sax.saxutils import escape
from rainfall import ART

def table_rows(name):
    folder=ART/name
    paths=list(folder.glob('part-*.csv'))
    if not paths:return []
    with paths[0].open(encoding='utf-8') as f:return list(csv.DictReader(f))

def plot_results():
    os.environ.setdefault('MPLCONFIGDIR',str(ART/'matplotlib_cache'/os.name))
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import numpy as np
    out=ART/'figures'; out.mkdir(exist_ok=True)
    def finish(name):
        plt.tight_layout(); plt.savefig(out/(name+'.png'),dpi=160); plt.close()
    rows=table_rows('month_hour')
    if rows:
        grid=np.full((12,24),np.nan)
        for r in rows:grid[int(r['month'])-1,int(r['hour'])]=float(r['wet_frequency'])
        plt.figure(figsize=(8,4));plt.imshow(grid,aspect='auto',origin='lower',extent=[-.5,23.5,.5,12.5]);plt.colorbar(label='Wet interval fraction');plt.xlabel('Hour (SGT)');plt.ylabel('Month');plt.yticks(range(1,13));finish('month_hour')
    rows=table_rows('station_year_coverage')
    if rows:
        stations=sorted({r['station_id'] for r in rows}); grid=np.full((len(stations),8),np.nan)
        for r in rows:grid[stations.index(r['station_id']),int(r['year'])-2017]=float(r['coverage'])
        plt.figure(figsize=(8,max(4,len(stations)*.10)));plt.imshow(grid,aspect='auto',vmin=0,vmax=1);plt.xticks(range(8),range(2017,2025));plt.yticks(range(len(stations)),stations,fontsize=5);plt.colorbar(label='Full-year coverage');finish('coverage')
        for old in out.glob('coverage_part_*.png'):old.unlink()
        groups=(len(stations)+49)//50;chunk=(len(stations)+groups-1)//groups
        for i,start in enumerate(range(0,len(stations),chunk),1):
            subset=stations[start:start+chunk]
            plt.figure(figsize=(8,max(4,len(subset)*.13)));plt.imshow(grid[start:start+chunk],aspect='auto',vmin=0,vmax=1);plt.xticks(range(8),range(2017,2025));plt.yticks(range(len(subset)),subset,fontsize=8);plt.colorbar(label='Full-year coverage');plt.title('White: no station-year observations');finish('coverage_part_'+str(i))
    for name in ['monthly_rain','annual_station','fixed_station_annual']:
        rows=table_rows(name)
        if not rows:continue
        plt.figure(figsize=(8,4))
        if name=='monthly_rain':
            for year in sorted({r['year'] for r in rows}):
                sub=sorted([r for r in rows if r['year']==year],key=lambda r:int(r['month']))
                plt.plot([int(r['month']) for r in sub],[float(r['mean_daily_rain']) for r in sub],label=year)
            plt.xlabel('Month');plt.ylabel('Mean valid station-day rainfall (mm)');plt.legend(ncol=4)
        else:
            years=sorted({r['year'] for r in rows})
            plt.plot([int(y) for y in years],[sum(float(r['valid_day_total']) for r in rows if r['year']==y)/sum(r['year']==y for r in rows) for y in years],marker='o')
            plt.ylabel('Mean station sum over valid days (mm)');plt.xlabel('Year')
        finish(name)
    rows=table_rows('station_map')
    if rows:
        plt.figure(figsize=(7,4));plt.scatter([float(r['longitude']) for r in rows],[float(r['latitude']) for r in rows],c=[float(r['mean_interval_rain']) for r in rows]);plt.colorbar(label='Mean observed interval rainfall (mm)');plt.xlabel('Longitude');plt.ylabel('Latitude');finish('stations')
    rows=table_rows('daily_quantiles')
    if rows:
        rows.sort(key=lambda r:int(r['year']));plt.figure(figsize=(7,4))
        for i,q in enumerate(['50%','90%','95%','99%']):
            plt.plot([int(r['year']) for r in rows],[json.loads(r['quantiles'])[i] for r in rows],label=q,marker='o')
        plt.ylabel('Valid station-day rainfall quantile (mm)');plt.xlabel('Year');plt.legend();finish('daily_quantiles')
    rows=table_rows('annual_intensity')
    if rows:
        rows.sort(key=lambda r:int(r['year']));plt.figure(figsize=(7,4));plt.plot([int(r['year']) for r in rows],[float(r['wet_interval_intensity']) for r in rows],marker='o');plt.xlabel('Year');plt.ylabel('Mean positive five-minute rainfall (mm)');finish('annual_intensity')
    rows=table_rows('annual_station')
    if rows:
        years=sorted({r['year'] for r in rows});plt.figure(figsize=(7,4))
        plt.plot([int(y) for y in years],[sum(float(r['wet_day_fraction']) for r in rows if r['year']==y)/sum(r['year']==y for r in rows) for y in years],marker='o');plt.xlabel('Year');plt.ylabel('Mean station wet-day fraction');finish('annual_wet_days')
        plt.figure(figsize=(7,4))
        for name,label in [('annual_station','All available stations'),('fixed_station_annual','Fixed station IDs')]:
            sub=table_rows(name)
            if not sub:continue
            ys=sorted({r['year'] for r in sub})
            plt.plot([int(y) for y in ys],[sum(float(r['valid_day_total'])/int(r['valid_days']) for r in sub if r['year']==y)/sum(r['year']==y for r in sub) for y in ys],marker='o',label=label)
        plt.xlabel('Year');plt.ylabel('Mean rainfall per valid station-day (mm)');plt.legend();finish('annual_daily_mean')
    for name in ['test_calibration','five_minute_test_calibration']:
        rows=table_rows(name)
        if not rows:continue
        plt.figure(figsize=(5,5));plt.plot([0,1],[0,1],'k--')
        for a in sorted({r['threshold_mm'] for r in rows}):
            sub=sorted([r for r in rows if r['threshold_mm']==a],key=lambda r:int(r['bin']))
            plt.plot([float(r['mean_probability']) for r in sub],[float(r['observed_rate']) for r in sub],marker='o',label=a+' mm')
        plt.xlabel('Predicted probability');plt.ylabel('Observed frequency');plt.legend();finish(name)
    return [p for p in sorted(out.glob('*.png')) if p.stem!='coverage']

def build_report():
    from reportlab.platypus import SimpleDocTemplate,Paragraph,Spacer,Table,Image,PageBreak
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib import colors
    styles=getSampleStyleSheet(); story=[]
    def para(s,style='BodyText'):story.extend([Paragraph(escape(s),styles[style]),Spacer(1,8)])
    para('Singapore Rainfall: Experimental Results','Title')
    if ART.parent.name=='smoke':para('SMOKE TEST ONLY: one station in January; these are not final full-data results.')
    members=ART.parent/'members.json'
    if members.exists():
        for m in json.loads(members.read_text(encoding='utf-8')):para(m['name']+' / '+m['student_id'])
    else:para('DRAFT: group names and student IDs have not been supplied. Do not submit this draft unchanged.')
    para('Method','Heading1')
    para('2017-2021 training, 2022 validation, 2023-2024 held-out testing. Targets sum the next six five-minute records and apply a strict greater-than threshold. Missing records are not zeros. Complete windows require 12 historical and six future readings. All fitted preprocessing uses training data only.')
    para('Timestamp limitation: the official collection establishes SGT but does not establish whether a timestamp marks the beginning or end of a rainfall accumulation interval. Results forecast the next six recorded observations; physical interval-end semantics remain an assumption.')
    para('Daily totals include station-days with at least 95% coverage. Annual observed sums are not complete climatological annual totals. The fixed station panel contains IDs with qualifying station-days in all eight years; annual valid-day counts must still be considered. A separate strict panel requires at least 95% full-year coverage in every year.')
    para('Early records ending one second before a five-minute grid boundary are aligned within a one-second tolerance. Off-grid counts are audited. Historical update timestamps can lag observation timestamps; the retrospective forecast assumes observations through t have already become available. No real-time latency guarantee is made.')
    para('Environment and experiment status','Heading1')
    for name in ['environment.json','sample_availability.json','split_sizes.json','model_config.json']:
        p=ART/name
        if p.exists():para(name+': '+p.read_text(encoding='utf-8'))
    para('Data quality and descriptive observations','Heading1')
    rows=table_rows('quality_year')
    if rows:
        fields=['year','raw_records','invalid_records','off_grid_records','unique_valid_records','clean_records']
        data=[fields]+[[r.get(k,'') for k in fields] for r in sorted(rows,key=lambda r:r.get('year',''))]
        t=Table(data,repeatRows=1);t.setStyle([('FONTSIZE',(0,0),(-1,-1),6),('GRID',(0,0),(-1,-1),.2,colors.grey)]);story.extend([t,Spacer(1,12)])
    rows=table_rows('monthly_rain')
    if rows:
        means={}
        for m in range(1,13):
            sub=[r for r in rows if int(r['month'])==m]
            if sub:means[m]=sum(float(r['mean_daily_rain'])*int(r['valid_station_days']) for r in sub)/sum(int(r['valid_station_days']) for r in sub)
        wet=max(means,key=means.get);dry=min(means,key=means.get)
        para(f'Across available valid station-days, month {wet} has the highest mean daily rainfall ({means[wet]:.2f} mm), and month {dry} the lowest ({means[dry]:.2f} mm). This comparison is weighted by the number of valid station-days; changing station coverage remains a possible confounder.')
    rows=table_rows('month_hour')
    if rows:
        peak=max(rows,key=lambda r:float(r['wet_frequency']))
        para(f"The highest observed wet-interval fraction among month/hour cells is {float(peak['wet_frequency']):.3f} in month {peak['month']}, hour {peak['hour']} SGT. This is an aggregate descriptive pattern, not a causal result.")
        para('MSS climate context: its long-term reference describes wetter conditions in November-January and more frequent afternoon rainfall. Its reference identifies February as the driest month, whereas the minimum in this study can differ because the period, station mix and completeness filtering differ. This comparison does not establish the cause of any difference. Source: https://www.weather.gov.sg/climate-climate-of-singapore/')
    if (ART/'strict_fixed_station_annual').exists() and not table_rows('strict_fixed_station_annual'):
        para('No station reaches at least 95% full-year coverage in all eight years. The strict-coverage panel is empty; the common-ID comparison remains available but does not eliminate missing-day bias.')
    rows=table_rows('fixed_station_annual')
    if rows:para(f"The common-ID panel contains {len({r['station_id'] for r in rows})} stations. Alongside observed cumulative totals, mean rainfall per valid day is compared to reduce differences in the number of observed days; this still does not correct nonrandom missingness or station relocation.")
    rows=table_rows('extreme_events')
    if rows:
        para('Largest valid station-day totals','Heading2')
        data=[['Station','Date','Rain (mm)','Coverage']]+[[r['station_id'],r['date'],r['daily_rain'],f"{float(r['coverage']):.3f}"] for r in rows[:10]]
        t=Table(data);t.setStyle([('GRID',(0,0),(-1,-1),.2,colors.grey),('FONTSIZE',(0,0),(-1,-1),8)]);story.extend([t,Spacer(1,12)])
        para('These are observed, unvalidated extreme totals. The largest values require cross-station and provider checks before being interpreted as physical weather records. Positive outliers are retained; five-minute readings above 50 mm are flagged as an engineering audit rule, not an official climatological limit.')
    rows=table_rows('extreme_flags_year')
    if rows:
        for r in sorted(rows,key=lambda r:r['year']):para(f"{r['year']}: {r['flagged_intervals']} flagged five-minute readings; maximum observed reading {r['maximum_five_minute_rain']} mm.")
    results=['B0_test_metrics.json','B1_test_metrics.json','test_metrics.json','five_minute_test_metrics.json']
    for name in results:
        p=ART/name
        if not p.exists():para(name+': NOT RUN');continue
        para(name,'Heading2')
        rows=json.loads(p.read_text());data=[['mm','N','Positive','Brier','PR-AUC','ROC-AUC','Log loss']]
        for r in rows:
            data.append([str(r['threshold_mm']),str(r['n'])]+[('N/A' if r.get(k) is None else f'{r[k]:.4f}') for k in ['positive_rate','brier','pr_auc','roc_auc','log_loss']])
        t=Table(data,repeatRows=1);t.setStyle([('BACKGROUND',(0,0),(-1,0),colors.lightgrey),('GRID',(0,0),(-1,-1),.3,colors.grey),('FONTSIZE',(0,0),(-1,-1),8)]);story.extend([t,Spacer(1,12)])
        para('Macro Brier score: '+f"{sum(r['brier'] for r in rows)/len(rows):.6f}")
    p=ART/'model_comparison.json'
    if p.exists():
        para('Validation model comparison','Heading1')
        for r in json.loads(p.read_text()):para(r['kind']+' '+str(r['params'])+' / validation macro Brier '+f"{r['macro_brier']:.6f}")
    p=ART/'ablations.json'
    if p.exists():
        para('Feature ablation','Heading1')
        for r in json.loads(p.read_text()):para('F'+str(r['level'])+' / validation macro Brier '+f"{sum(x['brier'] for x in r['validation'])/4:.6f}"+' / test macro Brier '+f"{sum(x['brier'] for x in r['test'])/4:.6f}")
    for name in ['test_monotonicity.json','five_minute_test_monotonicity.json','reload_check.json']:
        p=ART/name
        if p.exists():para(name+': '+p.read_text())
    p=ART/'test_classification.json'
    if p.exists():
        para('Classification operating points','Heading2')
        for r in json.loads(p.read_text()):para(f"Rainfall threshold {r['threshold_mm']} mm / validation-selected probability cutoff {r['decision_probability']:.2f} / test precision {r['precision']:.4f}, recall {r['recall']:.4f}, F1 {r['f1']:.4f}.")
    para('Figures','Heading1')
    for p in plot_results():
        para(p.stem,'Heading2');story.append(Image(str(p),width=450,height=360 if p.stem.startswith('coverage') else 240,kind='proportional'));story.append(Spacer(1,12))
    para('Limitations and AI disclosure','Heading1')
    para('Eight years support descriptive comparisons, not proof of a long-term climate trend. Completeness filtering can bias samples. The model is evaluated on existing stations and selected rainfall thresholds; unseen stations and other thresholds are not independently validated. Threshold monotonicity is measured, not enforced. No radar, wind or neighbouring-station observations are included.')
    para('AI usage: OpenAI Codex assisted with experimental design, source code and report generation. Group members must verify the implementation and interpret actual results. Missing experiments are explicitly identified above; no results are fabricated.')
    para('Sources: https://data.gov.sg/collections/2279/view ; https://spark.apache.org/docs/3.5.3/ml-classification-regression.html')
    def footer(canvas,doc):
        canvas.setFont('Helvetica',8);canvas.drawRightString(550,25,str(doc.page))
    target=ART/'rainfall_report.pdf'
    SimpleDocTemplate(str(target),title='Singapore Rainfall Experiments',author='DSA5208 project group').build(story,onFirstPage=footer,onLaterPages=footer)
    print(target)
