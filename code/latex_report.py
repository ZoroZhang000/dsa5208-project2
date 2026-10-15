"""Generate editable LaTeX from actual aggregated experimental artifacts."""
import json
from pathlib import Path
from rainfall import ART
from report import table_rows,plot_results

def esc(value):
    mapping={'\\':r'\textbackslash{}','&':r'\&','%':r'\%','$':r'\$','#':r'\#','_':r'\_','{':r'\{','}':r'\}','~':r'\textasciitilde{}','^':r'\textasciicircum{}'}
    return ''.join(mapping.get(c,c) for c in str(value))

def build_latex_report():
    lines=[r'\documentclass[11pt,a4paper]{article}',r'\usepackage[margin=2.3cm]{geometry}',
        r'\usepackage{amsmath,booktabs,graphicx,float}',r'\usepackage[hidelinks]{hyperref}',
        r'\graphicspath{{figures/}}',r'\title{Singapore Rainfall: Patterns and 30-Minute Exceedance Forecasting}',
        r'\author{Group members and student IDs: \textit{to be completed}}',r'\date{}',r'\begin{document}',r'\maketitle']
    def paragraph(text):lines.extend([esc(text),''])
    def section(text):lines.append(r'\section{'+esc(text)+'}')
    def subsection(text):lines.append(r'\subsection{'+esc(text)+'}')
    def load(name):
        path=ART/name
        return json.loads(path.read_text(encoding='utf-8')) if path.exists() else None
    def table(headers,rows,caption):
        lines.extend([r'\begin{table}[H]\centering\small',r'\caption{'+esc(caption)+'}',
            r'\resizebox{\textwidth}{!}{\begin{tabular}{'+'l'*len(headers)+'}',r'\toprule',
            ' & '.join(esc(x) for x in headers)+r' \\',r'\midrule'])
        lines.extend(' & '.join(esc(x) for x in row)+r' \\' for row in rows)
        lines.extend([r'\bottomrule\end{tabular}}',r'\end{table}'])
    def number(x):return 'N/A' if x is None else f'{x:.5f}'

    if ART.parent.name=='smoke':paragraph('SMOKE TEST ONLY: one station in January; not final full-data results.')
    section('Data, cleaning and computing environment')
    paragraph('We use NEA rainfall observations from 2017 through 2024. The original fields include observation timestamp, station ID, station metadata, coordinates and five-minute rainfall totals in millimetres. The official collection states that timestamps use Singapore Time and that records may be missing and have not undergone climate-data quality control.')
    lines.append(r'Data source: \url{https://data.gov.sg/collections/2279/view}.')
    paragraph('All processing uses Spark DataFrames. Exact semantic duplicates are removed. Conflicting observations at the same station and timestamp are excluded. Negative, nonfinite, unparseable, geographically invalid and off-grid observations are excluded. Early timestamps within one second of the five-minute grid are aligned and audited. Missing observations are never treated as zero rainfall. Five-minute values above 50 mm are retained and flagged as an engineering audit rule, not an official weather limit.')
    rows=[r for r in table_rows('quality_year') if r['year'] and 2017<=int(r['year'])<=2024]
    if rows:table(['Year','Raw','Invalid','Off-grid','Unique valid','Clean'],
        [[r[k] for k in ['year','raw_records','invalid_records','off_grid_records','unique_valid_records','clean_records']] for r in sorted(rows,key=lambda r:r['year'])],'Record-level quality audit; off-grid counts include accepted one-second alignment.')
    changes=table_rows('coordinate_changes')
    if changes:paragraph(f"{sum(int(r['coordinate_versions'])>1 for r in changes)} stations have more than one distinct coordinate pair. These changes can represent metadata updates or relocation; fixed IDs do not guarantee fixed physical locations.")
    environment=load('environment.json')
    if environment:table(['Setting','Value'],[[k,v] for k,v in environment.items() if k not in ['python','platform']],'Local Spark runtime configuration')
    if environment:paragraph('Python: '+environment['python']+'. Platform: '+environment['platform']+'.')
    timing=[]
    for name in ['prepare','train']:
        value=load(name+'_timing.json')
        if value:timing.append([name,f"{value['seconds']:.1f}"])
    if timing:table(['Stage','Seconds'],timing,'Measured stage elapsed times; preparation includes cleaning and exploratory aggregation.')

    section('Exploratory analysis')
    paragraph('A station-day is used for daily statistics only when at least 95% of its 288 expected intervals are observed. Monthly statistics report numbers of qualifying station-days. Annual sums cover qualifying days and are not complete climatological annual totals. Mean rainfall per valid day, wet-day frequency and positive-interval intensity are analysed separately to distinguish exposure, frequency and intensity.')
    paragraph('Wet intervals and wet days here mean positive observed rainfall. This operational definition differs from the MSS climate rainday criterion of at least 0.2 mm; their counts are not directly comparable.')
    panel=table_rows('fixed_station_annual')
    if panel:paragraph(f"A common-ID panel contains {len({r['station_id'] for r in panel})} stations with qualifying days in every year. This controls changing station IDs, but annual valid-day counts and potential relocation must still be considered.")
    if (ART/'strict_fixed_station_annual').exists() and not table_rows('strict_fixed_station_annual'):
        paragraph('No station reaches 95% full-year coverage in all eight years. The additional strict-coverage panel is therefore empty. The common-ID comparison does not eliminate missing-day bias.')
    monthly=table_rows('monthly_rain')
    if monthly:
        means={}
        for m in range(1,13):
            rows=[r for r in monthly if int(r['month'])==m]
            if rows:means[m]=sum(float(r['mean_daily_rain'])*int(r['valid_station_days']) for r in rows)/sum(int(r['valid_station_days']) for r in rows)
        wet=max(means,key=means.get);dry=min(means,key=means.get)
        paragraph(f'Across qualifying station-days, month {wet} has the highest mean daily rainfall ({means[wet]:.2f} mm) and month {dry} the lowest ({means[dry]:.2f} mm). These means are weighted by qualifying station-day counts.')
    hourly=table_rows('month_hour')
    if hourly:
        peak=max(hourly,key=lambda r:float(r['wet_frequency']))
        paragraph(f"The maximum month/hour wet-interval fraction is {float(peak['wet_frequency']):.3f}, occurring in month {peak['month']} at hour {peak['hour']} SGT.")
    paragraph('MSS long-term climatology describes wetter conditions in November-January and more frequent afternoon rainfall, with February as the driest month. Differences from this study can reflect different periods, station mixtures and completeness filtering; their contributions cannot be identified from these data alone.')
    lines.append(r'Climate context: \url{https://www.weather.gov.sg/climate-climate-of-singapore/}.')
    extreme=table_rows('extreme_events')
    if extreme:
        table(['Station','Date','Observed daily mm','Coverage'],[[r['station_id'],r['date'],r['daily_rain'],f"{float(r['coverage']):.3f}"] for r in extreme[:10]],'Largest qualifying station-day totals, requiring further quality checks')
        paragraph('These are observed, unvalidated extremes. In particular, very large daily totals require cross-station and provider checks before being treated as physical weather records. No positive outlier was silently removed.')
    flagged=table_rows('extreme_flags_year')
    if flagged:table(['Year','Flagged intervals','Maximum five-minute mm'],[[r['year'],r['flagged_intervals'],r['maximum_five_minute_rain']] for r in sorted(flagged,key=lambda r:r['year'])],'Positive-value audit; absence of a flag does not establish climate-data validity')

    section('Prediction protocol')
    paragraph('The external interface accepts station ID, prediction time and rainfall threshold. The program retrieves observations through the prediction time internally. Predictions are retrospective: the latest reading is assumed already available. Historical update timestamps can lag observation times, so this experiment does not establish real-time publication latency.')
    lines.extend([r'\[R_{30}(s,t)=\sum_{k=1}^{6}r(s,t+5k\text{ minutes}),\qquad y(s,t,a)=\mathbf{1}[R_{30}(s,t)>a].\]'])
    paragraph('The original reading type confirms a five-minute total, but the official collection does not establish whether its timestamp is the start or end of the accumulation interval. The target therefore represents the next six recorded observations; physical interval-end semantics remain an assumption.')
    paragraph('Training uses 2017-2021, validation uses 2022 and testing uses 2023-2024. Targets crossing split boundaries are excluded. Complete examples require 12 past readings at t-55 through t and six future readings at t+5 through t+30. No future observations enter features. Encoders, standardisation and baseline probabilities are fitted using training data only.')
    config=load('model_config.json')
    if config:
        paragraph(f"Training uses prediction origins every {config['training_stride_minutes']} minutes and a uniform origin fraction of {config.get('training_fraction',1):.3f}. The stable station/time hash uses seed 42. All four threshold examples from a retained origin remain together. Validation and main testing retain all complete 30-minute origins; an additional evaluation retains all complete five-minute test origins. No class-dependent sampling or reweighting is used.")
    availability=load('sample_availability.json')
    if availability:paragraph(f"There are {availability['clean_records']:,} cleaned readings and {availability['complete_windows']:,} complete windows. The fraction excluded by window completeness is {100*(1-availability['complete_windows']/availability['clean_records']):.2f} percent.")
    sizes=load('split_sizes.json')
    if sizes:table(['Split','Threshold-expanded examples'],[[k,f'{v:,}'] for k,v in sizes.items()],'Main experiment sample counts after completeness, temporal boundaries and training sampling')
    table(['Features','Contents'],[['F0','Station encoding, coordinates, cyclical hour/day-of-year, threshold'],['F1','F0 plus current five-minute rainfall and six five-minute lags'],['F2','F1 plus 15/30/60-minute sums, 60-minute maximum, wet count and 15-minute trend']],'Feature groups; all history ends at prediction time')
    paragraph('Thresholds are 0, 1, 5 and 10 mm, using strict exceedance. A single joint binary classifier includes threshold as a numeric feature. Other nonnegative thresholds can be queried but are not independently validated. Thresholds are experimental settings, not official warning levels.')
    paragraph('Models are MLlib logistic regression, random forest and gradient-boosted trees. LR regularisation is 0.01/0.1/1; RF uses 50/100 trees at depth 5/8; GBT uses 30/60 iterations at depth 3/5. Identical fitted F2 preprocessing is shared across candidates. Hyperparameters are selected by validation macro Brier score, averaging the four thresholds equally. The winner family and parameters are frozen for the F0/F1/F2 ablation. We do not use random cross-validation or test results for tuning.')
    paragraph('B0 predicts training exceedance frequency by threshold. B1 uses training station/month/threshold frequency, falling back to B0 for absent cells. Probability evaluation reports Brier score, log loss, Spark PR-AUC and ROC-AUC. AUC is unavailable in single-class subsets. Reliability plots use ten equal-width probability bins. Per-threshold decision cutoffs maximise validation F1 over 0.05 to 0.95, then remain fixed for test classification metrics.')
    lines.append(r'MLlib source: \url{https://spark.apache.org/docs/3.5.3/ml-classification-regression.html}.')

    section('Predictive results')
    comparison=load('model_comparison.json')
    if comparison:table(['Model','Parameters','Validation macro Brier'],[[r['kind'],str(r['params']),number(r['macro_brier'])] for r in comparison],'Chronological validation model selection; lower Brier is better')
    if config:paragraph('Selected model: '+config['kind']+', parameters '+str(config['params'])+'.')
    for name,title in [('B0_test_metrics.json','B0 baseline'),('B1_test_metrics.json','B1 seasonal baseline'),('test_metrics.json','Selected model, 30-minute origins'),('five_minute_test_metrics.json','Selected model, five-minute origins')]:
        rows=load(name);subsection(title)
        if not rows:paragraph('NOT RUN.');continue
        table(['mm','N','Positive fraction','Brier','PR-AUC','ROC-AUC','Log loss'],
            [[r['threshold_mm'],f"{r['n']:,}"]+[number(r.get(k)) for k in ['positive_rate','brier','pr_auc','roc_auc','log_loss']] for r in rows],title)
        paragraph('Macro Brier score: '+number(sum(r['brier'] for r in rows)/4)+'.')
    baseline=load('B0_test_metrics.json');test=load('test_metrics.json')
    if baseline and test:
        b=sum(r['brier'] for r in baseline)/4;m=sum(r['brier'] for r in test)/4
        paragraph(f'The selected model changes macro Brier by {100*(m/b-1):.2f} percent relative to B0 on main test origins (negative denotes improvement). Per-threshold results must also be considered, particularly when exceedance is rare.')
    ablations=load('ablations.json')
    if ablations:table(['Feature group','Validation macro Brier','Test macro Brier'],[['F'+str(r['level']),number(sum(x['brier'] for x in r['validation'])/4),number(sum(x['brier'] for x in r['test'])/4)] for r in ablations],'Feature ablation with frozen winner family and hyperparameters')
    if ablations:
        scores={r['level']:sum(x['brier'] for x in r['test'])/4 for r in ablations}
        if all(k in scores for k in [0,1,2]):
            paragraph(f"Adding current rainfall and recent lags changes held-out macro Brier from {scores[0]:.5f} (F0) to {scores[1]:.5f} (F1). Adding rolling summaries changes it to {scores[2]:.5f} (F2). These frozen-parameter comparisons quantify the incremental predictive value of recent observations and their summaries.")
    classification=load('test_classification.json')
    if classification:table(['Rain mm','Probability cutoff','Precision','Recall','F1'],[[r['threshold_mm'],r['decision_probability'],number(r['precision']),number(r['recall']),number(r['f1'])] for r in classification],'Main test classification at validation-selected operating points')
    for name,title in [('test_monotonicity.json','30-minute origins'),('five_minute_test_monotonicity.json','Five-minute origins')]:
        r=load(name)
        if r:paragraph(title+': fraction of station/time examples with increasing probability as the rainfall threshold rises = '+number(r['violation_rate'])+'. Monotonicity is measured, not enforced.')
    for name,label in [('test_year','Year'),('test_currently_wet','Currently wet (0/1)')]:
        rows=table_rows(name);key='year' if name=='test_year' else 'currently_wet'
        if rows:table([label,'Rain mm','N','Positive fraction','Brier'],[[r[key],r['threshold_mm'],r['n'],number(float(r['positive_rate'])),number(float(r['brier']))] for r in sorted(rows,key=lambda r:(r[key],float(r['threshold_mm'])))],'Test subgroup probability performance')
    paragraph('Additional month and station subgroup tables are exported as CSV. Differences in Brier across subgroups can reflect both forecast errors and different event prevalence; they should not be read as a pure ranking of station difficulty. Adjacent time windows and stations are dependent, so no independent-sample confidence intervals are claimed.')
    check=load('reload_check.json')
    if check:paragraph(f"Model reload validation: {check['samples']} probe examples have matching saved/reloaded probabilities within 1e-12 (passed: {check['passed']}). Automated tests separately check exact windows, strict exceedance, missing future records, future-feature leakage, temporal boundaries, timestamp alignment and single-class metrics.")

    section('Figures')
    captions={'coverage_part_1':'Full-year observation coverage, station panel 1; white cells have no observations.',
        'coverage_part_2':'Full-year observation coverage, station panel 2; white cells have no observations.',
        'month_hour':'Wet-interval frequency by month and local hour.', 'monthly_rain':'Mean rainfall per qualifying station-day by year and month.',
        'annual_station':'Mean observed station cumulative rainfall over qualifying days; not complete annual climate totals.',
        'fixed_station_annual':'Observed cumulative rainfall for the fixed-ID panel; remaining valid-day differences still matter.',
        'annual_daily_mean':'Rainfall per valid station-day for all available stations and common station IDs.',
        'annual_wet_days':'Mean station wet-day fraction among qualifying days.', 'annual_intensity':'Mean positive five-minute rainfall intensity.',
        'daily_quantiles':'Quantiles of qualifying station-day rainfall.', 'stations':'Station coordinates coloured by mean observed five-minute rainfall; metadata changes may affect positions.',
        'test_calibration':'Reliability at 30-minute test origins; diagonal denotes perfect calibration.',
        'five_minute_test_calibration':'Reliability at five-minute test origins.'}
    for path in plot_results():
        lines.extend([r'\begin{figure}[htbp]\centering',r'\includegraphics[width=0.95\textwidth,height=0.65\textheight,keepaspectratio]{'+path.name+'}',
            r'\caption{'+esc(captions.get(path.stem,path.stem))+'}',r'\end{figure}'])
    lines.append(r'\clearpage')
    section('Limitations, further work and AI disclosure')
    paragraph('Eight years support descriptive comparisons, not proof of a long-term climate trend. Missingness and completeness filtering can bias both analysis and evaluation. Positive outliers have not received external quality control. Observed annual sums are incomplete when days fail coverage checks. A common station ID may have changing coordinates. Forecasts assume historical observations through t are available, and real-time data latency is not evaluated.')
    paragraph('The models are assessed on the available network, not an independent unseen-station experiment. Outputs at other thresholds can be queried but are not separately validated; joint classifiers need not satisfy threshold monotonicity. Uniform training-origin sampling controls local computation but may lose rare training events. No radar, wind or neighbouring-station features are used. Further work could verify extreme events, test stable-location panels, improve calibration and enforce a coherent exceedance curve.')
    paragraph('AI usage: OpenAI Codex assisted with experiment design, code implementation and LaTeX report generation. Group members should verify the implementation and actual results, complete member details and refine the interpretation before submission. Missing experiments are explicitly marked; no synthetic results are substituted for full-data experiments.')
    lines.append(r'\end{document}')
    target=ART/'rainfall_report.tex';target.write_text('\n'.join(lines)+'\n',encoding='utf-8')
    print(target)
    return target

if __name__=='__main__':build_latex_report()
