"""Reproducible Spark rainfall experiment. All timestamps use Asia/Singapore."""
import argparse
import calendar
import itertools
import json
import math
import os
from pathlib import Path
import sys
import time

ROOT = Path(os.getenv('RAINFALL_WORKSPACE', str(Path(__file__).resolve().parent))).resolve()
ART = ROOT / 'artifacts'
THRESHOLDS = [0., 1., 5., 10.]

def save_json(name, value):
    ART.mkdir(exist_ok=True)
    temporary=ART/(name+'.tmp')
    temporary.write_text(json.dumps(value, indent=2, default=str), encoding='utf-8')
    temporary.replace(ART/name)

def spark_session():
    try:
        import jdk4py
        os.environ.setdefault('JAVA_HOME', str(jdk4py.JAVA_HOME))
    except ImportError:
        pass
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    from pyspark.sql import SparkSession
    return (SparkSession.builder.master(os.getenv('SPARK_MASTER', 'local[4]'))
            .appName('SingaporeRainfall').config('spark.sql.session.timeZone', 'Asia/Singapore')
            .config('spark.sql.shuffle.partitions', '16').config('spark.driver.memory', os.getenv('SPARK_DRIVER_MEMORY','4g'))
            .config('spark.sql.adaptive.enabled', 'true').getOrCreate())

def download():
    import requests
    raw = ROOT / 'data' / 'raw'
    raw.mkdir(parents=True, exist_ok=True)
    base = 'https://api-production.data.gov.sg/v2/public/api'
    session = requests.Session()
    meta = session.get(base + '/collections/2279/metadata', timeout=60)
    meta.raise_for_status()
    ids = meta.json()['data']['collectionMetadata']['childDatasets']
    manifest = []
    for ident in ids:
        api = 'https://api-open.data.gov.sg/v1/public/api/datasets/' + ident
        response = session.get(api + '/poll-download', timeout=60)
        response.raise_for_status()
        info = response.json()['data']
        if not info.get('url'):
            session.get(api + '/initiate-download', timeout=60).raise_for_status()
            for _ in range(30):
                time.sleep(2)
                info = session.get(api + '/poll-download', timeout=60).json()['data']
                if info.get('url'): break
        if not info.get('url'): raise RuntimeError('Download not ready: ' + ident)
        target = raw / (ident + '.csv')
        if not target.exists():
            partial = target.with_suffix('.part')
            with session.get(info['url'], stream=True, timeout=120) as r:
                r.raise_for_status()
                with partial.open('wb') as f:
                    for chunk in r.iter_content(1024 * 1024): f.write(chunk)
            partial.replace(target)
        import hashlib
        digest=hashlib.sha256()
        with target.open('rb') as f:
            for chunk in iter(lambda:f.read(1024*1024),b''):digest.update(chunk)
        manifest.append({'dataset_id': ident, 'bytes': target.stat().st_size,'sha256':digest.hexdigest()})
        print(ident, target.stat().st_size, flush=True)
    save_json('download_manifest.json', manifest)

def clean(spark, source):
    from pyspark.sql import functions as F
    from pyspark import StorageLevel
    raw = spark.read.option('header', True).csv(source)
    df = raw.select(F.trim('station_id').alias('station_id'),
        F.to_timestamp('timestamp').alias('timestamp'),
        F.col('reading_value').cast('double').alias('rain'),
        F.col('location_latitude').cast('double').alias('latitude'),
        F.col('location_longitude').cast('double').alias('longitude'))
    # Early records end at xx:04:59 rather than xx:05:00. Snap only
    # timestamps at most one second from the five-minute grid, and audit it.
    epoch = F.unix_timestamp('timestamp')
    nearest = (F.round(epoch/300)*300).cast('long')
    df = df.withColumn('timestamp_offset_seconds', epoch-nearest)
    df = df.withColumn('timestamp', F.when(F.abs(F.col('timestamp_offset_seconds')) <= 1,
        F.to_timestamp(F.from_unixtime(nearest))).otherwise(F.col('timestamp')))
    df = df.withColumn('year', F.year('timestamp'))
    valid = (F.col('station_id').isNotNull() & (F.length('station_id') > 0)
        & F.col('timestamp').isNotNull() & F.col('rain').isNotNull()
        & ~F.isnan('rain') & (F.col('rain') >= 0) & (F.col('rain') < float('inf'))
        & F.col('latitude').between(-90, 90) & F.col('longitude').between(-180, 180)
        & (F.unix_timestamp('timestamp') % 300 == 0))
    quality = df.groupBy('year').agg(F.count('*').alias('raw_records'),
        F.sum(F.when(~F.coalesce(valid, F.lit(False)), 1).otherwise(0)).alias('invalid_records'),
        F.sum((F.col('timestamp_offset_seconds') != 0).cast('int')).alias('off_grid_records'))
    good = df.filter(valid & F.col('year').between(2017, 2024)).drop('timestamp_offset_seconds').dropDuplicates().persist(StorageLevel.DISK_ONLY)
    keys = good.groupBy('station_id', 'timestamp').count()
    conflicts = keys.filter('count > 1')
    cleaned = good.join(conflicts.select('station_id', 'timestamp'),
                        ['station_id', 'timestamp'], 'left_anti')
    cleaned = cleaned.withColumn('extreme_flag', F.col('rain') > 50)
    stats = cleaned.groupBy('year').count().withColumnRenamed('count', 'clean_records')
    unique_stats = good.groupBy('year').count().withColumnRenamed('count','unique_valid_records')
    quality = quality.join(unique_stats,'year','full').join(stats, 'year', 'full')
    return cleaned, quality, conflicts

def features(df, with_target=True):
    from pyspark.sql import Window, functions as F
    d = df.withColumn('epoch', F.unix_timestamp('timestamp'))
    w = Window.partitionBy('station_id').orderBy('epoch')
    for k in range(1, 7):
        d = d.withColumn('lag_' + str(k), F.max('rain').over(w.rangeBetween(-300*k, -300*k)))
    # [t-55m,t] comprises 12 five-minute intervals ending at t.
    for minutes in [15, 30, 60]:
        window = w.rangeBetween(-(minutes-5)*60, 0)
        d = d.withColumn('sum_' + str(minutes), F.sum('rain').over(window))
        if minutes == 60:
            d = d.withColumn('history_count', F.count('rain').over(window))
            d = d.withColumn('max_60', F.max('rain').over(window))
            d = d.withColumn('wet_60', F.sum((F.col('rain') > 0).cast('int')).over(window))
    d = d.withColumn('trend_15', 2*F.col('sum_15')-F.col('sum_30'))
    if with_target:
        future = w.rangeBetween(300, 1800)
        d = d.withColumn('future_count', F.count('rain').over(future))
        d = d.withColumn('future_rain', F.when(F.col('future_count') == 6, F.sum('rain').over(future)))
    hour = F.hour('timestamp') + F.minute('timestamp')/60
    for name, value, period in [('hour', hour, 24), ('day', F.dayofyear('timestamp'), 365.25)]:
        d = d.withColumn(name+'_sin', F.sin(value*2*math.pi/period))
        d = d.withColumn(name+'_cos', F.cos(value*2*math.pi/period))
    return d.withColumn('month', F.month('timestamp'))

def expand(df):
    from pyspark.sql import functions as F
    return (df.withColumn('threshold_mm', F.explode(F.array(*[F.lit(a) for a in THRESHOLDS])))
        .withColumn('label', (F.col('future_rain') > F.col('threshold_mm')).cast('double')))

def split(df):
    from pyspark.sql import functions as F
    end = F.year(F.from_unixtime(F.col('epoch') + 1800))
    return (df.filter((F.col('year') <= 2021) & (end <= 2021)),
            df.filter((F.col('year') == 2022) & (end == 2022)),
            df.filter(F.col('year').between(2023, 2024) & (end <= 2024)))

def feature_names(level):
    result = ['station_vector','latitude','longitude','hour_sin','hour_cos','day_sin','day_cos','threshold_mm']
    if level >= 1: result += ['rain'] + ['lag_'+str(k) for k in range(1,7)]
    if level >= 2: result += ['sum_15','sum_30','sum_60','max_60','wet_60','trend_15']
    return result

def pipeline(kind, level, params):
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
    from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, GBTClassifier
    stages = [StringIndexer(inputCol='station_id', outputCol='station_index', handleInvalid='keep'),
              OneHotEncoder(inputCols=['station_index'], outputCols=['station_vector'], handleInvalid='keep'),
              VectorAssembler(inputCols=feature_names(level), outputCol='assembled'),
              StandardScaler(inputCol='assembled', outputCol='features', withMean=False)]
    classes = {'lr': LogisticRegression, 'rf': RandomForestClassifier, 'gbt': GBTClassifier}
    extra = {'maxIter':100} if kind == 'lr' else {'seed':42}
    stages.append(classes[kind](**extra, **params))
    return Pipeline(stages=stages)

def probabilities(model, df):
    from pyspark.ml.functions import vector_to_array
    return model.transform(df).withColumn('p', vector_to_array('probability')[1])

def metrics(pred):
    from pyspark.sql import functions as F
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    pred=pred.select('threshold_mm','label','p').cache()
    p = F.greatest(F.lit(1e-12), F.least(F.lit(1-1e-12), F.col('p')))
    rows = pred.groupBy('threshold_mm').agg(F.count('*').alias('n'), F.avg('label').alias('positive_rate'),
        F.avg(F.pow(F.col('p')-F.col('label'),2)).alias('brier'),
        F.avg(-F.col('label')*F.log(p)-(1-F.col('label'))*F.log(1-p)).alias('log_loss')).collect()
    output=[]
    for row in rows:
        r=row.asDict(); sub=pred.filter(F.col('threshold_mm') == r['threshold_mm'])
        for key,metric in [('pr_auc','areaUnderPR'),('roc_auc','areaUnderROC')]:
            r[key] = (BinaryClassificationEvaluator(rawPredictionCol='p', metricName=metric).evaluate(sub)
                      if 0 < r['positive_rate'] < 1 else None)
        output.append(r)
    pred.unpersist()
    if len(output) != 4: raise ValueError('All four thresholds must be present')
    return sorted(output,key=lambda r:r['threshold_mm'])

def diagnostics(pred, decisions, prefix):
    from pyspark.sql import functions as F
    from pyspark.sql import Window
    calibration = pred.withColumn('bin', F.least(F.lit(9), F.floor(F.col('p')*10)))
    export(calibration.groupBy('threshold_mm','bin').agg(F.count('*').alias('n'),
        F.avg('p').alias('mean_probability'), F.avg('label').alias('observed_rate')), prefix+'_calibration')
    for group in ['year','month','station_id','currently_wet']:
        d=pred.withColumn('currently_wet',(F.col('rain')>0).cast('int'))
        export(d.groupBy('threshold_mm',group).agg(F.count('*').alias('n'),
            F.avg('label').alias('positive_rate'),F.avg(F.pow(F.col('p')-F.col('label'),2)).alias('brier')),prefix+'_'+group)
    w=Window.partitionBy('station_id','timestamp').orderBy('threshold_mm')
    violations=pred.withColumn('previous_p',F.lag('p').over(w)).groupBy('station_id','timestamp').agg(
        F.max((F.col('p')>F.col('previous_p')+1e-10).cast('int')).alias('violation'))
    save_json(prefix+'_monotonicity.json',violations.agg(F.avg('violation').alias('violation_rate')).first().asDict())
    scores=[]
    for a,c in decisions.items():
        sub=pred.filter(F.col('threshold_mm')==float(a)); scores.append(confusion(sub,c)|{'threshold_mm':a,'decision_probability':c})
    save_json(prefix+'_classification.json',scores)

def confusion(df, cutoff):
    from pyspark.sql import functions as F
    positive=F.col('p')>=cutoff
    r=df.agg(F.sum((positive & (F.col('label')==1)).cast('int')).alias('tp'),
        F.sum((positive & (F.col('label')==0)).cast('int')).alias('fp'),
        F.sum((~positive & (F.col('label')==1)).cast('int')).alias('fn')).first().asDict()
    precision=r['tp']/max(1,r['tp']+r['fp']); recall=r['tp']/max(1,r['tp']+r['fn'])
    return r|{'precision':precision,'recall':recall,'f1':2*precision*recall/max(1e-12,precision+recall)}

def export(df, name):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType,MapType,StructType
    for field in df.schema.fields:
        if isinstance(field.dataType,(ArrayType,MapType,StructType)):
            df=df.withColumn(field.name,F.to_json(F.col(field.name)))
    df.coalesce(1).write.mode('overwrite').option('header',True).csv(str(ART/name))

def explore(df):
    from pyspark.sql import functions as F
    export(df.groupBy('year').agg(F.sum(F.col('extreme_flag').cast('int')).alias('flagged_intervals'),
        F.max('rain').alias('maximum_five_minute_rain')),'extreme_flags_year')
    station_year=df.groupBy('station_id','year').agg(F.count('*').alias('n'),F.sum('rain').alias('observed_total'))
    days=F.when((F.col('year')%4==0),366).otherwise(365)
    station_year=station_year.withColumn('coverage', F.col('n')/(days*288))
    export(station_year,'station_year_coverage')
    export(df.groupBy('station_id').agg(F.countDistinct(F.struct('latitude','longitude')).alias('coordinate_versions')),'coordinate_changes')
    daily=df.withColumn('date',F.to_date('timestamp')).groupBy('station_id','year','date').agg(
        F.count('*').alias('n'),F.sum('rain').alias('daily_rain'))
    daily=daily.withColumn('coverage',F.col('n')/288).filter('coverage >= 0.95')
    daily=daily.withColumn('month',F.month('date'))
    export(daily.groupBy('year','month').agg(F.avg('daily_rain').alias('mean_daily_rain'),F.count('*').alias('valid_station_days')),'monthly_rain')
    export(df.groupBy(F.month('timestamp').alias('month'),F.hour('timestamp').alias('hour')).agg(
        F.avg((F.col('rain')>0).cast('int')).alias('wet_frequency'),F.count('*').alias('n')),'month_hour')
    annual=daily.groupBy('station_id','year').agg(F.sum('daily_rain').alias('valid_day_total'),
        F.count('*').alias('valid_days'),F.avg((F.col('daily_rain')>0).cast('int')).alias('wet_day_fraction'))
    export(annual,'annual_station')
    export(df.groupBy('year').agg(F.avg(F.when(F.col('rain')>0,F.col('rain'))).alias('wet_interval_intensity')),'annual_intensity')
    fixed=annual.groupBy('station_id').count().filter('count = 8').select('station_id')
    export(annual.join(fixed,'station_id'),'fixed_station_annual')
    strict=station_year.filter('coverage >= 0.95').groupBy('station_id').count().filter('count = 8').select('station_id')
    export(annual.join(strict,'station_id'),'strict_fixed_station_annual')
    export(df.groupBy('station_id').agg(F.avg('latitude').alias('latitude'),F.avg('longitude').alias('longitude'),
        F.avg('rain').alias('mean_interval_rain')),'station_map')
    export(daily.groupBy('year').agg(F.percentile_approx('daily_rain',[0.5,0.9,0.95,0.99]).alias('quantiles')),'daily_quantiles')
    export(daily.orderBy(F.desc('daily_rain')).limit(30),'extreme_events')

def train(df, quick=False, stride=30, training_fraction=1.):
    from pyspark.sql import functions as F
    from pyspark.ml import Pipeline, PipelineModel
    eligible=features(df).filter('history_count = 12 AND future_count = 6')
    window_path=ROOT/'data'/'windows'
    eligible.write.mode('overwrite').partitionBy('year').parquet(str(window_path))
    eligible=df.sparkSession.read.parquet(str(window_path))
    export(eligible.groupBy('year').count(),'eligible_windows_year')
    n_clean=df.count();n_eligible=eligible.count()
    save_json('sample_availability.json',{'clean_records':n_clean,'complete_windows':n_eligible,
        'excluded_fraction':1-n_eligible/max(1,n_clean)})
    sampled=eligible.filter(F.minute('timestamp') % 30 == 0)
    train_base,validation_base,test_base=split(sampled)
    train_base=train_base.filter(F.minute('timestamp') % stride == 0)
    if training_fraction < 1:
        # Sample origins, not individual threshold rows or negative labels.
        train_base=train_base.filter(F.pmod(F.xxhash64('station_id','timestamp',F.lit(42)),F.lit(10000))
            < int(training_fraction*10000))
    tr,va,te=[expand(x).cache() for x in (train_base,validation_base,test_base)]
    sizes=[x.count() for x in (tr,va,te)]
    save_json('split_sizes.json', dict(zip(['train','validation','test'],sizes)))
    if min(sizes)==0: raise ValueError('Empty time split; provide observations for training, 2022, and 2023/24')
    grids={'lr':[{'regParam':r} for r in [.01,.1,1.]],
           'rf':[dict(numTrees=n,maxDepth=d) for n,d in itertools.product([50,100],[5,8])],
           'gbt':[dict(maxIter=n,maxDepth=d) for n,d in itertools.product([30,60],[3,5])]}
    if quick: grids={'lr':[{'regParam':.1}],'rf':[{'numTrees':5,'maxDepth':3}],'gbt':[{'maxIter':3,'maxDepth':2}]}
    # Fit identical F2 preprocessing once on training data, then reuse it
    # across the parameter grid. The saved model remains a full PipelineModel.
    processor=Pipeline(stages=pipeline('lr',2,{}).getStages()[:-1]).fit(tr)
    processed=processor.transform(tr).select('features','label').cache()
    processed.count()
    candidates=[]; winner=None; best=float('inf')
    for kind,grid in grids.items():
        for params in grid:
            estimator=pipeline(kind,2,params).getStages()[-1]
            model=PipelineModel(stages=processor.stages+[estimator.fit(processed)])
            result=metrics(probabilities(model,va)); score=sum(r['brier'] for r in result)/4
            candidates.append({'kind':kind,'params':params,'validation':result,'macro_brier':score})
            save_json('model_comparison.json',candidates)
            print(kind,params,score,flush=True)
            if score < best: best=score; winner=(kind,params,model)
    kind,params,model=winner
    processed.unpersist()
    model.write().overwrite().save(str(ART/'best_model'))
    save_json('model_config.json',{'kind':kind,'params':params,'level':2,'thresholds':THRESHOLDS,
        'training_stride_minutes':stride,'evaluation_stride_minutes':30,'quick_debug_run':quick,
        'training_fraction':training_fraction,'origin_sampling':'xxhash64(station_id,timestamp,42), mod 10000',
        'feature_inputs':feature_names(2),'time_zone':'Asia/Singapore',
        'target':'sum at t+5,t+10,t+15,t+20,t+25,t+30 minutes; strictly greater than threshold'})
    columns=['station_id','timestamp','year','month','rain','threshold_mm','label','p']
    vp=probabilities(model,va).select(*columns).cache(); decisions={}
    for a in THRESHOLDS:
        sub=vp.filter(F.col('threshold_mm')==a)
        decisions[a]=max([i/20 for i in range(1,20)],key=lambda c:confusion(sub,c)['f1'])
    save_json('decision_cutoffs.json',decisions)
    tp=probabilities(model,te).select(*columns).cache(); save_json('test_metrics.json',metrics(tp)); diagnostics(tp,decisions,'test')
    ablations=[]
    for level in [0,1,2]:
        m=model if level==2 else pipeline(kind,level,params).fit(tr)
        ablations.append({'level':level,'validation':metrics(probabilities(m,va)),'test':metrics(probabilities(m,te))})
    save_json('ablations.json',ablations)
    overall=tr.groupBy('threshold_mm').agg(F.avg('label').alias('p'))
    seasonal=tr.groupBy('station_id','month','threshold_mm').agg(F.avg('label').alias('seasonal_p'))
    export(overall,'baseline_global_probabilities')
    export(seasonal,'baseline_seasonal_probabilities')
    for name,pred in [('B0',te.join(overall,'threshold_mm')),
        ('B1',te.join(seasonal,['station_id','month','threshold_mm'],'left').join(overall,'threshold_mm')
         .withColumn('p',F.coalesce('seasonal_p','p')))]:
        save_json(name+'_test_metrics.json',metrics(pred))
    _,_,fulltest=split(eligible)
    fullp=probabilities(model,expand(fulltest)).select(*columns).cache()
    save_json('five_minute_test_metrics.json',metrics(fullp)); diagnostics(fullp,decisions,'five_minute_test')
    from pyspark.ml import PipelineModel
    loaded=PipelineModel.load(str(ART/'best_model'))
    probe=te.orderBy('station_id','timestamp','threshold_mm').limit(20)
    left=probabilities(model,probe).select('p').collect(); right=probabilities(loaded,probe).select('p').collect()
    if any(abs(a.p-b.p)>1e-12 for a,b in zip(left,right)): raise AssertionError('Reload mismatch')
    save_json('reload_check.json',{'passed':True,'samples':len(left)})

class RainfallPredictor:
    def __init__(self, spark, observations=None):
        from pyspark.ml import PipelineModel
        self.spark=spark
        self.observations=observations if observations is not None else spark.read.parquet(str(ROOT/'data'/'clean'))
        self.model=PipelineModel.load(str(ART/'best_model'))

    def predict(self, station_id, prediction_time, threshold_mm):
        from pyspark.sql import functions as F
        if not math.isfinite(threshold_mm) or threshold_mm < 0: raise ValueError('Threshold must be finite and nonnegative')
        target=F.to_timestamp(F.lit(prediction_time))
        observations=self.observations
        if 'year' in observations.columns:
            observations=observations.filter(F.col('year').between(F.year(target-F.expr('INTERVAL 60 MINUTES')),F.year(target)))
        history=observations.filter((F.col('station_id')==station_id)&(F.col('timestamp')<=target)
            &(F.col('timestamp')>=target-F.expr('INTERVAL 60 MINUTES')))
        row=features(history,False).filter((F.col('timestamp')==target)&(F.col('history_count')==12))
        if not row.take(1): return {'status':'insufficient_history','probability':None}
        p=probabilities(self.model,row.withColumn('threshold_mm',F.lit(float(threshold_mm)))).select('p').first().p
        return {'status':'available','probability':p,'threshold_mm':threshold_mm,
                'evaluated_threshold':threshold_mm in THRESHOLDS,
                'known_training_station':station_id in self.model.stages[0].labels,
                'time_semantics':'next six recorded five-minute observations'}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=['download','prepare','train','predict','report'])
    parser.add_argument('--source',default=str(ROOT/'data'/'raw'/'*.csv'))
    parser.add_argument('--quick',action='store_true'); parser.add_argument('--stride',type=int,default=30,choices=[30,60])
    parser.add_argument('--training-fraction',type=float,default=1.)
    parser.add_argument('--station'); parser.add_argument('--time'); parser.add_argument('--threshold',type=float,default=1.)
    args=parser.parse_args(); ART.mkdir(exist_ok=True)
    if not .0001 <= args.training_fraction <= 1:parser.error('--training-fraction must be in [0.0001,1]')
    if args.command=='download': download(); return
    if args.command=='report':
        from latex_report import build_latex_report
        build_latex_report(); return
    spark=spark_session(); start=time.time()
    import platform, psutil
    environment={'python':sys.version,'spark':spark.version,'platform':platform.platform(),
        'logical_cpus':os.cpu_count(),'ram_bytes':psutil.virtual_memory().total,'master':spark.sparkContext.master,
        'driver_memory':spark.conf.get('spark.driver.memory'),
        'shuffle_partitions':spark.conf.get('spark.sql.shuffle.partitions'),'java':spark._jvm.java.lang.System.getProperty('java.version')}
    save_json(args.command+'_environment.json',environment)
    if args.command in ['prepare','train']:save_json('environment.json',environment)
    try:
        if args.command=='prepare':
            df,q,c=clean(spark,args.source); df=df.cache()
            export(q,'quality_year'); export(c.groupBy(F_year()).count(),'conflicts_year')
            df.write.mode('overwrite').partitionBy('year').parquet(str(ROOT/'data'/'clean'))
            explore(df)
        elif args.command=='train': train(spark.read.parquet(str(ROOT/'data'/'clean')),args.quick,args.stride,args.training_fraction)
        else: print(json.dumps(RainfallPredictor(spark).predict(args.station,args.time,args.threshold)))
    finally:
        save_json(args.command+'_timing.json',{'seconds':time.time()-start}); spark.stop()

def F_year():
    from pyspark.sql import functions as F
    return F.year('timestamp').alias('year')

if __name__=='__main__': main()
