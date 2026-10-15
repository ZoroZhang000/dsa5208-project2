from datetime import datetime, timedelta
import pytest
from rainfall import spark_session, features, expand, split, clean

@pytest.fixture(scope='module')
def spark():
    s=spark_session()
    s.sparkContext.setLogLevel('ERROR')
    yield s
    s.stop()

def observations(spark, missing=None, changed=False):
    from pyspark.sql import functions as F
    d=spark.range(25)
    if missing is not None:d=d.filter(F.col('id')!=missing)
    return d.selectExpr("'S1' station_id", "timestamp_seconds(unix_timestamp('2021-12-31 23:00:00') + id*300) timestamp",
        ('cast(if(id>12,10,1) as double) rain' if changed else 'cast(1 as double) rain'),
        'cast(1.3 as double) latitude','cast(103.8 as double) longitude', 'if(id<12,2021,2022) year')

def test_exact_window_and_threshold(spark):
    d=features(observations(spark)).filter("timestamp = '2022-01-01 00:00:00'")
    row=d.first()
    assert row.history_count==12 and row.future_count==6 and row.future_rain==6
    assert row.sum_15==3 and row.sum_60==12
    assert expand(d).filter('threshold_mm = 5').first().label==1
    from pyspark.sql import functions as F
    assert d.withColumn('threshold_mm',F.lit(6.)).select((F.col('future_rain')>F.col('threshold_mm')).alias('y')).first().y is False

def test_missing_future_is_not_labelled(spark):
    row=features(observations(spark,missing=15)).filter("timestamp = '2022-01-01 00:00:00'").first()
    assert row.future_count==5 and row.future_rain is None

def test_no_future_feature_leakage(spark):
    a=features(observations(spark)).filter("timestamp = '2022-01-01 00:00:00'").first()
    b=features(observations(spark,changed=True)).filter("timestamp = '2022-01-01 00:00:00'").first()
    for name in ['rain','sum_15','sum_30','sum_60','max_60','wet_60','trend_15']+[f'lag_{k}' for k in range(1,7)]:
        assert a[name]==b[name]
    assert a.future_rain!=b.future_rain

def test_split_boundary(spark):
    train,validation,_=split(features(observations(spark)))
    assert train.filter("timestamp > '2021-12-31 23:25:00'").count()==0
    assert validation.filter("timestamp < '2022-01-01 00:00:00'").count()==0

def test_clean_conflicts_and_invalid(spark,tmp_path):
    path=tmp_path/'sample.csv'
    path.write_text('timestamp,station_id,reading_value,location_latitude,location_longitude\n'
        '2022-01-01T00:00:00+08:00,S1,1,1.3,103.8\n'
        '2022-01-01T00:00:00+08:00,S1,1,1.3,103.8\n'
        '2022-01-01T00:05:00+08:00,S1,1,1.3,103.8\n'
        '2022-01-01T00:05:00+08:00,S1,2,1.3,103.8\n'
        '2022-01-01T00:10:00+08:00,S1,-1,1.3,103.8\n')
    d,q,c=clean(spark,str(path))
    assert d.count()==1 and c.count()==1
    assert q.first().invalid_records==1

def test_one_second_alignment(spark,tmp_path):
    path=tmp_path/'offset.csv'
    path.write_text('timestamp,station_id,reading_value,location_latitude,location_longitude\n'
        '2017-01-01T08:04:59+08:00,S1,1,1.3,103.8\n'
        '2017-01-01T08:10:02+08:00,S1,1,1.3,103.8\n')
    d,q,_=clean(spark,str(path))
    assert d.count()==1
    assert d.selectExpr("date_format(timestamp,'yyyy-MM-dd HH:mm:ss') t").first().t=='2017-01-01 08:05:00'
    assert q.first().off_grid_records==2

def test_single_class_probability_metrics(spark):
    from rainfall import metrics
    d=spark.range(4).selectExpr('cast(id as double) threshold_mm', 'cast(0 as double) label', 'cast(0.25 as double) p')
    result=metrics(d)
    assert len(result)==4
    assert all(r['roc_auc'] is None and r['pr_auc'] is None for r in result)
    assert all(abs(r['brier']-.0625)<1e-12 for r in result)

def test_shared_preprocessing_model_reload(spark,tmp_path):
    from rainfall import pipeline,probabilities
    from pyspark.ml import Pipeline,PipelineModel
    d=expand(features(observations(spark)).filter('history_count = 12 AND future_count = 6'))
    estimator=pipeline('lr',2,{'regParam':.1})
    processor=Pipeline(stages=estimator.getStages()[:-1]).fit(d)
    classifier=estimator.getStages()[-1].fit(processor.transform(d))
    model=PipelineModel(stages=processor.stages+[classifier])
    path=str(tmp_path/'model');model.write().overwrite().save(path)
    loaded=PipelineModel.load(path)
    keys=['station_id','timestamp','threshold_mm']
    left=probabilities(model,d).select(*keys,'p').withColumnRenamed('p','before')
    right=probabilities(loaded,d).select(*keys,'p').withColumnRenamed('p','after')
    assert left.join(right,keys).filter('abs(before-after) > 1e-12').count()==0
