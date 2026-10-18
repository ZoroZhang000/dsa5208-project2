"""Exact ROC-AUC and PR-AUC for a saved model on the main test split.

The training pipeline (rainfall.metrics) uses Spark's BinaryClassificationEvaluator
with its default numBins=1000, which downsamples the ROC and PR curves before
integrating them. This script re-scores an existing saved model with numBins=0,
which disables that downsampling. It does not retrain anything and does not
change rainfall.py or any pipeline output.

The main test split is rebuilt from the prepared complete-window table with the
pipeline's own functions, exactly as rainfall.train does: origins at 30-minute
intervals, years 2023-2024 with target windows that stay inside the split,
expanded to the four thresholds. Per-threshold counts and Brier scores are
recorded too, so the output can be checked against the original test_metrics.json
to confirm that the same examples were scored.

Usage, from the code directory, after `python rainfall.py train` has written
data/windows in the workspace:

    python exact_auc.py --model artifacts/best_model --windows data/windows \
        --output artifacts/exact_auc_test_metrics.json \
        --compare artifacts/test_metrics.json
"""
import argparse
import hashlib
import json
import platform
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

EVALUATOR = {'class': 'pyspark.ml.evaluation.BinaryClassificationEvaluator',
             'rawPredictionCol': 'p (probability of exceedance)', 'labelCol': 'label',
             'numBins': 0, 'metrics': {'roc_auc': 'areaUnderROC', 'pr_auc': 'areaUnderPR'}}


def tree_digest(path, skip_crc=True):
    """SHA-256 over relative paths and contents of every file under path, in sorted order."""
    digest, count, size = hashlib.sha256(), 0, 0
    for p in sorted(q for q in path.rglob('*') if q.is_file()):
        if skip_crc and p.name.endswith('.crc'):
            continue
        data = p.read_bytes()
        digest.update(str(p.relative_to(path)).encode()); digest.update(data)
        count += 1; size += len(data)
    return digest.hexdigest(), count, size


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--model', required=True, type=Path, help='saved PipelineModel directory to evaluate')
    parser.add_argument('--windows', required=True, type=Path,
                        help='complete-window Parquet table written by `rainfall.py train` (data/windows)')
    parser.add_argument('--output', required=True, type=Path, help='JSON file to write')
    parser.add_argument('--compare', type=Path, help='original test_metrics.json, to confirm the same examples')
    parser.add_argument('--data-note', default='', help='free-text provenance of the windows table, stored in the output')
    args = parser.parse_args(argv)

    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import rainfall
    from pyspark.ml import PipelineModel
    from pyspark.ml.evaluation import BinaryClassificationEvaluator
    from pyspark.sql import functions as F

    started = time.time()
    spark = rainfall.spark_session()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        model = PipelineModel.load(str(args.model))
        windows = spark.read.parquet(str(args.windows))
        _, _, test = rainfall.split(windows.filter(F.minute('timestamp') % 30 == 0))
        pred = (rainfall.probabilities(model, rainfall.expand(test))
                .select('threshold_mm', 'label', 'p').cache())
        rows = []
        for threshold in rainfall.THRESHOLDS:
            sub = pred.filter(F.col('threshold_mm') == threshold)
            s = sub.agg(F.count('*').alias('n'), F.avg('label').alias('positive_rate'),
                        F.avg(F.pow(F.col('p') - F.col('label'), 2)).alias('brier')).first()
            row = {'threshold_mm': threshold, 'n': s.n, 'positive_rate': s.positive_rate, 'brier': s.brier}
            for name, metric in EVALUATOR['metrics'].items():
                row[name] = BinaryClassificationEvaluator(rawPredictionCol='p', labelCol='label',
                                                          metricName=metric, numBins=0).evaluate(sub)
            rows.append(row)
            print(json.dumps(row), flush=True)
        pred.unpersist()
        spark_version = spark.version
    finally:
        spark.stop()

    model_sha, model_files, _ = tree_digest(args.model)
    data_sha, data_files, data_bytes = tree_digest(args.windows)
    config = args.model.parent / 'model_config.json'
    result = {
        'description': 'Exact ROC-AUC and PR-AUC (numBins=0) of a saved model on the main test split. '
                       'Supplements the original pipeline outputs; no model was retrained.',
        'model': {'path': str(args.model), 'sha256_of_files': model_sha, 'files_hashed': model_files,
                  'config': json.loads(config.read_text()) if config.exists() else None},
        'data': {'path': str(args.windows), 'table': 'complete windows (12 past and 6 future five-minute readings)',
                 'sha256_of_files': data_sha, 'files_hashed': data_files, 'bytes': data_bytes,
                 'note': args.data_note},
        'split': {'name': 'main test', 'years': [2023, 2024], 'origin_interval_minutes': 30,
                  'boundary_rule': 'target window must end by 2024', 'thresholds_mm': rainfall.THRESHOLDS},
        'evaluator': EVALUATOR,
        'environment': {'python': platform.python_version(), 'spark': spark_version, 'platform': platform.platform(),
                        'generated_utc': datetime.now(timezone.utc).isoformat(timespec='seconds'),
                        'seconds': round(time.time() - started, 1)},
        'per_threshold': rows,
    }
    if args.compare:
        ref = {r['threshold_mm']: r for r in json.loads(args.compare.read_text())}
        result['comparison'] = {
            'file': str(args.compare),
            'same_counts': all(ref[r['threshold_mm']]['n'] == r['n'] for r in rows),
            'max_brier_difference': max(abs(ref[r['threshold_mm']]['brier'] - r['brier']) for r in rows)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2))
    print('wrote', args.output)


if __name__ == '__main__':
    main()
