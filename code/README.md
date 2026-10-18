# Singapore rainfall experiments

Run from this directory in VS Code. Python 3.12, Spark 3.5.3, Java 11.

On this machine use **VS Code Remote - WSL (Ubuntu-24.04)** for the full pipeline. Native Windows Spark arithmetic works, but Hadoop file access fails without native Windows Hadoop libraries. The WSL environment is `.venv-wsl`; equivalent commands are `.venv-wsl/bin/python rainfall.py prepare`, etc. Java is supplied by `jdk4py` inside the environment; no system Java installation is required.

```bash
cd dsa5208-project2/code
python3 -m venv .venv-wsl
.venv-wsl/bin/python -m pip install -r requirements.txt
.venv-wsl/bin/python -m pytest tests -q
.venv-wsl/bin/python rainfall.py prepare
.venv-wsl/bin/python rainfall.py train --training-fraction 0.1
.venv-wsl/bin/python rainfall.py report
```

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.txt
.venv/Scripts/python.exe rainfall.py download
```

`download` resumes completed files and downloads the collection (including its small December 2016 file); `prepare` retains only 2017–2024. Interrupted files have `.part` extensions and are downloaded again. No missing observations are imputed as zero.

For real-data smoke validation run `make_smoke_sample.py`, then set `RAINFALL_WORKSPACE` to the absolute `smoke` directory before running prepare/train. This isolates smoke data/models/results from the full experiment. The sample comprises S77 in January of each year. `train --quick` is only a debugging run with reduced model parameters, not the agreed final experiment. `--stride 60` uniformly reduces only training prediction origins; validation/main-test remain at 30 minutes and the additional five-minute test remains intact. No negative-class downsampling is applied.

```bash
.venv-wsl/bin/python rainfall.py predict --station S50 --time '2024-06-01T14:00:00+08:00' --threshold 5
```

Python interface: `RainfallPredictor(spark).predict(station_id, prediction_time, threshold_mm)`. It reads historical observations internally and returns `available` or `insufficient_history`. Queries should use an exact five-minute timestamp; timestamps without offsets are interpreted in SGT. Nonnegative finite thresholds are accepted, with performance validated only at 0, 1, 5 and 10 mm. Unseen stations use the unknown encoding; this is not a guarantee of generalization.

Outputs: `data/clean` and `data/windows` (Parquet), `artifacts/best_model` (MLlib PipelineModel), comparison and evaluation JSON, aggregated CSV directories, `artifacts/figures/`, and **`artifacts/rainfall_report.tex`**. Each Spark CSV directory contains a `part-*.csv` file. The LaTeX author field is left for group names and student IDs. No PDF is included in the submission bundle, following the requested LaTeX delivery.

Open the `.tex` in VS Code with LaTeX Workshop and an installed TeX distribution. Keep its `figures/` folder beside it. To compile manually, run `pdflatex rainfall_report.tex` from `artifacts/`. The report generator rebuilds the source from actual outputs; complete your personal edits after the final generation. No TeX compiler is supplied by the Python virtual environment.

`run_experiments.py` runs prepare, train and report sequentially with stage logs and `run_status.json`; `--from-stage train` resumes after successful preparation. After final results and report review, run `package_submission.py` to create `../rainfall_submission.zip`, containing code, model and results but excluding downloaded data and virtual environments. The packaging command refuses to package missing final experiment artifacts.

For the approximately 48.5 million cleaned observations on this machine, `train --training-fraction 0.1` is a resource-controlled option: retain 10% of training origins using a stable hash of station/time/seed 42, then expand all four thresholds. All years/stations/hours retain the same selection probability; there is no label-dependent sampling. Validation and both test resolutions remain complete. The chosen fraction is saved in model configuration and must be disclosed in the report. Default `1` retains all training origins. Cached fitted F2 preprocessing is shared across model candidates, and metric computation caches only labels/probabilities to avoid repeated model inference.

Important definitions: the past 60 minutes include 12 readings at t-55,...,t; lags include t-5,...,t-30; target includes t+5,...,t+30. The latest reading at t must be available to the caller. Physical interval semantics require confirmation from the data provider. Early timestamps ending in `:59` are snapped to the nearest five-minute boundary only when within one second, with counts audited. Other off-grid observations are excluded. Daily coverage denominator is 288; annual coverage uses the full calendar year, including periods before/after a station's operation. Conflicting rain/coordinate observations are excluded. Annual totals sum only qualifying days and are labeled as observed sums. Historical publication/update timestamps can lag observations; retrospective experiments assume the reading at t is already available and do not establish real-time publication latency.

The validation Brier score selects the model; parameters are frozen before test evaluation. Ablations reuse the winning model family and hyperparameters. Classification cutoffs maximize validation F1 on 0.05,...,0.95; ties select the smaller cutoff. ROC/PR AUC are undefined for single-class subsets. Probabilities are evaluated without class reweighting or calibration fitting. Seasonal baseline falls back to training global frequency for unseen station/month cells. Threshold monotonicity violations are reported, not repaired.

## Exact AUC (separate evaluation)

`rainfall.metrics` computes ROC-AUC and PR-AUC with Spark's `BinaryClassificationEvaluator` at its default `numBins=1000`, which downsamples the curves before integrating them; those approximate values are what `artifacts/test_metrics.json` contains. The report's per-threshold table instead uses exact values from a separate evaluation of the same saved model on the same main test split, produced by `exact_auc.py` with `numBins=0`. This step does not retrain anything and does not modify `rainfall.py` or the original outputs.

After `train` has written `data/windows` in the workspace, run from this directory:

```bash
python exact_auc.py --model artifacts/best_model --windows data/windows --output artifacts/exact_auc_test_metrics.json --compare artifacts/test_metrics.json
```

The output records the model's file hash and configuration, the windows table's hash, the split, thresholds and evaluator settings, per-threshold counts, positive rates, Brier scores and exact AUCs, and a comparison confirming that the same examples as `test_metrics.json` were scored. The committed `artifacts/exact_auc_test_metrics.json` evaluates the original submitted model; its windows table was rebuilt on macOS from a fresh download whose byte counts and SHA-256 checksums match `download_manifest.json`.

The fixed-ID panel includes stations having qualifying days in every year; it controls changing station IDs but not missing observation days. Annual valid-day counts remain essential. The additional strict panel requires 95% full-year coverage in every year; if empty, its unavailability is explicitly reported.

Do not report fixture/debug results as real-data results. The report generator marks absent results NOT RUN. Review figures and add your discussion before submission. AI assistance must be declared.
