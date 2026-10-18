# Project 2 report

The written report for DSA5208 Project 2: Scalable Rainfall Forecasting with Apache Spark.

Group members: Yan Yixin, Zhang Yue, Leon Yiliang Qian

## Contents

| File | Purpose |
|---|---|
| `main.tex` | Report source |
| `rho.bib` | Bibliography |
| `rho-class/` | Two-column report template (`rho.cls` and its style files) |
| `*.png` | The five figures referenced by the report |

Every number in the report is read from the metric files in `../code/artifacts/`. This folder is separate from `../code/artifacts/rainfall_report.tex`, which is the automatically generated draft.

## Compiling

Upload this folder to Overleaf, or compile locally with a LaTeX distribution that includes `biber`:

```
pdflatex main.tex
biber main
pdflatex main.tex
pdflatex main.tex
```

Keep the figures in the same folder as `main.tex`; the report references them by file name only.
