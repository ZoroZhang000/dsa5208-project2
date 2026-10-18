# Project 2 report

The written report for DSA5208 Project 2: Scalable Rainfall Forecasting with Apache Spark.

Group members: Yan Yixin, Zhang Yue, Leon Yiliang Qian

## Contents

| File | Purpose |
|---|---|
| `main.tex` | Report source |
| `rho.bib` | Bibliography |
| `rho-class/` | Two-column report template (`rho.cls` and its style files) |
| `*.png` | The seven figures referenced by the report |

Reported numerical results were checked against the preserved outputs in `../code/artifacts/`, including the separate exact-AUC evaluation. This folder is separate from `../code/artifacts/rainfall_report.tex`, which is the automatically generated draft.

## Compiling

Upload this folder to Overleaf, or compile locally with a LaTeX distribution that includes `biber`:

```
pdflatex main.tex
biber main
pdflatex main.tex
pdflatex main.tex
```

Keep the figures in the same folder as `main.tex`; the report references them by file name only.
