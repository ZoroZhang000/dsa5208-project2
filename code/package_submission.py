"""Package code and experiment artifacts, excluding datasets and environments."""
from pathlib import Path
from zipfile import ZipFile,ZIP_DEFLATED
import json
from rainfall import ROOT,ART

required=['best_model','test_metrics.json','five_minute_test_metrics.json','reload_check.json','rainfall_report.tex']
missing=[name for name in required if not (ART/name).exists()]
if missing:raise SystemExit('Final experiment incomplete: '+', '.join(missing))
if json.loads((ART/'model_config.json').read_text()).get('quick_debug_run'):
    raise SystemExit('Refusing to package a quick/debug experiment as final')
target=ROOT.parent/'rainfall_submission.zip'
with ZipFile(target,'w',ZIP_DEFLATED) as z:
    for pattern in ['*.py','*.md','*.txt','pytest.ini','members.json','tests/*.py']:
        for p in ROOT.glob(pattern):z.write(p,'code/'+p.relative_to(ROOT).as_posix())
    for p in ART.rglob('*'):
        if p.is_file() and 'matplotlib_cache' not in p.parts and p.suffix not in ['.crc','.pdf'] and not p.name.startswith(('qa_','report_preview')):
            z.write(p,'code/artifacts/'+p.relative_to(ART).as_posix())
print(target)
