# DSA5208 Project 2 — Singapore Rainfall

使用 Apache Spark 对新加坡 2017–2024 年降雨记录进行探索分析，并预测站点未来 30 分钟累计降雨超过指定阈值的概率。

## 已完成实验

- 清洗后记录：48,538,625；历史和未来窗口完整的预测时刻：47,048,853。
- 时间划分：2017–2021 训练，2022 验证，2023–2024 测试。
- 为控制本地资源，训练起点按固定哈希均匀抽取 10%；验证和测试保留完整样本，不做类别抽样。
- 比较逻辑回归、随机森林和 GBT，共 11 个参数组合；另含总体频率与站点月份频率基线、F0/F1/F2 特征消融。
- 最佳模型：GBT，60 次迭代、最大深度 5。
- 主测试（30 分钟起点）平均 Brier：0.014605；完整五分钟测试平均 Brier：0.014598。
- 完整五分钟测试包含 13,025,429 个预测时刻，每个展开为 0、1、5、10 mm 四个阈值。
- 8 项验收测试通过；正式模型保存后重新加载的 20 条预测一致性检查通过。

## 文件位置

| 内容 | 路径 |
|---|---|
| 运行说明 | [code/README.md](code/README.md) |
| Spark 数据处理、训练与预测 | [code/rainfall.py](code/rainfall.py) |
| LaTeX 报告 | [code/artifacts/rainfall_report.tex](code/artifacts/rainfall_report.tex) |
| 报告图表 | [code/artifacts/figures/](code/artifacts/figures/) |
| 已训练的完整 MLlib PipelineModel | [code/artifacts/best_model/](code/artifacts/best_model/) |
| 参数比较 | [model_comparison.json](code/artifacts/model_comparison.json) |
| 主测试指标 | [test_metrics.json](code/artifacts/test_metrics.json) |
| 五分钟测试指标 | [five_minute_test_metrics.json](code/artifacts/five_minute_test_metrics.json) |
| 测试代码 | [code/tests/](code/tests/) |

## 复现

实测环境为 Windows 上的 WSL2 Ubuntu 24.04，Python 3.12、Java 11、Spark 3.5.3；Spark `local[6]`，Driver 内存 4 GB，Shuffle 分区数 16。Java 由 `jdk4py` 提供。推荐通过 VS Code Remote - WSL 运行。

```bash
git clone https://github.com/ZoroZhang000/dsa5208-project2.git
cd dsa5208-project2/code
python3 -m venv .venv-wsl
.venv-wsl/bin/python -m pip install -r requirements.lock.txt
.venv-wsl/bin/python -m pytest tests -q
.venv-wsl/bin/python rainfall.py download
SPARK_MASTER='local[6]' .venv-wsl/bin/python rainfall.py prepare
SPARK_MASTER='local[6]' .venv-wsl/bin/python run_experiments.py --from-stage train --training-fraction 0.1
```

下载源为 [NEA Historical Rainfall across Singapore](https://data.gov.sg/collections/2279/view)。原始 CSV、清洗及特征 Parquet、虚拟环境和调试输出不包含在仓库中，运行下载及处理命令后生成。已有模型的预测也需要本地清洗后的历史观测。

```bash
.venv-wsl/bin/python rainfall.py predict \
  --station S77 --time '2024-01-01T01:00:00+08:00' --threshold 5
```

报告作者栏留待填写。编译 LaTeX 时应保留同目录下的 `figures/`；源文件已做结构与图表引用检查，尚未实际编译。

## 数据与解释边界

缺测不填零；未来窗口必须包含六个完整五分钟观测。少数早期时间戳做一秒内对齐。异常正值保留并审计。全年覆盖率和有效日数量影响累计量，因此报告同时给出覆盖率、有效日均值与固定站点比较；不能据此断言长期气候趋势。

预测假设时刻 t 的观测已经可用，未验证实时发布延迟。阈值单调性经过统计检查但未强制约束；非训练阈值和独立新站点的泛化能力未单独验证。

AI 使用声明：OpenAI Codex 辅助了实验设计、实现、运行和 LaTeX 报告生成；组员需审阅结果与报告，并填写姓名及学号。
