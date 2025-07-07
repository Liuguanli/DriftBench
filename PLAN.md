autoquerybench/
├── __init__.py
├── config/
│   ├── settings.yaml              # 全局参数配置
│   └── templates/                 # 可选预定义模板（JSON格式）
├── core/
│   ├── schema_extractor.py       # 读取CSV/PG schema + 类型分类
│   ├── template_generator.py     # 生成结构化查询模板
│   ├── query_instantiator.py     # 根据模板生成SQL语句
│   ├── drift_engine.py           # 模拟数据漂移（添加/删除）
│   └── utils.py                  # 公共函数（连接数据库、日志等）
├── data/
│   ├── sample.csv                # 示例输入数据
│   └── drifted_sample.csv        # Drift 后数据
├── output/
│   ├── workload.sql              # 生成的查询语句
│   └── log.json                  # 日志记录
├── notebooks/
│   └── demo.ipynb                # 用法演示
├── cli.py                        # 命令行入口
└── main.py                       # 主入口脚本（支持 import 和 CLI）
