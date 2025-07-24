import json
from pathlib import Path
from typing import Dict
from driftbench.core.workload.sql_renderer import PostgreSQLRenderer
from driftbench.core.workload.sql_renderer import PredicateValueSampler


# Load template JSON
template_file = Path("./autoquerybench/data/sample_templates.json")
with open(template_file) as f:
    templates = json.load(f)


dist_config = {
    "sample_rich.age": {
        "distribution": "normal",
        "mean": 100,
        "std": 5
    },
    "sample_rich.category": {
        "distribution": "choice",
        "choices": ["A", "B", "C"]
    },
    "sample_rich.signup_date": {
        "distribution": "fixed",
        "value": "2021-01-01"
    }
}

sampler = PredicateValueSampler(dist_config=dist_config, seed=42)
# value = sampler.sample("age", "numeric")

# Instantiate the renderer
renderer = PostgreSQLRenderer(sampler)

# Render all SQLs from templates
# sql_outputs = [renderer.render(t) for t in templates]

for t in templates:
    print('-' * 50)
    for _ in range(10):
        renderer.render(t)
        print(renderer.render(t))
    break

# print(sql_outputs)