import pandas as pd
from datetime import datetime, timedelta
import random


num = 10

data = {
    "id": range(1, num + 1),  # int
    "name": [f"User_{i}" for i in range(1, num + 1)],  # str
    "age": [random.randint(18, 65) for _ in range(num)],  # int
    "income": [round(random.uniform(30000, 120000), 2) for _ in range(num)],  # float
    "is_active": [random.choice([True, False]) for _ in range(num)],  # boolean
    "signup_date": [datetime.today() - timedelta(days=random.randint(0, 1000)) for _ in range(num)],  # datetime
    "category": [random.choice(["A", "B", "C"]) for _ in range(num)],  # category
    "comment": [random.choice(["Good", "Average", "Poor", ""]) for _ in range(num)],  # ""
}

df = pd.DataFrame(data)

csv_path = "./data/sample_rich.csv"
df.to_csv(csv_path, index=False)

csv_path
