# RealityStats — Dagster Specific information

## Data Pipeline struture (Dagster)
├── data_pipeline/              # ETL pipeline (Dagster)
│   │   ├── definitions.py          # Dagster Definitions object
│   │   └── defs/
│   │       ├── jobs.py             # Job definitions
│   │       └── assets/
│   │           └── refresh_analytics.py  # Analytics refresh 

### Adding a new Dagster asset

```python
# src/data_pipeline/defs/assets/my_asset.py
import dagster as dg
import pandas as pd
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
data_folder = f"{REPO_ROOT}/data/"

@dg.asset
def my_new_asset(upstream_asset):
    df = pd.read_csv(data_folder + "reality_contestants.csv")
    # ... transform ...
    df.to_csv(data_folder + "my_output.csv", index=False)
```

### Defining a job over a set of assets

```python
# src/data_pipeline/defs/assets/my_asset.py
my_job = dg.define_asset_job(
    name="my_job",
    selection=[asset_one, asset_two, my_new_asset],
)
```

### Registering assets and jobs in Dagster

```python
# src/data_pipeline/definitions.py
from dagster import Definitions
from src.data_pipeline.defs.assets.my_asset import my_new_asset, my_job

defs = Definitions(
    assets=[my_new_asset],
    jobs=[my_job],
)
```