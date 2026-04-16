---
name: realitystats-code-examples
description: Project-specific code examples for the RealityStats codebase
---
# RealityStats — Project-Specific Examples

dagster - ./dagster.md
dagster - ./dash.md

## Config & Environment

### Checking current environment

```python
import config as cfg
env = cfg.get_config().env   # 'dev' or 'prod'
```

### Branching on dev vs prod for data loading

```python
def get_display_data():
    if cfg.get_config().env == 'dev':
        return pd.read_csv(os.path.join(ROOT_DIR, "data/analytics_page.csv"))
    else:
        return aws.get_s3_file("data/analytics_page.csv")
```

### Setting the environment in .env

```
APP_ENV=dev   # or prod
LOG_LEVEL=DEBUG
```

---

## AWS Utilities

### Reading a CSV from S3

```python
import aws
df = aws.get_s3_file("data/analytics_page.csv")   # bucket defaults to 'realitystats'
```

