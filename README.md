# Portus: NL queries for `pandas`

## Setup connection

```python
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://readonly_role:>sU9y95R(e4m@ep-young-breeze-a5cq8xns.us-east-2.aws.neon.tech/netflix"
)
```

## SQL query with `sqlalchemy` + `pandas`

```python
import pandas

df = pd.read_sql(
    """
    SELECT *
    FROM netflix_shows
    WHERE country = 'Germany'
    """,
    engine
)
print(df)
```

## NL query with `sqlalchemy` + `pandas` + `portus`

```python
import portus
portus.init()

df = pd.read_ai("list all german shows", engine)
print(df)
```

