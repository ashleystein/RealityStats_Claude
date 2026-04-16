# Data Flow: refresh_analytics.py

## Overview

Three Dagster assets run in sequence to produce `analytics_page.csv`.

```
episode_info.csv
      │
      ▼
currently_airing_episodes
      │  List[str] — e.g. ["Bachelor 29", "Traitors 3"]
      ▼
currently_on_air_contestants  ◄── reality_contestants.csv
      │  DataFrame [name, show]
      ▼
analytics_page_source  ◄── insta_latest.csv
      │
      ▼
analytics_page.csv
```

---

## Asset 1: `currently_airing_episodes`

**Input:** `data/episode_info.csv`  
**Output:** `List[str]` — show+season strings for episodes airing within ±30 days of today

| Step | Detail |
|------|--------|
| Read | `episode_info.csv` → columns `Air Date`, `Show`, `Season` |
| Filter | Keep rows where `Air Date` is within 30 days before/after today |
| Build | Combine as `"{Show} {Season}"` (e.g. `"Bachelor 29"`) |
| Dedupe | `list(set(...))` |

---

## Asset 2: `currently_on_air_contestants`

**Input:** output of `currently_airing_episodes` + `data/reality_contestants.csv`  
**Output:** DataFrame with columns `[name, show]`

| Step | Detail |
|------|--------|
| Read | `reality_contestants.csv` → columns `name`, `show` |
| Filter | For each show in the airing list, select matching contestants |
| Build | Concatenate into a single DataFrame |

---

## Asset 3: `analytics_page_source`

**Input:** output of `currently_on_air_contestants` + `data/insta_latest.csv`  
**Output:** `data/analytics_page.csv`

| Step | Detail |
|------|--------|
| Read | `insta_latest.csv` → columns `name`, `insta_username` |
| Merge | Left join contestants on `name` to bring in `insta_username` |
| Derive | `IG Username` — markdown hyperlink: `[username](https://instagram.com/username/)` |
| Split | `Season` = last word of `show` (e.g. `"29"`) |
| Split | `Show` = `show` minus season (e.g. `"Bachelor"`) |
| Rename | `name` → `Contestant` |
| Write | Save to `data/analytics_page.csv` |

### Final columns in `analytics_page.csv`

| Column | Source |
|--------|--------|
| `Contestant` | `reality_contestants.csv` → `name` |
| `show` | `reality_contestants.csv` (original, kept) |
| `insta_username` | `insta_latest.csv` |
| `IG Username` | Derived — markdown link from `insta_username` |
| `Season` | Derived — last word of `show` |
| `Show` | Derived — `show` minus season suffix |
