# RealityStats Claude

## Project Overview

<!-- A dashboard web application with Reality TV data. -->

## Development Guidelines

<!-- Add coding conventions, style guides, and best practices here

-->

## Architecture

<!-- 
Frontend: Made using Dash and Plotly
Backend: ETL pipeline orchestrated using Dagster
Infrastructure: AWS services
-->

## Project Structure

```
RealityStats_Claude/
├── .claude/
│   └── CLAUDE.md
├── .vscode/
│   └── launch.json
├── data/
│   ├── raw/                        # Raw source CSVs (Bachelor, Bachelorette, Traitors)
│   ├── html_raw/                   # Raw HTML content
│   ├── analytics_page.csv
│   ├── episode_info.csv
│   ├── insta_latest.csv
│   ├── reality_contestants.csv
│   ├── show_details.csv
│   └── wiki_urls.csv
├── src/
│   ├── app.py                      # Main application entry point
│   ├── aws.py                      # AWS integration
│   ├── config.py                   # Configuration management
│   ├── utils.py                    # Shared utilities
│   ├── data_pipeline/              # ETL pipeline (Dagster)
│   │   ├── definitions.py          # Dagster Definitions object
│   │   └── defs/
│   │       ├── jobs.py             # Job definitions
│   │       └── assets/
│   │           └── refresh_analytics.py  # Analytics refresh assets
│   └── pages/
│       └── analytics.py            # Analytics page (Dash)
├── pyproject.toml
└── .env
```

## Commands

<!-- List common commands used in this project -->

## Notes

<!-- Any additional context for Claude to be aware of -->
