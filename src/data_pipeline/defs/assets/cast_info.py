import pandas as pd
import dagster as dg
from pathlib import Path
from src.utils import remove_references


REPO_ROOT = Path(__file__).resolve().parents[4]
raw_folder = f"{REPO_ROOT}/data/csv_raw/"
processed_folder = f"{REPO_ROOT}/data/processed/"


@dg.asset
def raw_bachelorette_cast() -> pd.DataFrame:
    file = raw_folder + "bachelorette_wiki_raw.csv"
    df = pd.read_csv(file, dtype=str)
    return df


@dg.asset
def processed_bachelorette_cast(raw_bachelorette_cast: pd.DataFrame) -> pd.DataFrame:
    df = raw_bachelorette_cast.copy()

    # Strip whitespace from all string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip().map(remove_references))
    df = df.rename(columns={"occupation": "job", "outcome": "eliminated"})

    # Add show and show_season columns
    df["show"] = "The Bachelorette"

    # Write processed output
    Path(processed_folder).mkdir(parents=True, exist_ok=True)
    output_file = processed_folder + "bachelorette_cast.csv"
    df.to_csv(output_file, index=False)

    return df


cast_info_job = dg.define_asset_job(
    name="cast_info_job",
    selection=[raw_bachelorette_cast, processed_bachelorette_cast],
)