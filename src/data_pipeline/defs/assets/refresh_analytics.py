import pandas as pd
import dagster as dg
from datetime import datetime, date, timedelta
from pathlib import Path

# Repo root: .../RealityStats (4 levels up from this file: assets → defs → dagster_code → src → root)
REPO_ROOT = Path(__file__).resolve().parents[4]
data_folder = f"{REPO_ROOT}/data/"

@dg.asset
def currently_airing_episodes():
    airing = []
    data_dir = "data/episode_info.csv"
    data_file = f'{REPO_ROOT}/{data_dir}'
    df = pd.read_csv(data_file)
    # Get air dates
    air_dates = pd.to_datetime(df['Air Date'])

    if air_dates.empty:
        return False
    else:
        today = date.today()
        lower_bound = today - timedelta(days=30)
        upper_bound = today + timedelta(days=30)
        
        for index, item in air_dates.items():
            #print(index)
            if item is None or pd.isna(item):
                continue

            parsed_date = None
            if isinstance(item, datetime):
                parsed_date = item.date()
            elif isinstance(item, date):
                parsed_date = item
            elif isinstance(item, str):
                try:
                    parsed_date = datetime.fromisoformat(item).date()
                except ValueError:
                    continue
            else:
                continue
            if lower_bound <= parsed_date <= upper_bound:
                show = df.iloc[index]['Show']
                season = df.iloc[index]['Season']
                airing.append(f'{show} {season}')
    show_list = list(set(airing))
    return show_list

@dg.asset
def currently_on_air_contestants(currently_airing_episodes):
    show_list = currently_airing_episodes
    contestants = pd.DataFrame(columns = ['name', 'show'])
    df = pd.read_csv(data_folder + "reality_contestants.csv")
    for show in show_list:
        print(show)
        show_cont = df[df['show'] == show]['name']
        contes = pd.DataFrame(columns = ['name', 'show'])
        contes['name'] = show_cont
        contes['show'] = show
        contestants = pd.concat([contestants, contes])
    return contestants

@dg.asset
def analytics_page_source(currently_on_air_contestants):
    df = pd.read_csv(data_folder + "insta_latest.csv")
    merged = currently_on_air_contestants.merge(
        df[['name', 'insta_username']], on='name', how='left'
    )
    merged["IG Username"] = merged["insta_username"].apply(
        lambda u: f"[{u}](https://www.instagram.com/{u}/)" if pd.notna(u) else ""
    )
    merged.insert(1, "Season", merged["show"].str.split().str[-1])
   #merged["Season"] = merged["show"].str.split().str[-1]
    merged.insert(2, "Show", merged["show"].str.rsplit(" ", n=1).str[0])

    merged = merged.rename(columns={"name": "Contestant"})
    merged = merged.drop(columns=['show'])
    file = data_folder + "analytics_page.csv"
    merged.to_csv(file, index=False)


analytics_page_refresh_job = dg.define_asset_job(
    name="analytics_page_refresh_job",
    selection=[currently_airing_episodes, currently_on_air_contestants, analytics_page_source],
)


# def get_contestant_info(name):
#     df = pd.read_csv(data_folder + "reality_contestants.csv")
#     filtered_df = df[df['name'] == name]
#     return filtered_df[['name', 'age', 'hometown', 'state', 'show', 'eliminated']]