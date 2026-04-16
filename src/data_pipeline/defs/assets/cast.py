
from datetime import date
from src import utils
from datetime import datetime, timedelta
import dagster as dg
import pandas as pd
from pathlib import Path

dt = date.today().strftime("%Y-%m-%d")
REPO_ROOT = Path(__file__).resolve().parents[4]
data_folder = f"{REPO_ROOT}/data/"


#@dg.asset
def get_not_scraped_url():
    df = pd.read_csv(f'{data_folder}/scrape_url.csv', dtype=str)
    scraping = df[df['scraped'] != 'TRUE'][:1][['show', 'season', 'url']]
    row = scraping.values[0]
    show = utils.remove_leading_chars(row[0], 'the')
    return {"show": show, "season": row[1], "url": row[2]}

#@dg.asset
def save_wiki_html(get_not_scraped_url):
    show = get_not_scraped_url['show']
    season = get_not_scraped_url['season']
    url = get_not_scraped_url['url']

    file_name = f"{data_folder}html_raw/{show}_{season}"
    # Only scrape if it hasn't been scraped and
    # the last scrape was at least 3 days ago.
    if utils.does_file_exist(file_name):

        last_modified_time = utils.get_last_modified_time(file_name)

        if datetime.now() - datetime.fromtimestamp(last_modified_time) < timedelta(days=3):
            print(f"{file_name} already exist, returning location.")
            return {"html_file": file_name, "show": show, "season": season}
    else:
        soup = utils.scrapePage(url)

        text = soup.find_all('table', class_="wikitable sortable")
        with open(file_name, "w") as f:
            f.write(str(text))
        print(f"Saved to {file_name}")
        return {"html_file": file_name, "show": show, "season": season}


@dg.asset
def extract_to_csv(save_wiki_html):
    html_file = save_wiki_html['html_file']
    show = save_wiki_html['show']
    season = save_wiki_html['season']
    df_list = pd.read_html(html_file) # This reads into a list of dataframes.
    df = df_list[0]
    df.insert(0, ['season']) = season
    file_name = f"{REPO_ROOT}/data/csv_raw/{show}_wiki_raw.csv"
    df.to_csv(file_name, mode="a", header=False, index=False)


extract_to_csv(save_wiki_html(get_not_scraped_url()))