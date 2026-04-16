import re
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup
import requests

# String cleaning
def clean_strings(raw_string, type=''):
    str_clean =  raw_string.text.replace('\n', '')
    if type == 'age':
        return str_clean
    else:
        return re.sub(r'^[^a-zA-Z]+|[^a-zA-Z]+$', '', str_clean)


def remove_leading_chars(orig_str, remove_char):
    text = orig_str.lower()
    new_text = text.lstrip(remove_char)
    return new_text.strip()

def remove_references(text: str):
    if not isinstance(text, str):
        return text
    pattern = r'\[([0-9]+|[a-z]+)\]'
    return re.sub(pattern, '', text)

def remove_weired_chars(text: str):
    if not isinstance(text, str):
        return text
    pattern = r'\[([0-9]+|[a-z]+)\]'
    return re.sub(pattern, '', text)


def does_file_exist(file_name):
    return Path(file_name).is_file()

def get_last_modified_time(file):

    file_path = Path(file)

    # Get the timestamp
    timestamp = file_path.stat().st_mtime

    return timestamp


## Web scraping 
def get_next_page_imdb(curr, soup):
    """
    Returns the link to the next page
    from imdb page
    :param curr: current page
    :param soup:
    :return:
    """
    link = soup.find_all('a', class_='flat-button lister-page-next next-page')
    if len(link) == 1:
        return link[0].attrs['href']
    else:
        return False


def scrapePage(url):
    # PROD
    # secret_name = 'agent_for_wiki_scraping'
    # agent = aws.get_secret(secret_name)
    #try:
        # response = requests.get(
        #     url=url, headers={'user-agent': agent['UserAgent']}
        # 
    #DEV
    agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
    try:
        response = requests.get(
            url=url, headers={'user-agent': agent}
        )
        return BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        print('Error occurred while getting the page, ' + str(e))