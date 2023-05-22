import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep
from datetime import date, datetime
from dateutil.rrule import rrule, MONTHLY
from bs4 import BeautifulSoup
import requests
import camelot
from urllib.parse import urljoin
from pathlib import Path
from PyPDF2 import PdfReader
from itertools import chain


def get_links(url, keyword):
    """ Returns all links on a page which contain a keyword. """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if keyword in href:
            links.append(urljoin(url, href))
    return links


def parse_pdf(url):
    """ Open a pdf, read titles, parse tables, and close pdf. """
    path = Path('temp.pdf')
    path.write_bytes(requests.get(url).content)

    pdf = PdfReader(str(path))
    parsed = []
    for idx, page in enumerate(pdf.pages):
        try:
            parsed.append(parse_page(str(path), page, idx))
        except:
            print(f'Unable to parse page {idx + 1} from {url}')
    path.unlink()
    return parsed


def parse_page(path, page, idx):
    """ 
    Parse a single page of the pdf. 

    Title is first non-empty line.
    Title contains Category and Date.
    Category determines Online or Sports Type.
    """
    title = get_title(page)
    category, date = title.split(' - ')
    # Skip full year for now.
    if "FY" in date:
        raise(f"FY not currently being parsed")
    table = camelot.read_pdf(path, pages=str(idx + 1))[0]
    # Check that category matches up.
    if "ONLINE SPORTS WAGERING" in category:
        return OnlineIowa(table, date)
    elif "SPORTS WAGERING REVENUE" in category:
        return SportsIowa(table, date)
    else:
        raise(f"{category} - {date} not found")


def get_title(page):
    """ Title is first nonempty line found on page. Replaces '--' and removes [$0.] """
    for line in page.extract_text().split('\n'):
        line = line.lstrip('$0.')
        if not line.isspace() and any(x in line for x in ['SPORTS WAGERING REVENUE', 'ONLINE SPORTS WAGERING']):
            line = line.replace('--', '-')
            return line.strip()


class Iowa:
    state = 'Iowa'
    
    def __init__(self, table, date):
        self.df = table.df
        self.df.iloc[:,0] = self.df.iloc[:,0].str.replace('\\', 'I')
        # Expecting date to be in June 2020 format.
        self.timestamp = datetime.strptime(date, "%B %Y")

    def clean(self):
        df = self.df
        # Detect splits by 1st column keyword
        slices = self.slice_by_cond(df, df.iloc[:,0].str.casefold() == self.keyword.casefold())
        # Split and get new cols.
        split = self.split_by_rows(df, slices)
        for i, x in enumerate(split):
            split[i] = self.first_row_to_columns(x.T)
        # Combine.
        out_df = (pd.concat(split).
                  replace(r'^\s*$', pd.NA, regex=True).
                  dropna(how='all'))
        out_df.rename(columns={out_df.columns[0]: "Provider"}, inplace=True)
        out_df.insert(0, 'State', self.state)
        out_df.insert(1, 'Category', self.category)
        out_df.insert(2, 'Date', self.timestamp)
        out_df.reset_index(drop=True, inplace=True)
        # Remove empty Providers.
        out_df.dropna(how='any', subset='Provider', inplace=True)
        self.fix_whitespace(out_df, 'Provider')
        return out_df

    @staticmethod
    def slice_by_cond(df, cond):
        """ Return slices splitting the dataframe by a certain condition. """
        idxs = df.loc[cond].index
        slices = []
        last_idx = 0
        for idx in idxs:
            slices.append(slice(last_idx, idx + 1))
            last_idx = idx + 1
        return slices
    
    @staticmethod
    def split_by_rows(df, slices):
        dfs = []
        for s in slices:
            dfs.append(df.iloc[s])
        return dfs

    @staticmethod
    def split_by_cols(df, slices):
        dfs = []
        for s in slices:
            dfs.append(df.iloc[:, s].dropna(how='all', axis=1).dropna(how='all', axis=0))
        return dfs

    @staticmethod
    def first_row_to_columns(df):
        """ Replaces the columns of a dataframe with the values in the first row. Drops that row. """
        df.columns = df.iloc[0].values
        return df[1:]

    @staticmethod
    def repeat(df, cond):
        """ Repeat rows of a dataframe where a condition is met. """
        return pd.concat([df, df.loc[cond]]).sort_index().reset_index(drop=True)

    @staticmethod
    def fix_whitespace(df, col):
        """ Double space belongs to the next row. Removes \n. """
        mistake = None
        for idx, entry in enumerate(df[col]):
            if pd.isna(entry):
                entry, next_entry = df.at[idx+1, col].split('  ')
                df.at[idx+1, col] = next_entry
            if mistake:
                entry = f'{mistake} {entry}'
                mistake = None
            # Double space is a mistake.
            if '  ' in entry:
                entry, mistake = entry.split('  ')
            df.at[idx, col] = entry.replace('\n', ' ').replace('  ', ' ')

class SportsIowa(Iowa):
    category = "TOTAL SPORTS WAGERING"
    keyword = "STATE TAX"

    def __init__(self, table, date):
        super().__init__(table, date)

class OnlineIowa(Iowa):
    category = "ONLINE SPORTS WAGERING"
    keyword = "INTERNET PAYOUTS"

    def __init__(self, table, date):
        super().__init__(table, date)


if __name__ == '__main__':
    historical = get_links('https://irgc.iowa.gov/publications-reports/sports-wagering-revenue/archived-sports-revenue', 'media')
    parsed = [parse_pdf(h) for h in historical]
    parsed = list(chain(*parsed))
    cleaned = [p.clean() for p in parsed]
    df = pd.concat(cleaned)
    df.to_excel('Iowa (OSB).xlsx')
