import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import pandas as pd


def get_links(url, href_keys=[], text_keys=[]):
    """ Returns all links on a page which contain a keyword. """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if all(key in href for key in href_keys) and all(key in link.text for key in text_keys):
            links.append(urljoin(url, href))
    return links


class Table:
    def clean(self):
        """ Work should happen in this function for any subclasses. """
        raise NotImplementedError

    @staticmethod
    def first_row_to_columns(df):
        """ Replaces the columns of a dataframe with the values in the first row. Drops that row. """
        df.columns = df.iloc[0].values
        return df[1:].reset_index(drop=True)
    
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
    def repeat(df, cond):
        """ Repeat rows of a dataframe where a condition is met. """
        return pd.concat([df, df.loc[cond]]).sort_index().reset_index(drop=True)

    # Might be too specific.
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