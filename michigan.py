import bobs
import re
import pandas as pd
import numpy as np
from openpyxl import load_workbook
from datetime import datetime


class Michigan(bobs.Table):
    state = 'Michigan'

    def __init__(self, link, category, subcategory):
        self.category = category
        self.subcategory = subcategory
        category = 'Online Sports Betting (OSB)'
        self.df = pd.read_excel(link, sheet_name=0)
        self.year = re.search(r'\d{4}', self.df.columns[1])[0]
        self.header = (self.first_row_to_columns(
            self.df.iloc[:3].
            dropna(how='all', axis=1).
            T.
            reset_index(drop=True)
        ))
        self.body = (self.first_row_to_columns(
            self.df.iloc[4:18].
            replace(0, np.nan).
            dropna(thresh=4)
        ))

    def clean(self):
        data = []
        # TODO - Totals row should be similar logic.
        for month, *casinos in self.body[:-1].itertuples(index=False):
            idx = 0
            date = datetime.strptime(f'{self.year}-{month}', '%Y-%B')
            while idx < len(casinos) - 1:
                data.append({'State': self.state,
                             'Category': self.category,
                             'Sub-Category': self.subcategory,
                             'Date': date,
                             **self.header.iloc[idx // 4],
                             'Total Handle':  casinos[idx],
                             'Gross Sports Betting Receipts': casinos[idx+1],
                             'Adjusted Gross Sports Betting Receipts': casinos[idx+2],
                             'Internet Sports Betting State Tax': casinos[idx+3]})
                idx += 4
            data.append({'State': self.state,
                         'Category': self.category,
                         'Sub-Category': self.subcategory,
                         'Date': date,
                         **self.header.iloc[-1],
                         'City Wagering Taxes': casinos[-1]})
        out_df = pd.DataFrame(data)
        out_df.rename(columns={'Casino Name': 'Provider', 'Platform Providers': 'Sub-Provider'}, inplace=True)
        return out_df

if __name__ == '__main__':
    url = 'https://www.michigan.gov/mgcb/detroit-casinos/resources/revenues-and-wagering-tax-information'
    internet_sports = bobs.get_links('https://www.michigan.gov/mgcb/detroit-casinos/resources/revenues-and-wagering-tax-information', text_keys=['Internet Sports Betting'])
    parsed = [Michigan(l, 'Online Sports Betting (OSB)', 'Internet') for l in internet_sports]
    cleaned = [p.clean() for p in parsed]
    df = pd.concat(cleaned)
    df.to_excel('Michigan (OSB).xlsx', index=False)
