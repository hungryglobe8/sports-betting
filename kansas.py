import camelot
import pandas as pd
from datetime import datetime
import re
import bobs


class Kansas(bobs.Table):
    state = 'Kansas'
    category = 'Online Sports Betting (OSB)'
    
    def __init__(self, link):
        self.link = link
        # Extract time from link.
        extracted_time = re.search(r'\d{4}-\d{2}', link)[0]
        self.timestamp = datetime.strptime(extracted_time, '%Y-%m')
        # Assuming Page 1 is always current month.
        self.df = camelot.read_pdf(self.link, pages='1')[0].df
        self.df = self.df.replace('', pd.NA).dropna(how='all')

    def clean(self):
        df = self.df
        df = self.first_row_to_columns(df)
        # Detect splits by 1st column keyword.
        slices = self.slice_by_cond(df, df.iloc[:,0].str.contains('Subtotal'))
        # Split and get new cols.
        retail, online = self.split_by_rows(df, slices)
        retail.insert(0, 'Segment', 'Retail')
        online.insert(0, 'Segment', 'Online')
        # Totals ended up weird.
        totals = df.iat[-1, 0].split('\n')
        totals = [x.strip() for x in totals]
        totals_row = pd.DataFrame({'Segment': 'Total',
                                   'Provider': 'Totals',
                                   'Settled Wagers': totals[2],
                                   'Revenues': totals[3],
                                   'State Share': totals[4]}, index=[0])
        # Combine.
        out_df = pd.concat([retail, online, totals_row])
        out_df.rename(columns={'Casino': 'Provider', 'Provider': 'Sub-Provider'}, inplace=True)
        out_df.insert(0, 'State', self.state)
        out_df.insert(1, 'Category', self.category)
        out_df.insert(2, 'Date', self.timestamp)
        out_df.reset_index(drop=True, inplace=True)
        return out_df


if __name__ == '__main__':
    links = bobs.get_links('https://kslottery.com/publications/sports-monthly-revenues/', ['media', 'revenue'])
    parsed = [Kansas(l) for l in links]
    cleaned = [p.clean() for p in parsed]
    df = pd.concat(cleaned)
    df.to_excel('Kansas (OSB).xlsx', index=False)
