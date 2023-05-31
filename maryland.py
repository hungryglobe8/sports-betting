import bobs
import pandas as pd
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, MONTHLY
from dateutil.relativedelta import relativedelta
from urllib.error import HTTPError

def get_links():
    links = []
    start_date = date(2022, 5, 1)
    end_date = date.today() - relativedelta(months=1)
    months = list(rrule(MONTHLY, dtstart=start_date, until=end_date))
    for m in months:
        upload_month = m + relativedelta(months=1)
        upload_str = upload_month.strftime('%Y/%m')
        data_str = m.strftime('%B-%Y')
        links.append(f'https://www.mdgaming.com/wp-content/uploads/{upload_str}/{data_str}-Sports-Wagering-Data.xlsx')
    return links


class Maryland(bobs.Table):
    state = 'Maryland'
    category = 'Online Sports Betting (OSB)'
    ordered = ['State', 'Category', 'Sub-Category', 'Date', 'Provider', 'Handle', 'Amount Won', 'Promotion Play', 'Other Deductions', 'Adjusted Gross Revenue']

    def __init__(self, link):
        try:
            self.df = pd.read_excel(link)
        except HTTPError:
            link = link.replace('Sports-Wagering', 'SW')
            self.df = pd.read_excel(link)
        self.link = link
        self.date = bobs.extract_date(self.link, r'\w+-\d{4}', '%B-%Y')

    def clean(self):
        df = pd.read_excel(self.link, skiprows=3)
        df = df.dropna(thresh=5, axis=1).dropna(subset='Licensee', how='any').dropna(thresh=5)
        df.reset_index(drop=True, inplace=True)
        slices = self.slice_by_cond(df, df['Licensee'] == 'Combined')
        retail = df.iloc[slices[0]].copy()
        retail['Sub-Category'] = 'Retail'
        online = None
        # Columns are different only take first df.
        if self.date < datetime(2022, 9, 1):
            rename_cols = {'Licensee': 'Provider',
                           'Prizes Paid': 'Amount Won',
                           'Taxable Win': 'Adjusted Gross Revenue'}
        # Take both dfs if exist.
        else:
            rename_cols = {'Licensee': 'Provider',
                           'Unnamed: 2': 'Handle',
                           'Unnamed: 3': 'Amount Won',
                           'Promotion': 'Promotion Play',
                           'Other': 'Other Deductions',
                           'Unnamed: 7': 'Adjusted Gross Revenue'}
            if len(slices) == 2:
                online = df.iloc[slices[1]].copy()
                online = online.iloc[1:]
                online['Sub-Category'] = 'Online'
        combined = pd.concat([retail, online])
        combined = combined.rename(columns=rename_cols)
        combined['State'] = self.state
        combined['Date'] = self.date
        combined['Category'] = self.category
        return combined[self.ordered].reset_index(drop=True)
    
    @staticmethod
    def save(df):
        df.to_excel('Maryland (OSB).xlsx', index=False)


if __name__ == '__main__':
    url = "https://www.mdgaming.com/maryland-sports-wagering/revenue-reports/all-financial-reports/"
    links = get_links()
    
    df = pd.concat([Maryland(l).clean() for l in links])
    Maryland.save(df)
