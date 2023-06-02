import bobs
import pandas as pd
from urllib.parse import unquote


class NewYork(bobs.Table):
    state = 'New York'
    category = 'Online Sports Betting (OSB)'

    def __init__(self, link):
        self.link = link
        self.provider = unquote(self.link.split('/')[-1].split('.')[0].split('%20')[-1])

    def clean(self):
        data = []
        excel_file = pd.ExcelFile(self.link)
        sheets = excel_file.sheet_names
        for sheet in sheets:
            sheet_df = pd.read_excel(excel_file, sheet_name=sheet)
            sheet_df = sheet_df.loc[:, sheet_df.applymap(lambda x: 'Month' in str(x) or 'GGR' in str(x)).any()]
            sheet_df['Provider'] = self.provider
            data.append(sheet_df)
        df = pd.concat(data, ignore_index=True)
        df = df.rename(columns={'Unnamed: 0': 'Date', 'Unnamed: 3': 'GGR'})
        # convert the date_col column to datetime format
        df['Date'] = pd.to_datetime(df['Date'], format='mixed', errors='coerce')
        # keep only rows with datetime values in the date_col column
        df = df.dropna(subset=['Date', 'GGR'])
        df = df.sort_values('Date', ascending=True)
        df["GGR"] = df["GGR"].astype(int)
        df = df[["Date", "Provider", "GGR"]].reset_index(drop=True)
        df.insert(0, 'State', self.state)
        df.insert(1, 'Category', self.category)
        return df

    @staticmethod
    def save(df):
        df = df.copy()
        df = df.sort_values(by='Date', ascending=True)
        providers_filt = ['BetMGM', 'FanDuel', 'Caesars', 'DraftKings']
        df['Provider'] = df['Provider'].apply(lambda x: x if x in providers_filt else 'Others')
        df.to_excel('New York (OSB).xlsx', index=False)


if __name__ == '__main__':
    url = 'https://www.gaming.ny.gov/gaming/index.php?ID=4'
    links = bobs.get_links(url, href_keys=['Monthly Mobile Sports Wagering Report', '.xlsx'])

    df = pd.concat([NewYork(l).clean() for l in links])
    NewYork.save(df)
