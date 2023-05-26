import bobs
import requests
import pandas as pd
from io import BytesIO
from zipfile import ZipFile

class WestVirgina(bobs.Table):
    state = 'West Virginia'

    def __init__(self, zipfile):
        self.zip = zipfile
        self.filenames = [file.filename for file in self.zip.filelist]


class IGaming(WestVirgina):
    category = 'iGaming'

    def __init__(self, zipfile):
        super().__init__(zipfile)
        self.sheetnames = ['Mountaineer', 'Charles Town', 'Greenbrier']
    
    def clean(self):
        dataframes = []
        for file in self.filenames:
            for sheet in self.sheetnames:
                df = pd.read_excel(self.zip.open(file), sheet_name=sheet, skiprows=2)
                # Clean columns.
                df.columns = df.columns.str.rstrip('* ')
                df = df.rename(columns={'Week Ending': 'Date', 'Paids': 'Amount Won'})
                # Get relevant dates.
                df = df.replace(r'[\* ]', '', regex=True)
                df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce').dropna()
                grouped_df = df.groupby(pd.Grouper(key='Date', freq='MS')).sum()
                grouped_df.reset_index(inplace=True)
                grouped_df.insert(0, 'State', self.state)
                grouped_df.insert(1, 'Category', self.category)
                grouped_df.insert(3, 'Provider', sheet)
                dataframes.append(grouped_df)
        return pd.concat(dataframes)[['State', 'Category', 'Date', 'Provider', 'Wagers', 'Amount Won', 'Revenue']]

    @staticmethod
    def save(df):
        df = df.replace(0, pd.NA)
        df = df.dropna(subset=['Wagers', 'Amount Won', 'Revenue'], how='all')
        df = df.sort_values(by=['Date', 'Provider'], ascending=True)
        df.to_excel('West Virginia (iGaming).xlsx', index=False)


class Sports(WestVirgina):
    category = 'Online Sports Betting (OSB)'
    
    def __init__(self, zipfile):
        super().__init__(zipfile)
        self.sheetnames = ['Mountaineer', 'Wheeling', 'Mardi Gras', 'Charles Town', 'Greenbrier']

    def clean(self):
        dataframes = []
        for file in self.filenames:
            for sheet in self.sheetnames:
                df = pd.read_excel(self.zip.open(file), sheet_name=sheet, skiprows=3)
                df = df.rename(columns={df.columns[0]: 'Date'})
                # Get relevant dates.
                df = df.replace(r'[\* ]', '', regex=True)
                df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y', errors='coerce').dropna()
                df = df.dropna(how='all', axis=1).dropna()
                
                # Parse sub-categories.
                retail = df.iloc[:,:5]
                retail.columns = ['Date', 'Gross Tickets Written', 'Voids', 'Tickets Cashed', 'Total Taxable Receipts']
                retail.insert(0, 'Sub-Category', 'Retail')
                online = df.iloc[:,[0,5,6,7,8]]
                online.columns = ['Date', 'Gross Tickets Written', 'Voids', 'Tickets Cashed', 'Total Taxable Receipts']
                online.insert(0, 'Sub-Category', 'Online')
                total = df.iloc[:,[0,9,10,11,12]]
                total.columns = ['Date', 'Gross Tickets Written', 'Voids', 'Tickets Cashed', 'Total Taxable Receipts']
                total.insert(0, 'Sub-Category', 'Total')
                combined_df = pd.concat([retail, online, total], ignore_index=True)

                # Set Date as index for Grouper.
                combined_df = combined_df.set_index('Date')
                grouped_df = combined_df.groupby([pd.Grouper(freq='MS'), 'Sub-Category']).sum()
                grouped_df.reset_index(inplace=True)
                grouped_df.insert(0, 'State', self.state)
                grouped_df.insert(1, 'Category', self.category)
                grouped_df.insert(4, 'Provider', sheet)
                dataframes.append(grouped_df)
        return pd.concat(dataframes)[['State', 'Category', 'Sub-Category', 'Date', 'Provider', 'Gross Tickets Written', 'Voids', 'Tickets Cashed', 'Total Taxable Receipts']]

    @staticmethod
    def save(df):
        df = df.replace(0, pd.NA)
        df = df.dropna(how='all', subset=['Gross Tickets Written', 'Voids', 'Tickets Cashed', 'Total Taxable Receipts'])
        df['Sub-Category'] = df['Sub-Category'].astype('category').cat.set_categories(['Retail', 'Online', 'Total'])
        df = df.sort_values(by=['Date', 'Provider', 'Sub-Category'], ascending=True)
        df.to_excel('West Virginia (OSB).xlsx', index=False)


if __name__ == '__main__':
    sports_zip = bobs.get_links('https://wvlottery.com/requests/2020-06-15-1110/?report=new', text_keys='Sports Wagering')[0]
    igaming_zip = bobs.get_links('https://wvlottery.com/requests/2020-06-15-1110/?report=new', text_keys='iGaming')[0]

    HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36'}
    szip = ZipFile(BytesIO(requests.get(sports_zip, headers=HEADERS).content))
    izip = ZipFile(BytesIO(requests.get(igaming_zip, headers=HEADERS).content))

    df = Sports(szip).clean()
    Sports.save(df)

    df = IGaming(izip).clean()
    IGaming.save(df)