import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, MONTHLY
from dateutil.relativedelta import relativedelta
from time import sleep
from pathlib import Path
import camelot
import re
from PyPDF2 import PdfReader
import pypdfium2 as pdfium
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from urllib.error import HTTPError
from itertools import chain
from io import BytesIO
from zipfile import ZipFile


def get_dates(start, end=None):
    """ Returns a list of monthly datetimes ranging from start to end (today by default). """
    if not end:
        end = date.today().replace(day=1)
    return list(rrule(MONTHLY, dtstart=start, until=end))

def get_links(url, href_keys=[], text_keys=[]):
    """ Returns all links on a page which contain keywords. """
    response = requests.get(url)
    # Forbidden request, try more valid user header.
    if response.status_code == 403:
        HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36'}
        response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = []
    for link in soup.find_all('a'):
        href = link.get('href')
        if href is None:
            continue
        if all(key in href for key in href_keys) and all(key in link.text for key in text_keys):
            links.append(urljoin(url, href.replace(' ', '%20')))
    return links

def extract_date(text, regex, datefmt):
    """ Extract a date from a text through regex and datefmt. """
    return datetime.strptime(re.search(regex, text)[0], datefmt)

def save(df, filename, numeric_cols=None):
    """ 
    Save a dataframe to a file.
    
    Cleans up the numeric data by removing [($,)] and making negative where needed.
    
    Checks for existing file with filename to keep old data intact.
    """
    # Clean numeric data and remove blank rows.
    if numeric_cols:
        Table.to_numeric(df, numeric_cols)
        df = df.replace(0, pd.NA).dropna(how='all', subset=numeric_cols)
    # Look up old data, if exists.
    try:
        print('Attempting to find old data')
        matches = list(Path('Finished States').glob(filename))
        assert len(matches) <= 1, f"There should be one match for {filename} in current or sub-directories\nMatches: {matches}"
        print(f'Combining with "{matches[0]}"')
        old_df = pd.read_excel(matches[0])
        combined_df = pd.concat([old_df, df]).drop_duplicates()
        print(f'New Data {df.shape} Old Data {old_df.shape}')
        print(f'Combined {combined_df.shape}')
    except (IndexError, FileNotFoundError):
        print('No old data found')
        combined_df = df.copy()
    # Sort if columns are present. Index is usually somewhat ordered from scraping.
    combined_df = combined_df.reset_index(drop=True)
    combined_df.index.name = 'Index'
    if 'Sub-Category' in combined_df.columns:
        sorting = ['Retail', 'Online', 'Online Poker', 'Online Casino', 'Total', 'Interactive Slots', 'Banking Tables', 'Non-Banking Tables (Poker)']
        combined_df['Sub-Category'] = Table.categorize(combined_df['Sub-Category'], sorting)
    sorting = [x for x in ['Date', 'Provider', 'Sub-Category'] if x in combined_df.columns]
    sorting.append('Index')
    combined_df = combined_df.sort_values(by=sorting, ascending=True)
    combined_df.to_excel(filename, index=False)


class Table:
    @staticmethod
    def categorize(col, categories):
        """ Returns a category column, which is helpful for sorting. """
        return col.astype('category').cat.set_categories(categories)
    
    @staticmethod
    def to_numeric(df, cols):
        """ Tries to make certain columns in a dataframe numeric. Removes certain charcters. """
        # Remove weird characters.
        df[cols] = df[cols].replace(r'[$,)]', '', regex=True).replace(r'[(]', '-', regex=True)
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce', axis=1)
        df[cols] = df[cols].round(2)

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


class OSBTable(Table):
    category = 'Online Sports Betting (OSB)'
    
    @staticmethod
    def combine_old(cls, df):
        try:
            file = f'{cls.state} (OSB).xlsx'
            print('Attempting to find old data')
            matches = list(Path('Finished States').glob(file))
            assert len(matches) <= 1, f"There should be one match for {file} in current or sub-directories\nMatches: {matches}"
            print(f'Combining with "{matches[0]}"')
            old_df = pd.read_excel(matches[0])
            combined_df = pd.concat([old_df, df]).drop_duplicates()
            print(f'New Data {df.shape} Old Data {old_df.shape}')
            print(f'Combined {combined_df.shape}')
            return combined_df
        except (IndexError, FileNotFoundError):
            print('No old data found')
            return df.copy()
        
    @staticmethod
    def save(cls, df):
        df.to_excel(f'{cls.state} (OSB).xlsx', index=False)

class IGamingTable(Table):
    category = 'iGaming'

    @staticmethod
    def combine_old(cls, df):
        try:
            file = f'{cls.state} (iGaming).xlsx'
            print('Attempting to find old data')
            matches = list(Path('Finished States').glob(file))
            assert len(matches) <= 1, f"There should be one match for {file} in current or sub-directories\nMatches: {matches}"
            print(f'Combining with "{matches[0]}"')
            old_df = pd.read_excel(matches[0])
            combined_df = pd.concat([old_df, df]).drop_duplicates()
            print(f'New Data {df.shape} Old Data {old_df.shape}')
            print(f'Combined {combined_df.shape}')
            return combined_df
        except (IndexError, FileNotFoundError):
            print('No old data found')
            return df.copy() 
        
    @staticmethod
    def save(cls, df):
        df.to_excel(f'{cls.state} (iGaming).xlsx', index=False)


### State Classes ###
class Arizona(OSBTable):
    state = 'Arizona'
    url = "https://gaming.az.gov/resources/reports#event-wagering-report-archive"

    def __init__(self, url):
        self.url = url
        self.date = self.find_timestamp(self.url)

    def clean(self):
        path = Path('temp.pdf')
        path.write_bytes(requests.get(self.url).content)
        pdf = PdfReader(str(path))
        
        data = []
        # Skip first line.
        for line in pdf.pages[0].extract_text().split('\n')[1:]:
            provider = self.get_provider(line)
            if provider == '':
                break
            values = self.get_numerical(line)
            data.append({
                'State': self.state,
                'Category': self.category,
                'Sub-Category': 'Retail',
                'Date': self.date,
                'Provider': provider,
                'Gross Wagering Receipts': values[0],
                'Amount Won': values[2],
                'Adjusted Gross Wagering Receipts': values[4],
                'Promotional Credits': values[6]
            })
            data.append({
                'State': self.state,
                'Category': self.category,
                'Sub-Category': 'Online',
                'Date': self.date,
                'Provider': provider,
                'Gross Wagering Receipts': values[1],
                'Amount Won': values[3],
                'Adjusted Gross Wagering Receipts': values[5],
                'Promotional Credits': values[7]
            })
        path.unlink()
        return pd.DataFrame(data).replace(0, pd.NA).dropna(thresh=6)
    
    @staticmethod
    def find_timestamp(url):
        url = url.replace('%20', ' ')
        match = re.search(r'\w+ \d{4}', url)[0]
        match = f'{match[:3]} {match[-4:]}'
        return datetime.strptime(match, '%b %Y')
    
    @staticmethod
    def get_provider(line):
        values = line.split()
        # Find provider by everything before first '-', '$', or numeric value.
        provider = []
        for val in values:
            if val == '-' or val == '$':
                break
            else:
                val = val.replace(',', '')
                try:
                    float(val)
                    break
                except ValueError:
                    provider.append(val)
        return ' '.join(provider)

    @staticmethod
    def get_numerical(line):
        values = line.split()
        # Find values by '-', or numeric values.
        numerical = []
        for val in values:
            # Remove trailing '$'.
            val = val.strip('$')
            if '-' in val:
                numerical.append(0)
            else:
                val = val.replace(',', '')
                try:
                    numerical.append(float(val))
                except ValueError:
                    continue
        return numerical

    @staticmethod
    def save(df):
        df = OSBTable.combine_old(Arizona, df)
        df['Sub-Category'] = Table.categorize(df['Sub-Category'], ['Retail', 'Online', 'Total'])
        df = df.sort_values(by=['Date', 'Provider', 'Sub-Category'], ascending=True)
        OSBTable.save(Arizona, df)

class ConnecticutGaming(IGamingTable):
    state = 'Connecticut'
    url = "https://data.ct.gov/api/views/imqd-at3c/rows.csv?accessType=DOWNLOAD&bom=true&format=true"
    numeric_cols = ['Wagers', 'Amount Won', 'Gross Gaming Revenue', 'Promotional Credits', 'Adjusted Revenue']

    def __init__(self):
        self.df = pd.read_csv(self.url)

    def clean(self):
        out_df = pd.DataFrame({
            'State': self.state,
            'Category': self.category,
            'Date': self.df["Month Ending"],
            'Provider': self.df["Licensee"],
            'Wagers': self.df["Wagers"],
            'Amount Won': self.df["Patron Winnings"],
            'Gross Gaming Revenue': self.df["Online Casino Gaming Win/(Loss)"],
            'Promotional Credits': self.df["Promotional Coupons or Credits Wagered (3)"],
            'Adjusted Revenue': self.df["Total Gross Gaming Revenue"]
        })        
        out_df["Date"] = pd.to_datetime(out_df["Date"], format='mixed').values.astype("datetime64[M]")
        Table.to_numeric(out_df, self.numeric_cols)
        return out_df
    
    @staticmethod
    def save(df):
        df = IGamingTable.combine_old(ConnecticutGaming, df)
        df = df.sort_values(by=['Date', 'Provider'], ascending=True)
        IGamingTable.save(ConnecticutGaming, df)

class ConnecticutSports(OSBTable):
    state = 'Connecticut'
    numeric_cols = ['Wagers', 'Amount Won', 'Online Sports Wagering', 'Gross Gaming Revenue', 'Promotional Credits', 'Adjusted Revenue']

    def __init__(self, url, sub_category):
        self.url = url
        self.df = pd.read_csv(self.url)
        self.sub_category = sub_category

    def clean(self):
        out_df = pd.DataFrame({
            'State': self.state,
            'Category': self.category,
            'Sub-Category': self.sub_category,
            'Date': self.df["Month Ending"],
            'Provider': self.df["Licensee"],
            'Wagers': self.df["Wagers"],
            'Amount Won': self.df["Patron Winnings"],
            'Online Sports Wagering': self.df["Online Sports Wagering Win/(Loss)"],
            'Gross Gaming Revenue': self.df["Unadjusted Monthly Gaming Revenue"],
            'Promotional Credits': self.df["Promotional Coupons or Credits Wagered (5)"],
            'Adjusted Revenue': self.df["Total Gross Gaming Revenue"]
        })
        out_df["Date"] = pd.to_datetime(out_df["Date"], format='mixed').values.astype("datetime64[M]")
        Table.to_numeric(out_df, self.numeric_cols)
        return out_df
    
    @staticmethod
    def save(df):
        df = OSBTable.combine_old(ConnecticutSports, df)
        df = df.sort_values(by=['Date', 'Provider'], ascending=True)
        OSBTable.save(ConnecticutSports, df)

class Illinois(OSBTable):
    state = 'Illinois'
    url = "https://www.igb.illinois.gov/SportsReports.aspx"

    def __init__(self, dt, driver):
        self.date = dt
        # Downloads 'AllActivityDetail.csv' to this directory.
        file = 'AllActivityDetail.csv'
        self.download_report(driver)
        self.df = pd.read_csv(file, skiprows=3)
        # Removes 'AllActivityDetail.csv'.
        Path(file).unlink()

    def download_report(self, driver):
        """ Download a report through selenium driver by a specific date. Only month and year are important. """
        month, year = self.date.strftime('%B %Y').split()
        start_m, start_y, end_m, end_y = driver.find_elements(By.CLASS_NAME, 'interactiveDateData')
        start_m.find_element(By.TAG_NAME, 'select').send_keys(month)
        start_y.find_element(By.TAG_NAME, 'select').send_keys(year)
        end_m.find_element(By.TAG_NAME, 'select').send_keys(month)
        end_y.find_element(By.TAG_NAME, 'select').send_keys(year)
        driver.find_element(By.CSS_SELECTOR, 'input[value="ViewCSV"]').click()
        # Only one button.
        driver.find_element(By.CLASS_NAME, 'button').click()
        sleep(3)

    def clean(self):
        out_df = pd.DataFrame({
            'State': self.state,
            'Category': self.category,
            'Sub-Category': self.df['Location Type'].replace({'In-Person Wagering': 'Retail', 'Online Wagering': 'Online'}),
            'Date': self.date,
            'Provider': self.df['Licensee'],
            'Sport Level': self.df['Sport Level'],
            **self.df[['Tier 1 Wagers', 'Tier 1 Handle', 'Tier 2 Wagers', 'Tier 2 Handle']]
        })
        return out_df.replace(0, pd.NA).dropna(thresh=7)

    @staticmethod
    def selenium():
        """ Opens up selenium driver at Illinois url. Downloads to this directory. """
        options = webdriver.ChromeOptions()
        prefs = {'download.default_directory' : str(Path().absolute())}
        options.add_experimental_option('prefs', prefs)
        driver = webdriver.Chrome(options=options)
        
        driver.get(Illinois.url)
        sleep(2)
        return driver
    
    @staticmethod
    def save(df):
        df = OSBTable.combine_old(Illinois, df)
        df['Sport Level'] = Table.categorize(df['Sport Level'], ['Professional', 'College', 'Motor Race'])
        df['Sub-Category'] = Table.categorize(df['Sub-Category'], ['Retail', 'Online', 'Total'])
        df = df.sort_values(by=['Date', 'Provider', 'Sport Level', 'Sub-Category'], ascending=True)
        OSBTable.save(Illinois, df)

class Indiana(Table):
    state = 'Indiana'
    xlsx_date = date(2019, 7, 1)

    def __init__(self, dt):
        self.date = dt
        timestamp = self.date.strftime("%Y-%m")
        self.url = f'https://www.in.gov/igc/files/{timestamp}-Revenue.xlsx'
        self.gaming_df = self.original_gaming()
        self.sports_df = self.original_sports_betting()

    def original_gaming(self):
        """ HTML/PDF before July 2019. """
        # First sheet is casinos.
        df = pd.read_excel(self.url, sheet_name=0, skiprows=3)
        return df.dropna(how='all', subset=df.columns[1:], ignore_index=True).dropna(how='all', axis=1)

    def clean_gaming(self):
        df = self.gaming_df
        # Data is split into three groups, detectable by TOTAL row.
        slices = self.slice_by_cond(df, df['TOTAL TAX'] == 'TOTAL')
        # Split and get new cols.
        a, b, c = self.split_by_rows(df, slices)
        b = self.first_row_to_columns(b)
        c = self.first_row_to_columns(c)
        # Remove extra cols and backfill data where needed.
        b = (b.iloc[:,2:].
             fillna(method='ffill').
             reset_index(drop=True))
        c = (self.repeat(c, c['WAGERING TAX'] == 'Hard Rock Casino Northern Indiana').
             iloc[:,1:])
        out_df = pd.concat([a, b, c], axis=1)
        out_df.insert(0, 'State', self.state)
        out_df.insert(1, 'Category', 'iGaming')
        out_df.insert(2, 'Date', self.date)
        out_df.rename(columns={'TOTAL TAX': 'Provider'}, inplace=True)
        numeric_cols = ['Supplemental Tax', 'Sports Wagering Tax', 'Wagering Tax', 'Total Tax',
                'Win', 'Free Play', 'Other *', 'Taxable AGR', 'No. of Table Games',
                'Table Win', 'No. of EGD/Slots', 'EGD/Slot Win', 'AGR']
        Table.to_numeric(out_df, numeric_cols)
        return out_df

    def original_sports_betting(self):
        """ Sports betting was not recorded before September 2019, in Indiana. """
        if self.date < datetime(2019, 9, 1):
            return None
        else:
            # Last sheet is sports betting.
            return pd.read_excel(self.url, sheet_name=-1, skiprows=3)

    def clean_sports_betting(self):
        if self.sports_df is None:
            return None
        split_dfs = self.split_by_cols(self.sports_df, [slice(0, 4), slice(5, 9), slice(10, 14)])
        parsed_dfs = [self.parse_sports_wagers(x) for x in split_dfs]
        out_df = pd.concat(parsed_dfs).reset_index(drop=True)
        return out_df
    
    def parse_sports_wagers(self, df):
        out_df = []
        provider = None
        should_add_row = False
        total_handle = 0
        for sub, handle, gross in df.itertuples(index=False):
            if handle == "Handle":
                provider = sub
                should_add_row = True
                continue
            if should_add_row:
                # Adjustments handle is empty string.
                if sub == "Adjustments":
                    pass
                elif sub != "Taxable AGR":
                    total_handle += handle
                else:
                    sub = "Total"
                    should_add_row = False
                    handle = total_handle
                    total_handle = 0
                out_df.append({'State': self.state, 'Category': 'Online Sports Betting (OSB)', 'Date': self.date, 
                               'Provider': provider, 'Sub-Provider': sub, 'Handle': handle, 'AGR': gross})
        return pd.DataFrame(out_df)
      
    @staticmethod
    def save_games(df):
        df = IGamingTable.combine_old(Indiana, df)
        df = df.sort_values(by=['Date', 'Provider'], ascending=True)
        IGamingTable.save(Indiana, df)

    @staticmethod
    def save_sports(df):
        df = OSBTable.combine_old(Indiana, df)
        df.index.name = 'Index'
        df = df.sort_values(by=['Date', 'Index'], ascending=True)
        OSBTable.save(Indiana, df)

class Iowa(OSBTable):
    pass

class Kansas(OSBTable):
    state = 'Kansas'
    numeric_cols = ['Settled Wagers', 'Revenues', 'State Share']
    
    def __init__(self, link):
        self.link = link
        self.date = extract_date(link, r'\d{4}-\d{2}', '%Y-%m')
        # Assuming Page 1 is always current month.
        self.df = camelot.read_pdf(self.link, pages='1')[0].df
        self.df = self.df.replace('', pd.NA).dropna(how='all')

    def clean(self):
        df = self.df
        df = self.first_row_to_columns(df).reset_index(drop=True)
        # Detect splits by 1st column keyword.
        slices = self.slice_by_cond(df, df.iloc[:,0].str.contains('Subtotal'))
        # Split and get new cols.
        retail, online = self.split_by_rows(df, slices)
        retail.insert(0, 'Sub-Category', 'Retail')
        online.insert(0, 'Sub-Category', 'Online')
        # Totals ended up weird.
        totals = df.iat[-1, 0].split('\n')
        totals = [x.strip() for x in totals]
        totals_row = pd.DataFrame({'Sub-Category': 'Total',
                                   'Provider': 'Totals',
                                   'Settled Wagers': totals[2],
                                   'Revenues': totals[3],
                                   'State Share': totals[4]}, index=[0])
        # Combine.
        out_df = pd.concat([retail, online, totals_row])
        out_df.rename(columns={'Casino': 'Provider', 'Provider': 'Sub-Provider'}, inplace=True)
        out_df.insert(0, 'State', self.state)
        out_df.insert(1, 'Category', self.category)
        out_df.insert(3, 'Date', self.date)
        out_df.reset_index(drop=True, inplace=True)
        Table.to_numeric(out_df, self.numeric_cols)
        return out_df.replace(0, pd.NA).dropna(how='all', subset=self.numeric_cols)
    
    @staticmethod
    def save(df):
        df = OSBTable.combine_old(Kansas, df)
        df.index.name = 'Index'
        df = df.sort_values(by=['Date', 'Sub-Category', 'Index'], ascending=True)
        OSBTable.save(Kansas, df)

class Maryland(OSBTable):
    state = 'Maryland'
    numeric_cols = ['Handle', 'Amount Won', 'Promotion Play', 'Other Deductions', 'Adjusted Gross Revenue']
    ordered = ['State', 'Category', 'Sub-Category', 'Date', 'Provider', 'Handle', 'Amount Won', 'Promotion Play', 'Other Deductions', 'Adjusted Gross Revenue']

    def __init__(self, link):
        try:
            self.df = pd.read_excel(link)
        except HTTPError:
            link = link.replace('Sports-Wagering', 'SW')
            self.df = pd.read_excel(link)
        self.link = link
        self.date = extract_date(self.link, r'\w+-\d{4}', '%B-%Y')

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
        self.to_numeric(combined, self.numeric_cols)
        return combined[self.ordered].reset_index(drop=True)
    
    @staticmethod
    def save(df):
        df = OSBTable.combine_old(Maryland, df)
        OSBTable.save(Maryland, df)

class Michigan:
    state = 'Michigan'

    def __init__(self, df):
        self.df = df.replace(r'[\*\n]', '', regex=True)
        self.year = re.search(r'\d{4}', self.df.columns[1])[0]

    def clean(self, jump):
        data = []
        # TODO - Totals row should be similar logic.
        for month, *casinos in self.body[:-1].itertuples(index=False):
            idx = 0
            date = datetime.strptime(f'{self.year}-{month}', '%Y-%B')
            while idx < len(casinos) - 2:
                data.append({
                    'State': self.state,
                    'Category': self.category,
                    'Sub-Category': self.subcategory,
                    'Date': date,
                    **self.header.iloc[idx // jump],
                    **self.get_next(casinos, idx)
                })
                idx += jump
        out_df = pd.DataFrame(data)
        return out_df
    
class MichiganRetailSports(Michigan, OSBTable):
    def __init__(self, link):
        # PDFs are easier to parse than encrypted Excel.
        self.df = self.first_row_to_columns(camelot.read_pdf(link)[0].df).replace('', pd.NA)
        self.category = 'Online Sports Betting (OSB)'
        self.subcategory = 'Retail'
        
        super().__init__(self.df)

        self.header = pd.DataFrame(self.df.iloc[0].dropna().str.title()).reset_index(drop=True)
        self.header.columns = ['Provider']
        self.body = self.first_row_to_columns(self.df.iloc[1:16])
        # Edge case where extra dates are included.
        self.body['Month'] = self.body['Month'].apply(lambda x: x.split(' ')[0])

    def clean(self):
        return super().clean(4)

    def get_next(self, data, idx):
        return {
            'Total Handle': data[idx],
            'Total Gross Receipts': data[idx+1],
            'Adjusted Gross Receipts': data[idx+2],
            'State Tax': data[idx+3]
        }

class MichiganOnlineSports(Michigan, OSBTable):
    def __init__(self, link):
        self.df = pd.read_excel(link, sheet_name=0)
        self.category = 'Online Sports Betting (OSB)'
        self.subcategory = 'Online'
        
        super().__init__(self.df)

        self.header = (self.df.iloc[:3, 1:-1].
            dropna(how='all', axis=1).
            T.
            reset_index(drop=True)
        )
        self.header.columns = ['Operators', 'Provider', 'Sub-Provider']
        self.body = (self.first_row_to_columns(
            self.df.iloc[4:18].
            replace(0, pd.NA).
            dropna(thresh=4)
        ))
        
    def clean(self):
        return super().clean(4)

    def get_next(self, data, idx):
        return {
            'Total Handle': data[idx],
            'Total Gross Receipts': data[idx+1],
            'Adjusted Gross Receipts': data[idx+2],
            'State Tax': data[idx+3]
        }

class MichiganGaming(Michigan, IGamingTable):
    def __init__(self, link, sheet):
        self.df = pd.read_excel(link, sheet_name=sheet)
        self.category = 'iGaming'
        self.subcategory = None

        super().__init__(self.df)

        self.header = (self.df.iloc[:3, 1:-2].
            dropna(how='all', axis=1).
            T.
            reset_index(drop=True)
        )
        self.header.columns = ['Operators', 'Provider', 'Sub-Provider']
        self.body = (self.first_row_to_columns(
            self.df.iloc[4:18].
            replace(0, pd.NA).
            dropna(thresh=4)
        ))

    def clean(self):
        return super().clean(3).replace(0, pd.NA).dropna(how='all', axis=1)

    def get_next(self, data, idx):
        return {
            'Total Gross Receipts': data[idx],
            'Adjusted Gross Receipts': data[idx+1],
            'State Tax': data[idx+2]
        }

class NewJersey:
    state = 'New Jersey'
        
    def __init__(self, link):
        self.link = link
        self.date = extract_date(self.link, '\w+\d{4}', '%B%Y')
        self.temp_storage = 'temp.pdf'

    def read_pdf(self):
        """ Saves a content stream to temp_storage. """
        Path(self.temp_storage).write_bytes(requests.get(self.link).content)

    def close_pdf(self):
        """ Closes temp_storage. """
        Path(self.temp_storage).unlink()

    def get_pages(self):
        """ Gets the number of pages from temp storage. """
        return len(PdfReader(self.temp_storage).pages)

    def get_casinos(self):
        """ Open pdfium on page, getting casino header text only. """
        pdf = pdfium.PdfDocument(self.temp_storage)
        casinos = []
        for page in pdf:
            textpage = page.get_textpage()
            casino = ''
            # Using a flexible bound has proven the most successful. 2022 formatting in particular is unreliable.
            bound = 1100
            while casino == '':
                bound -= 50
                text = textpage.get_text_bounded(bottom=bound).removeprefix('INTERNET WIN - CURRENT MONTH')
                casino = text.split('MONTHLY')[0].replace('\r\n', '').title()
            casinos.append(casino)
        return casinos
    
    def get_tables(self):
        """ Open pdf through camelot, getting all tables. """
        return camelot.read_pdf(self.link, pages='all', line_scale=25)

    def get_first_table(self, page_num):
        """ Open camelot on page, getting first table. """
        tables = camelot.read_pdf(self.link, pages=str(page_num+1))
        return tables[0].df

class NewJerseyGaming(NewJersey, IGamingTable):
    def clean(self):
        """ Open PDF, read titles, and relevant first table values. """
        self.read_pdf()
        num_pages = self.get_pages()
        # Gather data from each page.
        out = []
        # Avoid parsing title if possible.
        full_list = ["Bally's Atlantic City", 'Borgata Hotel Casino & Spa', 
                     'Caesars Interactive Entertainment', 'Golden Nugget', 'Hard Rock Atlantic City', 
                     'Ocean Casino Resort', 'Resorts Digital Gaming, LLC', 'Tropicana Casino & Resort']
        extra_list = full_list.copy()
        extra_list.insert(4, 'Golden Nugget')
        casinos = {7: full_list[1:].copy(), 8: full_list.copy(), 9: extra_list}
        tables = self.get_tables()
        assert len(tables) == num_pages * 2, "Parser didn't get correct number of tables."
        for i, casino in zip(range(num_pages), casinos[num_pages]):
            table = tables[i*2].df.replace(r'[$ \n]', '', regex=True)
            row = table.iloc[1:,-1].str.rstrip('-')
            out.append({'State': self.state,
                        'Category': self.category,
                        'Sub-Category': ['Online Poker', 'Online Casino', 'Total'],
                        'Date': self.date,
                        'Provider': casino,
                        'Internet Gaming Win': [row[1], row[2], row[3]]})
        self.close_pdf()
        return pd.DataFrame(out).explode(['Sub-Category', 'Internet Gaming Win'])

class NewJerseySports(NewJersey, OSBTable):
    def clean(self):
        """ Open PDF, read titles, and get relevant values from first and third tables. """
        self.read_pdf()
        num_pages = self.get_pages()
        # Gather data from each page.
        out = []
        casinos = self.get_casinos()
        tables = self.get_tables()
        for i, casino in zip(range(num_pages), casinos):
            monthly_retail = self.get_value_from_table(tables, i*2, (3, -1))
            monthly_internet = self.get_value_from_table(tables, 1+i*2, (3, -1))
            out.append({'State': self.state,
                        'Category': self.category,
                        'Sub-Category': ['Retail', 'Online'],
                        'Date': self.date,
                        'Provider': casino,
                        'Gross Revenue': [monthly_retail, monthly_internet]})
        self.close_pdf()
        out_df = pd.DataFrame(out).explode(['Sub-Category', 'Gross Revenue'])
        out_df['Gross Revenue'] = out_df['Gross Revenue'].str.rstrip('-')
        return out_df

    def get_value_from_table(self, tables, table_num, coords):
        """ Open camelot table, extract value from coordinates. """
        df = tables[table_num].df
        return df.iat[coords]
    
class NewYork(OSBTable):
    state = 'New York'

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
        #providers_filt = ['BetMGM', 'FanDuel', 'Caesars', 'DraftKings']
        #df['Provider'] = df['Provider'].apply(lambda x: x if x in providers_filt else 'Others')
        return df

class Pennsylvania:
    state = 'Pennsylvania'

    def __init__(self, link):
        self.link = link
        self.df = pd.read_excel(link, skiprows=3)

    def get_providers(self, key):
        """ Get values above keys as providers. """
        indexes = self.df[self.df.iloc[:,0] == key].index - 1
        return self.df.iloc[indexes,0].to_list()

    def clean(self):
        """ Clean an Excel sheet. clean_row should change for type of table. """
        out_df = []
        for month, row in self.body.iterrows():
            month = datetime.strptime(month, '%B %Y')
            i_row = iter(row)
            idx = 0
            while idx < len(self.providers):
                out_df.append(self.clean_row(idx, i_row, month))
                idx += 1
        return pd.DataFrame(out_df)

class PennsylvaniaGaming(Pennsylvania, IGamingTable):
    numeric_cols = ['Wagers Received', 'Amount Won', 'Gross Revenue']

    def __init__(self, link):
        super().__init__(link)
        self.parse_columns = ['Wagers Received', 'Amount Won', 'Gross Revenue', 'Revenue (Rake & Tournament Fees)']
        self.providers = self.get_providers('Interactive Slots')
        self.body = self.df.loc[self.df.isin(self.parse_columns).any(axis=1)]
        self.body = self.body.dropna(how='all', axis=1).T.iloc[1:-1]

    def clean(self):
        return super().clean().explode(['Sub-Category', 'Wagers Received', 'Amount Won', 'Gross Revenue'])

    def clean_row(self, idx, i_row, month):
        # 1 - wagers, 2 - amount won, 3 - revenue
        islots_1, islots_2, islots_3 = next(i_row), next(i_row), next(i_row)
        ibanking_1, ibanking_3 = next(i_row), next(i_row)
        nbanking_3 = next(i_row)
        return {'State': self.state,
                'Category': self.category,
                'Sub-Category': ['Interactive Slots', 'Banking Tables', 'Non-Banking Tables (Poker)'],
                'Date': month,
                'Provider': self.providers[idx],
                'Wagers Received': [islots_1, ibanking_1, pd.NA],
                'Amount Won': [islots_2, pd.NA, pd.NA],
                'Gross Revenue': [islots_3, ibanking_3, nbanking_3]}

class PennsylvaniaSports(Pennsylvania, OSBTable):
    numeric_cols = ['Handle', 'Revenue', 'Promotional Credits', 'Gross Revenue']

    def __init__(self, link):
        super().__init__(link)
        self.df.iloc[:,0] = self.df.iloc[:,0].str.rstrip('*')
        self.parse_columns = ['Handle', 'Revenue', 'Promotional Credits', 'Gross Revenue (Taxable)']
        self.providers = self.get_providers('Total Sports Wagering')
        self.body = self.df.loc[self.df.isin(self.parse_columns).any(axis=1)]
        self.body = self.body.dropna(how='all', axis=1).T.iloc[1:-3]

    def clean(self):
        return super().clean().explode(['Sub-Category', 'Handle', 'Revenue', 'Promotional Credits', 'Gross Revenue'])

    def clean_row(self, idx, i_row, month):
        # 1 - handle, 2 - revenue, 3 - promotional, 4 - gross revenue
        total_1, total_2, total_3, total_4 = next(i_row), next(i_row), next(i_row), next(i_row)
        retail_1, retail_4 = next(i_row), next(i_row)
        online_1, online_2, online_3, online_4 = next(i_row), next(i_row), next(i_row), next(i_row)
        return {'State': self.state,
                'Category': self.category,
                'Sub-Category': ['Total', 'Retail', 'Online'],
                'Date': month,
                'Provider': self.providers[idx],
                'Handle': [total_1, retail_1, online_1],
                'Revenue': [total_2, retail_4, online_2],
                'Promotional Credits': [total_3, pd.NA, online_3],
                'Gross Revenue': [total_4, retail_4, online_4]}

class WestVirgina:
    state = 'West Virginia'

    def __init__(self, zipfile):
        self.zip = zipfile
        self.filenames = [file.filename for file in self.zip.filelist]

class WestVirginiaGaming(WestVirgina, IGamingTable):
    numeric_cols = ['Wagers', 'Amount Won', 'Revenue']

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

class WestVirginiaSports(WestVirgina, OSBTable):
    numeric_cols = ['Gross Tickets Written', 'Voids', 'Tickets Cashed', 'Total Taxable Receipts']

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

### Scraping functions ###
def scrape(data, cls, *args):
    try:
        print(f"Scraping {args}")
        data.append(cls(*args).clean())
    except BaseException as e:
        print(e.args)
        print("*Unable to scrape")
    
def scrape_arizona():
    print("Starting Arizona".center(50, '-'))
    dataframes = []
    # Arizona urls are hard-coded. Cannot be parsed automatically.
    links = [
        "https://gaming.az.gov/sites/default/files/EW%20Website%20Report%20-%20Sept%202021.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Website%20Report%20-%20Oct%202021.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Report%20for%20Website%20-%20Nov%202021.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website-Dec%202021.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20-%20Jan%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Rpt%20for%20Website%20-%20Feb%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20-%20Mar%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20April%202022%20Revenue%20Report.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Website%20Revenue%20Report-May%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20June%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20July%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Report%20for%20Website%20-%20Aug%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20-Sep%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20-%20Oct%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20-%20Nov%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20-%20Dec%202022.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20-%20Jan%202023.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Website%20Revenue%20Report-Feb%202023.pdf",
        # Attempt March in two formats.
        "https://gaming.az.gov/sites/default/files/EW%20Revenue%20Report%20for%20Website%20-%20Mar%202023.pdf",
        "https://gaming.az.gov/sites/default/files/EW%20Website%20Revenue%20Report-Mar%202023.pdf"]
    for link in links:
        print(f"Scraping {link}")
        try:
            dataframes.append(Arizona(link).clean())
        except:
            Path('temp.pdf').unlink()
            print(f"**Unable to scrape {link}")
    df = pd.concat(dataframes)
    Arizona.save(df)
    print("Finished Arizona".center(50, '-'))

def scrape_connecticut():
    print("Starting Connecticut".center(50, '-'))
    print("Scraping Connecticut iGaming")
    ConnecticutGaming.save(ConnecticutGaming().clean())
    print("Scraping Connecticut OSB")
    retail_sports_url = "https://data.ct.gov/api/views/yb54-t38r/rows.csv?accessType=DOWNLOAD&bom=true&format=true"
    retail_df = ConnecticutSports(retail_sports_url, 'Retail').clean()
    online_sports_url = "https://data.ct.gov/api/views/xf6g-659c/rows.csv?accessType=DOWNLOAD&bom=true&format=true"
    online_df = ConnecticutSports(online_sports_url, 'Online').clean()
    ConnecticutSports.save(pd.concat([retail_df, online_df]))
    print("Finished Connecticut".center(50, '-'))
    
def scrape_illinois():
    print("Starting Illinois".center(50, '-'))
    driver = Illinois.selenium()
    dataframes = []
    for dt in get_dates(date(2023, 1, 1)):
        try:
            print(f"Scraping {dt}")
            dataframes.append(Illinois(dt, driver).clean())
        except:
            print(f"**Unable to scrape {dt}")
    df = pd.concat(dataframes)
    Illinois.save(df)
    print("Finished Illinois".center(50, '-'))

def scrape_indiana():
    print("Starting Indiana".center(50, '-'))
    games_data, sports_data = [], []
    for dt in get_dates(date(2019, 9, 1)):
        try:
            print(f"Scraping {dt}")
            x = Indiana(dt)
            games_data.append(x.clean_gaming())
            sports_data.append(x.clean_sports_betting())
        except:
            print(f"**Unable to scrape {dt}")
    Indiana.save_games(pd.concat(games_data))
    Indiana.save_sports(pd.concat(sports_data))
    print("Finished Indiana".center(50, '-'))

#def scrape_iowa():

def scrape_kansas():
    print("Starting Kansas".center(50, '-'))
    dataframes = []
    url = 'https://kslottery.com/publications/sports-monthly-revenues/'
    links = get_links(url, href_keys=['media', 'revenue'])
    for link in links:
        print(f"Scraping {link}")
        try:
            dataframes.append(Kansas(link).clean())
        except:
            print("**Unable to scrape")
    df = pd.concat(dataframes)
    Kansas.save(df)
    print("Finishing Kansas".center(50, '-'))

def scrape_maryland():
    print("Starting Maryland".center(50, '-'))
    data = []
    for dt in get_dates(date(2022, 5, 1)):
        try:
            upload_month = dt + relativedelta(months=1)
            upload_str = upload_month.strftime('%Y/%m')
            data_str = dt.strftime('%B-%Y')
            link = f'https://www.mdgaming.com/wp-content/uploads/{upload_str}/{data_str}-Sports-Wagering-Data.xlsx'
            print(f"Scraping {link}")
            data.append(Maryland(link).clean())
        except:
            print("**Unable to scrape")
    df = pd.concat(data)
    Maryland.save(df)
    print("Ending Maryland".center(50, '+'))

def scrape_michigan():
    print("Starting Michigan".center(50, '-'))
    data = []
    url = 'https://www.michigan.gov/mgcb/detroit-casinos/resources/revenues-and-wagering-tax-information'
    retail_osb = get_links(url, text_keys=['Retail Sports Betting', 'PDF'])
    online_osb = get_links(url, text_keys=['Internet Sports Betting'])
    for link in retail_osb:
        try:
            print(f"Scraping {link}")
            data.append(MichiganRetailSports(link).clean())
        except:
            print("**Unable to scrape")
    for link in online_osb:
        try:
            print(f"Scraping {link}")
            data.append(MichiganOnlineSports(link).clean())
        except:
            print("**Unable to scrape")
    df = pd.concat(data)
    df = df[['State', 'Category', 'Sub-Category', 'Date', 'Operators', 'Provider', 'Sub-Provider', 
             'Total Handle', 'Total Gross Receipts', 'Adjusted Gross Receipts', 'State Tax']]
    save(df, 'Michigan (OSB).xlsx', numeric_cols=['Total Handle', 'Total Gross Receipts', 'Adjusted Gross Receipts', 'State Tax'])

    data = []
    internet_games = get_links(url, text_keys=['Internet Gaming', 'Excel'])
    for link, sheet in zip(internet_games, ['Internet Gaming 2023', 'Internet Gaming 2022', 'Internet Gaming 2021']):
        try:
            print(f"Scraping {link}, {sheet}")
            data.append(MichiganGaming(link, sheet).clean())
        except:
            print("**Unable to scrape")
    df = pd.concat(data)
    df = df[['State', 'Category', 'Date', 'Operators', 'Provider', 'Sub-Provider', 
             'Total Gross Receipts', 'Adjusted Gross Receipts', 'State Tax']]
    save(df, 'Michigan (iGaming).xlsx', numeric_cols=['Total Gross Receipts', 'Adjusted Gross Receipts', 'State Tax'])
    print("Ending Michigan".center(50, '+'))

def scrape_newjersey():
    print("Starting New Jersey".center(50, '-'))
    base_url = "https://www.nj.gov/oag/ge/docs/Financials"
    data = []
    #for dt in get_dates(date(2021, 1, 1)):
    #    month, year = dt.strftime('%B %Y').split()
    #    link = f'{base_url}/IGRTaxReturns/{year}/{month}{year}.pdf'
    #    scrape(data, NewJerseyGaming, link)
    #df = pd.concat(data)
    #save(df, 'New Jersey (iGaming).xlsx', numeric_cols=['Internet Gaming Win'])
    
    data = []
    for dt in get_dates(date(2021, 1, 1)):
        month, year = dt.strftime('%B %Y').split()
        link = f'{base_url}/SWRTaxReturns/{year}/{month}{year}.pdf'
        scrape(data, NewJerseySports, link)
    df = pd.concat(data)
    save(df, 'New Jersey (OSB).xlsx', numeric_cols=['Gross Revenue'])
    print("Ending New Jersey".center(50, '+'))

def scrape_newyork():
    print("Starting New York".center(50, '-'))
    url = 'https://www.gaming.ny.gov/gaming/index.php?ID=4'
    data = []
    links = get_links(url, href_keys=['Monthly Mobile Sports Wagering Report', '.xlsx'])
    for link in links:
        scrape(data, NewYork, link)
    df = pd.concat(data)
    save(df, 'New York (OSB).xlsx', numeric_cols=['GGR'])
    print("Ending New York".center(50, '+'))

def scrape_pennsylvania():
    print("Starting Pennsylvania".center(50, '-'))
    base_url = "https://gamingcontrolboard.pa.gov/files/revenue"
    data = []
    for i in range(2019, 2023):
        link = f'{base_url}/Gaming_Revenue_Monthly_Interactive_Gaming_FY{i}{i+1}.xlsx'
        scrape(data, PennsylvaniaGaming, link)
    df = pd.concat(data)
    save(df, 'Pennsylvania (iGaming).xlsx', numeric_cols=PennsylvaniaGaming.numeric_cols)

    data = []
    for i in range(2019, 2023):
        link = f'{base_url}/Gaming_Revenue_Monthly_Sports_Wagering_FY{i}{i+1}.xlsx'
        scrape(data, PennsylvaniaSports, link)   
    df = pd.concat(data)
    #df.sort_values(by=['Date', 'Index', 'Sub-Category'])
    save(df, 'Pennsylvania (OSB).xlsx', numeric_cols=PennsylvaniaSports.numeric_cols) 
    
    print("Ending Pennsylvania".center(50, '+'))

def scrape_westvirginia():
    print("Starting West Virgina".center(50, '-'))
    url = 'https://wvlottery.com/requests/2020-06-15-1110/?report=new'
    sports_zip = get_links(url, text_keys='Sports Wagering')[0]
    igaming_zip = get_links(url, text_keys='iGaming')[0]

    HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36'}
    szip = ZipFile(BytesIO(requests.get(sports_zip, headers=HEADERS).content))
    izip = ZipFile(BytesIO(requests.get(igaming_zip, headers=HEADERS).content))

    print(f"Scraping {sports_zip}")
    df = WestVirginiaSports(szip).clean()
    save(df, 'West Virginia (OSB).xlsx', numeric_cols=WestVirginiaSports.numeric_cols)

    print(f"Scraping {igaming_zip}")
    df = WestVirginiaGaming(izip).clean()
    save(df, 'West Virginia (iGaming).xlsx', numeric_cols=WestVirginiaGaming.numeric_cols)
    print("Ending West Virgina".center(50, '+'))

if __name__ == '__main__':
    #scrape_arizona()
    #scrape_connecticut()
    #scrape_illinois()
    #scrape_indiana()
    #scrape_iowa()
    #scrape_kansas()
    #scrape_maryland()
    #scrape_michigan()
    ##scrape_newjersey()
    #scrape_newyork()
    #scrape_pennsylvania()
    scrape_westvirginia()
