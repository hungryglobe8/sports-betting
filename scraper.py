import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, MONTHLY
from dateutil.relativedelta import relativedelta
from time import sleep
from pathlib import Path
import re
from PyPDF2 import PdfReader
import requests


def get_dates(start, end=None):
    """ Returns a list of monthly datetimes ranging from start to end (today by default). """
    if not end:
        end = date.today().replace(day=1)
    return list(rrule(MONTHLY, dtstart=start, until=end))

def extract_date(text, regex, datefmt):
    """ Extract a date from a text through regex and datefmt. """
    return datetime.strptime(re.search(regex, text)[0], datefmt)

class Table:
    @staticmethod
    def categorize(col, categories):
        """ Returns a category column, which is helpful for sorting. """
        return col.astype('category').cat.set_categories(categories)
    
    @staticmethod
    def to_numeric(df, cols):
        """ Tries to make certain columns in a dataframe numeric. Removes commas. """
        # Remove commas.
        df[cols] = df[cols].replace(r',', '', regex=True)
        df[cols] = df[cols].apply(pd.to_numeric, errors='coerce', axis=1)


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



### Scraping functions ###
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



if __name__ == '__main__':
    #scrape_arizona()
    scrape_connecticut()
    #scrape_illinois()
