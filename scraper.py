import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, MONTHLY
from dateutil.relativedelta import relativedelta
from time import sleep
from pathlib import Path


def get_dates(start, end=None):
    """ Returns a list of monthly datetimes ranging from start to end (today by default). """
    if not end:
        end = date.today().replace(day=1)
    return list(rrule(MONTHLY, dtstart=start, until=end))

class Table:
    
    @staticmethod
    def categorize(col, categories):
        """ Returns a category column, which is helpful for sorting. """
        return col.astype('category').cat.set_categories(categories)


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
    pass


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
    scrape_illinois()
