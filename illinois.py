import bobs
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, MONTHLY
from dateutil.relativedelta import relativedelta
from time import sleep
from pathlib import Path


def launch_selenium():
    url = "https://www.igb.illinois.gov/SportsReports.aspx"

    # Setup selenium to download to this directory.
    options = webdriver.ChromeOptions()
    prefs = {'download.default_directory' : str(Path().absolute())}
    options.add_experimental_option('prefs', prefs)
    driver = webdriver.Chrome(options=options)
    
    driver.get(url)
    sleep(1)
    return driver

def get_dates():
    """ Start date range from previous Excel data or from 3/1/20. Returns a list of useful dates. """
    try:
        old_file = list(Path().rglob('Illinois (OSB).xlsx'))[0]
        old_dates = pd.read_excel(old_file)['Date']
        start_date = old_dates.dt.date.max() + relativedelta(months=1)
    except:
        start_date = date(2020, 3, 1)
    end_date = date.today().replace(day=1) - relativedelta(months=3)
    return list(rrule(MONTHLY, dtstart=start_date, until=end_date))

def download_report(dt, driver):
    """ Download a report through selenium driver by a specific date. Only month and year are important. """
    month = dt.strftime('%B')
    year = dt.strftime('%Y')
    start_m, start_y, end_m, end_y = driver.find_elements(By.CLASS_NAME, 'interactiveDateData')
    start_m.find_element(By.TAG_NAME, 'select').send_keys(month)
    start_y.find_element(By.TAG_NAME, 'select').send_keys(year)
    end_m.find_element(By.TAG_NAME, 'select').send_keys(month)
    end_y.find_element(By.TAG_NAME, 'select').send_keys(year)
    driver.find_element(By.CSS_SELECTOR, 'input[value="ViewCSV"]').click()
    # Only one button.
    driver.find_element(By.CLASS_NAME, 'button').click()
    sleep(3)
    

class Illinois(bobs.Table):
    state = 'Illinois'
    category = 'Online Sports Betting (OSB)'
    ordered = ['State', 'Category', 'Sub-Category', 'Date', 'Provider', 'Sport Level', 'Tier 1 Wagers', 'Tier 1 Handle', 'Tier 2 Wagers', 'Tier 2 Handle']
    file = 'AllActivityDetail.csv'

    def __init__(self, dt, driver):
        self.date = dt
        # Should download 'AllActivityDetail.csv' to this directory.
        download_report(dt, driver)
        self.df = pd.read_csv(self.file, skiprows=3)
        # Clean up temporary download.
        Path(self.file).unlink()

    def clean(self):
        df = self.df.copy()
        df = df.dropna(how='all', axis=1)
        df = df.rename(columns={'Location Type': 'Sub-Category', 'Licensee': 'Provider'})
        df['Sub-Category'] = df['Sub-Category'].replace({'In-Person Wagering': 'Retail', 'Online Wagering': 'Online'})
        df.insert(0, 'State', self.state)
        df.insert(1, 'Category', self.category)
        df.insert(2, 'Date', self.date)
        return df[self.ordered]

    @staticmethod
    def save(df):
        try:
            old_file = list(Path().rglob('Illinois (OSB).xlsx'))[0]
            old_df = pd.read_excel(old_file)
            df = pd.concat([old_df, df]).drop_duplicates()
        except:
            df = df.copy()
        # Remove zeros
        df = df.replace(0, pd.NA)
        df = df.dropna(thresh=7)
        # Set sorting orders
        df['Sport Level'] = df['Sport Level'].astype('category').cat.set_categories(['Professional', 'College', 'Motor Race'])
        df['Sub-Category'] = df['Sub-Category'].astype('category').cat.set_categories(['Retail', 'Online', 'Total'])
        df = df.sort_values(['Date', 'Provider', 'Sport Level', 'Sub-Category'], ascending=True)
        df.to_excel('Illinois (OSB).xlsx', index=False)


if __name__ == '__main__':
    driver = launch_selenium()
    dates = get_dates()
    df = pd.concat([Illinois(dt, driver).clean() for dt in dates])
    Illinois.save(df)
