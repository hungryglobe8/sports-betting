import bobs
from pathlib import Path
import requests
from PyPDF2 import PdfReader
from datetime import datetime
import pandas as pd
import re

def parse_pdf(url):
    """ Open a pdf, read titles, parse tables, and close pdf. """
    path = Path('temp.pdf')
    path.write_bytes(requests.get(url).content)
    pdf = PdfReader(str(path))
    return pdf

def get_lines(page):
    return [x.strip('$ ') for x in page.extract_text().split('\n')]

def extract_date(text, regex, datefmt):
    return datetime.strptime(re.search(regex, text)[0], datefmt)


class NewJersey(bobs.Table):
    state = 'New Jersey'


class IGaming(NewJersey):
    category = 'iGaming'
    
    def __init__(self, lines):
        lines[-4] = lines[-4].replace(',', '')
        self.lines = lines
        self.timestamp = extract_date(lines[-4], r'\w+ \d{4}', '%B %Y')
        self.df = self.parse_df()

    def parse_df(self):
        total_gross = 0
        adjusted_gross = 0
        subproviders = []
        for line in self.lines:
            if not total_gross and line.startswith('6'):
                total_gross = line.split()[-1]
            elif not adjusted_gross and line.startswith('8'):
                adjusted_gross = line.split()[-1]
            elif any(x in line for x in ['www', '.com']):
                subproviders.append(line)
            elif 'Title and License Number' in line:
                provider = line.split('Number')[-1]
        df = pd.DataFrame({'State': self.state,
                           'Category': self.category,
                           'Date': self.timestamp,
                           'Provider': provider,
                           'Sub-Provider': subproviders,
                           'Total Gross Receipts': total_gross,
                           'Adjusted Gross Receipts': adjusted_gross})
        return df
    

if __name__ == '__main__':
    url = 'https://www.njoag.gov/about/divisions-and-offices/division-of-gaming-enforcement-home/financial-and-statistical-information/monthly-internet-gross-revenue-reports/'
    igaming = bobs.get_links(url, href_keys=['IGRTaxReturns'])
    first = igaming[0]
    last = igaming[-1]
    pdf = parse_pdf(first)
    out_df = []
    for page in pdf.pages:
        lines = get_lines(page)
        out_df.append(IGaming(lines).df)
    df = pd.concat(out_df)
    df.to_excel('New Jersey (iGaming).xlsx', index=False)
