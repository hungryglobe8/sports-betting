import bobs
from PyPDF2 import PdfReader
import requests
from pathlib import Path
import pandas as pd
import re
from datetime import datetime

def find_timestamp(url):
    url = url.replace('%20', ' ')
    match = re.search(r'\w+ \d{4}', url)[0]
    match = f'{match[:3]} {match[-4:]}'
    return datetime.strptime(match, '%b %Y')

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


class Arizona(bobs.Table):
    state = 'Arizona'
    category = 'Online Sports Betting (OSB)'
    numeric_columns = ['Gross Wagering Receipts', 'Amount Won', 'Adjusted Gross Wagering Receipts', 'Promotional Credits']

    def __init__(self, url):
        self.url = url
        self.date = find_timestamp(self.url)

    def clean(self):
        path = Path('temp.pdf')
        path.write_bytes(requests.get(self.url).content)
        pdf = PdfReader(str(path))
        
        data = []
        # Skip first line.
        for line in pdf.pages[0].extract_text().split('\n')[1:]:
            provider = get_provider(line)
            if provider == '':
                break
            values = get_numerical(line)
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
        return pd.DataFrame(data)

    @staticmethod
    def save(df):
        df = df.replace(0, pd.NA)
        df = df.dropna(how='all', subset=Arizona.numeric_columns)
        df.to_excel('Arizona (OSB).xlsx', index=False)


if __name__ == '__main__':
    url = "https://gaming.az.gov/resources/reports#event-wagering-report-archive"
    links = ["https://gaming.az.gov/sites/default/files/EW%20Website%20Report%20-%20Sept%202021.pdf",
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
            "https://gaming.az.gov/sites/default/files/EW%20Website%20Revenue%20Report-Feb%202023.pdf"]

    df = pd.concat([Arizona(l).clean() for l in links])
    Arizona.save(df)