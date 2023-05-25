import bobs
from PyPDF2 import PdfReader
import pypdfium2 as pdfium
from pathlib import Path
import requests
import pandas as pd
import camelot

class NewJersey(bobs.Table):
    state = 'New Jersey'

class IGaming(NewJersey):
    category = 'iGaming'
    
    def __init__(self, link):
        self.link = link
        self.timestamp = bobs.extract_date(self.link, '\w+\d{4}', '%B%Y')
        self.temp_storage = 'temp.pdf'

    def clean(self):
        # Start by saving PDF.
        self.read_pdf()
        num_pages = self.get_pages()
        # Gather data from each page.
        out = []
        for i, casino in zip(range(num_pages), self.get_casinos()):
            #table = (self.get_first_table(i).
            #         replace(r'[$ \n]', '', regex=True))
            #row = table.iloc[1:,-1].str.rstrip('-')
            out.append({'State': self.state,
                        'Category': self.category,
                        'Date': self.timestamp,
                        'Provider': casino,
                        'Bound': bound})
                        #'Online Poker': row[1],
                        #'Online Casino': row[2],
                        #'Total': row[3]})
        return pd.DataFrame(out)

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
            bound = 900
            while casino == '':
                bound -= 50
                text = textpage.get_text_bounded(bottom=bound).removeprefix('INTERNET WIN - CURRENT MONTH')
                casino = text.split('MONTHLY')[0].replace('\r\n', '')
            casinos.append((casino, bound))
        return casinos

    def get_first_table(self, page_num):
        """ Open camelot on page, getting first table. """
        tables = camelot.read_pdf(self.link, pages=str(page_num+1))
        return tables[0].df
    

if __name__ == '__main__':
    url = 'https://www.njoag.gov/about/divisions-and-offices/division-of-gaming-enforcement-home/financial-and-statistical-information/monthly-internet-gross-revenue-reports/'
    igaming = bobs.get_links(url, href_keys=['IGRTaxReturns'])[:28]  # TODO no parsing past 2019.
    out_df = []
    for link in igaming:
        try:
            out_df.append(IGaming(link).clean())
            print(f'Success - {link}')
        except bobs.InvalidExtraction as e:
            print(e.message)
        except:
            print(f'Broken - {link}')
    df = pd.concat(out_df)
    df.to_excel('New Jersey (iGaming).xlsx', index=False)
