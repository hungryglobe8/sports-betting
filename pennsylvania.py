import bobs
import pandas as pd
from datetime import datetime

class Pennsylvania(bobs.Table):
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


class IGaming(Pennsylvania):
    category = 'iGaming'
    
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

    @staticmethod
    def save(df): 
        df.index.name = 'Index'
        df = df.replace(0, pd.NA)
        df = df.dropna(how='all', subset=['Wagers Received', 'Amount Won', 'Gross Revenue'])

        df = df.sort_values(by=['Date', 'Index'], ascending=True)
        df.to_excel('Pennsylvania (iGaming).xlsx', index=False)


class Sports(Pennsylvania):
    category = 'Online Sports Betting (OSB)'

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

    @staticmethod
    def save(df):
        df.index.name = 'Index'
        df = df.replace(0, pd.NA)
        df = df.dropna(how='all', subset=['Handle', 'Revenue', 'Promotional Credits', 'Gross Revenue'])
        
        df['Sub-Category'] = df['Sub-Category'].astype('category').cat.set_categories(['Retail', 'Online', 'Total'])
        df = df.sort_values(by=['Date', 'Index', 'Sub-Category'], ascending=True)
        df.to_excel('Pennsylvania (OSB).xlsx', index=False)


if __name__ == '__main__':
    sports = []
    igaming = []
    for i in range(2019, 2023):
        sports.append(f'https://gamingcontrolboard.pa.gov/files/revenue/Gaming_Revenue_Monthly_Sports_Wagering_FY{i}{i+1}.xlsx')
        igaming.append(f'https://gamingcontrolboard.pa.gov/files/revenue/Gaming_Revenue_Monthly_Interactive_Gaming_FY{i}{i+1}.xlsx')

    igaming_cleaned = [IGaming(i).clean() for i in igaming]
    IGaming.save(pd.concat(igaming_cleaned))

    sports_cleaned = [Sports(s).clean() for s in sports]
    Sports.save(pd.concat(sports_cleaned))
