import bobs
import pandas as pd


class Connecticut(bobs.Table):
    state = 'Connecticut'
    numeric_cols = ['Wagers', 'Amount Won', 'Online Sports Wagering', 'Gross Gaming Revenue', 'Promotional Credits', 'Adjusted Revenue']

    def __init__(self, url):
        self.url = url
        self.df = pd.read_csv(self.url)


class Sports(Connecticut):
    category = 'Online Sports Betting (OSB)'

    def __init__(self, url, sub_category):
        self.sub_category = sub_category
        super().__init__(url)
    
    def clean(self):
        df = self.df.copy()
        # Change date to the first of the month.
        df["Month Ending"] = pd.to_datetime(df["Month Ending"], format='mixed').values.astype("datetime64[M]")
        out_df = pd.DataFrame({
            'State': self.state,
            'Category': self.category,
            'Sub-Category': self.sub_category,
            'Date': df["Month Ending"],
            'Provider': df["Licensee"],
            'Wagers': df["Wagers"],
            'Amount Won': df["Patron Winnings"],
            'Online Sports Wagering': df["Online Sports Wagering Win/(Loss)"],
            'Gross Gaming Revenue': df["Unadjusted Monthly Gaming Revenue"],
            'Promotional Credits': df["Promotional Coupons or Credits Wagered (5)"],
            'Adjusted Revenue': df["Total Gross Gaming Revenue"]
        })
        return out_df

    @staticmethod
    def save(df):
        bobs.Table.to_numeric(df, Sports.numeric_cols)
        df.to_excel(f'{Connecticut.state} (OSB).xlsx', index=False)


class IGaming(Connecticut):
    category = 'iGaming'
    numeric_cols = ['Wagers', 'Amount Won', 'Gross Gaming Revenue', 'Promotional Credits', 'Adjusted Revenue']

    def __init__(self, url):
        super().__init__(url)
    
    def clean(self):
        df = self.df.copy()
        # Change date to the first of the month.
        df["Month Ending"] = pd.to_datetime(df["Month Ending"], format='mixed').values.astype("datetime64[M]")
        out_df = pd.DataFrame({
            'State': self.state,
            'Category': self.category,
            'Date': df["Month Ending"],
            'Provider': df["Licensee"],
            'Wagers': df["Wagers"],
            'Amount Won': df["Patron Winnings"],
            'Gross Gaming Revenue': df["Online Casino Gaming Win/(Loss)"],
            'Promotional Credits': df["Promotional Coupons or Credits Wagered (3)"],
            'Adjusted Revenue': df["Total Gross Gaming Revenue"]
        })
        return out_df

    @staticmethod
    def save(df):
        bobs.Table.to_numeric(df, IGaming.numeric_cols)
        df.to_excel(f'{Connecticut.state} (iGaming).xlsx', index=False)


if __name__ == '__main__':
    gaming_url = "https://data.ct.gov/api/views/imqd-at3c/rows.csv?accessType=DOWNLOAD&bom=true&format=true"
    IGaming.save(IGaming(gaming_url).clean())

    retail_sports_url = "https://data.ct.gov/api/views/yb54-t38r/rows.csv?accessType=DOWNLOAD&bom=true&format=true"
    retail_df = Sports(retail_sports_url, 'Retail').clean()
    online_sports_url = "https://data.ct.gov/api/views/xf6g-659c/rows.csv?accessType=DOWNLOAD&bom=true&format=true"
    online_df = Sports(online_sports_url, 'Online').clean()
    Sports.save(pd.concat([retail_df, online_df]))