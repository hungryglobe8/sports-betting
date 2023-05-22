import pandas as pd
from datetime import date
from dateutil.rrule import rrule, MONTHLY


class Indiana:
    xlsx_date = date(2019, 7, 1)
    
    def __init__(self, date):
        self.date = date
        self.timestamp = date.strftime("%Y-%m")
        self.url = f'https://www.in.gov/igc/files/{self.timestamp}-Revenue.xlsx'
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
        out_df.insert(0, 'State', 'Indiana')
        out_df.insert(1, 'Category', 'INDIANA GAMING COMMISSION')
        out_df.insert(2, 'Date', self.timestamp)
        out_df.rename(columns={'TOTAL TAX': 'Provider'}, inplace=True)
        return out_df

    def original_sports_betting(self):
        """ Sports betting was not recorded before September 2019, in Indiana. """
        if self.date < date(2019, 9, 1):
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
                out_df.append({'State': 'Indiana', 'Category': 'Online Sports Betting (OSB)', 'Date': self.timestamp,
                               'Provider': provider, 'Sub-Provider': sub, 'Handle': handle, 'AGR': gross})
        return pd.DataFrame(out_df)

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
    
    
def collection(start_date=date(2019, 9, 1), end_date=date.today()):
    """ Return games and sports over time period. 9/19 marks the clean data point. """
    months = list(rrule(MONTHLY, dtstart=start_date, until=end_date))
    # Start with newest months.
    months.reverse()
    games = []
    sports = []
    # CAREFUL - Changes if not reversed.
    for m in months[1:]:
        try:
            game_df, sport_df = get_single(m)
            games.append(game_df)
            sports.append(sport_df)
        except:
            raise
    return pd.concat(games), pd.concat(sports)

def get_single(month):
    """ Returns a game and sport df for a specific month. """
    x = Indiana(month.date())
    print(f'{month} - {x.url}')
    return x.clean_gaming(), x.clean_sports_betting()


if __name__ == '__main__':
    games, sports = collection()
    games.to_excel('Indiana - Gaming Commision.xlsx', index=False)
    sports.to_excel('Indiana - OSB.xlsx', index=False)