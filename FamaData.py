import pandas as pd
import json
from pandas.tseries.offsets import MonthEnd

class FamaData:
    def __init__(self, path, end_date = None):
        self.path = path
        self.dir = self.load_json()
        self.end_date = end_date

    def load_json(self):
        dir = json.load(open(self.path + 'data.json'))
        return dir
    
    def get_all_data(self):
        dfs = []
        for file in self.dir:
            df = pd.read_csv('data/' + file, skiprows=self.dir[file]['skip'], nrows=self.dir[file]['lines'], index_col=0)
            df['caldt'] = pd.to_datetime(df.index, format='%Y%m', errors='coerce') + MonthEnd(0)
            df = df.query("caldt >= '1963-07-31'")
            if self.end_date is not None:
                df = df.query("caldt <= '" + self.end_date + "'")
            df = df.reset_index(drop=True)
            df.name = file[:-4]
            dfs.append(df)
        return dfs
    
    def get_investment_data(self):
        return self._get_data(0)
    
    def get_49_industry_data(self):
        return self._get_data(1)
    
    def get_shortterm_reversal_data(self):
        return self._get_data(2)
    
    def get_cashflow_price_data(self):
        return self._get_data(3)
    
    def get_accrual_data(self):
        return self._get_data(4)
    
    def get_market_beta_data(self):
        return self._get_data(5)
    
    def get_net_share_issue_data(self):
        return self._get_data(6)
    
    def get_market_equity_data(self):
        return self._get_data(7)
    
    def get_size_data(self): # Really market equity
        return self._get_data(7)
    
    def get_longterm_reversal_data(self):
        return self._get_data(8)
    
    def get_10_industry_data(self):
        return self._get_data(9)
    
    def get_dividend_yield_data(self):
        return self._get_data(10)
    
    def get_operating_profitability_data(self):
        return self._get_data(11)
    
    def get_variance_data(self):
        return self._get_data(12)
    
    def get_momentum_data(self):
        return self._get_data(13)
    
    def get_book_to_market_data(self):
        return self._get_data(14)
    
    def get_earnings_price_data(self):
        return self._get_data(15)
    
    def get_excess_return_data(self):
        import getFamaFrenchFactors as gff
        exmt = gff.famaFrench5Factor(frequency='m')[['date_ff_factors', 'Mkt-RF']]
        exmt.columns = ['caldt', 'exmt']
        exmt['caldt'] = exmt['caldt'] + MonthEnd(0)
        exmt = exmt.query("caldt >= '1963-07-31'")
        if self.end_date is not None:
            exmt = exmt.query("caldt <= '" + self.end_date + "'")
        return exmt

    # Private method
    def _get_data(self, id):
        data = {}
        filename = ''
        dfs = self.get_all_data()
        for idx, df in enumerate(dfs):
            if idx == id:
                filename = df.name + '.CSV'
                data = self.dir[filename]
        df = pd.read_csv('data/' + filename, skiprows=data['skip'], nrows=data['lines'], index_col=0)
        df['caldt'] = pd.to_datetime(df.index, format='%Y%m', errors='coerce') + MonthEnd(0)
        df = df.query("caldt >= '1963-07-31'")
        if self.end_date is not None:
            df = df.query("caldt <= '" + self.end_date + "'")
        df = df.reset_index(drop=True)
        df.name = filename[:-4]
        return df