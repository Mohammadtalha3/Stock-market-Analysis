
Daily_stock_fetch="""
            CREATE TABLE IF NOT EXISTS stock_prices_daily (
                stock_symbol TEXT,
                timestamp TIMESTAMPTZ PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT
            );
        """

Intra_day_stock_fetch=  """
            CREATE TABLE IF NOT EXISTS stock_prices_intraday (
                stock_symbol TEXT,
                timestamp TIMESTAMPTZ PRIMARY KEY,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                volume BIGINT
            );
        """
daily_hypertable= "SELECT create_hypertable('stock_prices_daily', 'timestamp', if_not_exists = TRUE);"
intraday_hypertable= "SELECT create_hypertable('stock_prices_intraday', 'timestamp', if_not_exists = TRUE);"