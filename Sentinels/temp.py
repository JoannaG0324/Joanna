import akshare as ak

stock_zh_a_daily_qfq_df = ak.stock_zh_a_daily(symbol="sz000001", start_date="19910403", adjust="qfq")
print(stock_zh_a_daily_qfq_df)

# import akshare as ak

# stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol="000001", period="daily", start_date="20170301", adjust="")
# print(stock_zh_a_hist_df)