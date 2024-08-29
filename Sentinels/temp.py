
import pandas as pd
from sqlalchemy import create_engine
import time
import akshare as ak
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm  # 导入进度显示模块
# 定义数据库连接信息
import utils.config as config  

USER = config.DB_CONFIG['USER']
PASSWORD = config.DB_CONFIG['PASSWORD']
HOST = config.DB_CONFIG['HOST']
DATABASE = config.DB_CONFIG['DATABASE']

# 从配置文件中获取数据表名称
STOCK_INFO_TABLE_NAME = config.TABLE_NAMES['STOCK_INFO']# 股票基础信息  
STOCK_DAILY_TABLE_NAME = config.TABLE_NAMES['STOCK_DAILY']# 股票日线数据
STOCK_PRICE_RESULTS_TABLE_NAME = config.TABLE_NAMES['STOCK_PRICE_RESULTS']# 股票计算结果
SCORE_INDUSTRY_TABLE_NAME = config.TABLE_NAMES['SCORE_INDUSTRY']
STOCK_YJBB_TABLE_NAME = config.TABLE_NAMES['STOCK_YJBB']

# 创建数据库连接引擎
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}")

stock_individual_spot_xq_df = ak.stock_individual_spot_xq(symbol="sh600000")

print(stock_individual_spot_xq_df)

# stock_a_indicator_lg_df = ak.stock_a_indicator_lg(symbol="000001")
# print(stock_a_indicator_lg_df)