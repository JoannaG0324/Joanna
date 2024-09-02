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
SCORE_INDUSTRY_TABLE_NAME = config.TABLE_NAMES['SCORE_INDUSTRY']# 股票评分  
STOCK_YJBB_TABLE_NAME = config.TABLE_NAMES['STOCK_YJBB']# 业绩报表

# 创建数据库连接引擎
engine = create_engine(f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}/{DATABASE}")

# 更新股票基础信息表
def get_stock_info(engine):
    """
    Replace: 更新并覆盖股票基础信息表 
    
    """
    try:
        # 获取上海市场股票信息
        sh_stock_info = ak.stock_info_sh_name_code()
        sh_stock_info = sh_stock_info[['证券代码', '证券简称', '上市日期']]
        sh_stock_info.rename(columns={
            '证券代码': 'stock_code',
            '证券简称': 'stock_name',
            '上市日期': 'ipo_date'
        }, inplace=True)
        sh_stock_info['market'] = "sh"
        
        # 获取深圳市场股票信息
        sz_stock_info = ak.stock_info_sz_name_code()
        sz_stock_info = sz_stock_info[['A股代码', 'A股简称', 'A股上市日期']]
        sz_stock_info.rename(columns={
            'A股代码': 'stock_code',
            'A股简称': 'stock_name',
            'A股上市日期': 'ipo_date'
        }, inplace=True)
        sz_stock_info['market'] = "sz"
        
        # 合并上海和深圳市场的数据
        stock_info = pd.concat([sh_stock_info, sz_stock_info], ignore_index=True)
        
        # 覆盖写入到数据库中的股票基础信息表
        stock_info.to_sql(STOCK_INFO_TABLE_NAME, con=engine, if_exists='replace', index=False)
        return f"Update Success: '{STOCK_INFO_TABLE_NAME}'"

    except Exception as e:
        return f"更新股票基础信息时发生错误: {e}"

# 获取单只股票的历史数据：访问数据接口
def fetch_stock_history(full_stock_code, start_date, end_date):
    """
    获取单只股票的历史数据

    :param full_stock_code: str - 包含市场代码的完整股票代码
    :param start_date: str - 获取数据的开始日期，格式为'YYYYMMDD'
    :param end_date: str - 获取数据的结束日期，格式为'YYYYMMDD'
    :return: Tuple[pd.DataFrame, str] - 包含历史数据的DataFrame和操作结果的描述信息
    """
    try:
        stock_history = ak.stock_zh_a_daily(symbol=full_stock_code, start_date=start_date, end_date=end_date, adjust="qfq")
        if stock_history is not None and not stock_history.empty:
            stock_history['stock_code'] = full_stock_code  # 存入完整的股票代码（包含市场代码）
            return stock_history, f"历史数据获取成功: {full_stock_code}."
        else:
            return None, f"没有新的历史数据: {full_stock_code}"
    except Exception as e:
        return None, f"获取 {full_stock_code} 的历史数据时发生错误: {e}"    

# 更新所有股票的股价数据
def get_stock_history(engine, end_date):
    """
    Append: 更新所有股票的股价数据，并将新数据批量追加到股价数据表中

    :param engine: SQLAlchemy Engine - 数据库引擎。
    :param end_date: str - 获取数据的结束日期，格式为'YYYYMMDD'，默认为今天
    :return: str - 操作结果的描述信息。
    """
    if end_date is None:
        end_date = pd.Timestamp.today().strftime('%Y%m%d')
    
    try:
        # 获取数据库中所有的股票代码及市场信息
        stock_info_df = pd.read_sql(f"SELECT stock_code, market FROM {STOCK_INFO_TABLE_NAME}", con=engine)

        # 获取stock_daily表中的最新日期作为start_date
        latest_date_query = f"SELECT MAX(date) FROM {STOCK_DAILY_TABLE_NAME}"
        latest_date = pd.read_sql(latest_date_query, con=engine).iloc[0, 0]

        if latest_date is None:
            # 如果表中没有记录，默认从2024年1月1日开始
            start_date = '20240101'
        else:
            start_date = (latest_date + pd.Timedelta(days=1)).strftime('%Y%m%d')

        operation_messages = []  # 存储每只股票的处理结果信息

        # 使用tqdm显示进度条
        with ThreadPoolExecutor(max_workers=10) as executor:  # 设置线程池大小
            futures = []
            for index, row in stock_info_df.iterrows():
                stock_code = row['stock_code']
                market = row['market']
                full_stock_code = f"{market}{stock_code}"
                futures.append(executor.submit(fetch_stock_history, full_stock_code, start_date, end_date))

            for future in tqdm(futures, desc="Fetching and saving stock history"):
                stock_history, message = future.result()  # 获取数据和消息
                operation_messages.append(message)  # 保存操作消息
#                 print(message)
                if stock_history is not None and not stock_history.empty:
                    # 直接写入数据库
                    stock_history.to_sql(STOCK_DAILY_TABLE_NAME, con=engine, if_exists='append', index=False)

        return f"Update Success: '{STOCK_DAILY_TABLE_NAME}'"

    except Exception as e:
        return f"更新股票历史数据时发生错误: {e}"

# 计算价格：计算给定股价数据的 20 日均价和 14 日 ATR，并将结果写入数据库
def calculate_price(engine, ma_window=20, atr_window=14):
    """
    Replace: 从数据库读取股价数据，计算给定股价数据的 20 日均价和 14 日 ATR，并将结果写入数据库。

    参数:
    engine : SQLAlchemy Engine - 数据库引擎
    ma_window : int - 计算均价的窗口大小，默认为 20。
    atr_window : int - 计算 ATR 的窗口大小，默认为 14。
    
    返回:
    str - 操作结果的描述信息。
    """
    try:
        # 读取 `stock_daily` 表数据
        query = f"SELECT * FROM {STOCK_DAILY_TABLE_NAME}"
        stock_daily_df = pd.read_sql(query, engine)

        # 移除 `stock_code` 中的市场代码（如 'sh', 'sz' 等）
        stock_daily_df['stock_code'] = stock_daily_df['stock_code'].apply(lambda x: x[2:])

        # 按股票代码分组计算必要的列
        stock_daily_df['high_low'] = stock_daily_df.groupby('stock_code')['high'].transform(lambda x: x - stock_daily_df['low'])
        stock_daily_df['high_close'] = stock_daily_df.groupby('stock_code').apply(lambda x: (x['high'] - x['close'].shift()).abs()).reset_index(drop=True)
        stock_daily_df['low_close'] = stock_daily_df.groupby('stock_code').apply(lambda x: (x['low'] - x['close'].shift()).abs()).reset_index(drop=True)

        # 计算 TR
        stock_daily_df['TR'] = stock_daily_df[['high_low', 'high_close', 'low_close']].max(axis=1)

        # 按股票代码分组计算 20 日均价
        stock_daily_df['20_day_ma'] = stock_daily_df.groupby('stock_code')['close'].transform(lambda x: x.rolling(window=ma_window).mean())

        # 按股票代码分组计算 ATR
        stock_daily_df['ATR'] = stock_daily_df.groupby('stock_code')['TR'].transform(lambda x: x.rolling(window=atr_window).mean())

        # 计算 stock_score
        stock_daily_df['stock_score'] = ((stock_daily_df['close'] >= stock_daily_df['20_day_ma']) & stock_daily_df['20_day_ma'].notna()).astype(int)

        # 选择需要输出的列
        result_df = stock_daily_df[['date', 'stock_code', '20_day_ma', 'ATR', 'stock_score']]

        # 写入数据库
        result_df.to_sql(STOCK_PRICE_RESULTS_TABLE_NAME, con=engine, if_exists='replace', index=False)

        return f"Update Success: {STOCK_PRICE_RESULTS_TABLE_NAME}"

    except Exception as e:
        return f"Error in calculate_price: {e}"

# 重新计算行业得分：计算行业得分，并将结果写入数据库
def score_industry(com_ind_df, engine):
    """
    Replace: 计算行业得分

    :param com_ind_df: 包含个股与行业关联的数据框。
    :param engine: SQLAlchemy Engine - 数据库引擎。
    :return: str - 操作结果的描述信息。
    """
    try:
        # 从数据库中读取 calculate_price_df
        calculate_price_df = pd.read_sql(f"SELECT * FROM {STOCK_PRICE_RESULTS_TABLE_NAME}", engine)

        # 合并个股得分与行业数据
        merged_df = calculate_price_df.merge(
            com_ind_df[['stock_code', 'industry_level_1_name', 'industry_level_2_name']], 
            on='stock_code', 
            how='left'
        )

        # 按日期和二级行业计算得分
        industry_scores = (merged_df.groupby(['date', 'industry_level_1_name', 'industry_level_2_name'])
            .agg(industry_score=('stock_score', 'sum'),
                 industry_score_sum=('stock_score', 'count'))
            .reset_index()
        )

        # 计算行业得分比例
        industry_scores['industry_score_pct'] = industry_scores['industry_score'] / industry_scores['industry_score_sum'].replace(0, pd.NA)
        
        score_industry_df = industry_scores[['date', 'industry_level_1_name', 'industry_level_2_name', 'industry_score', 'industry_score_sum', 'industry_score_pct']]
        # 将结果写入数据库
        score_industry_df.to_sql(SCORE_INDUSTRY_TABLE_NAME, con=engine, if_exists='replace', index=False)

        return f"Update Success: '{SCORE_INDUSTRY_TABLE_NAME}'"

    except Exception as e:
        return f"Score industry calculation failed: {e}"

# 创建中证800与行业分类的关系并返回合并后的DataFrame    
def fetch_com_ind_relation(engine):
    """
    创建中证800与行业分类的关系并返回合并后的DataFrame。

    参数:
    engine : SQLAlchemy Engine - 数据库引擎

    返回:
    DataFrame - 包含中证800成分股及其行业分类的关系
    """
    # 查询中证800成分股的SQL语句
    query_components = """
    SELECT 
        stock_code,
        stock_name
    FROM 
        csindex_800_components;
    """

    # 查询个股关联行业的SQL语句
    query_industry = """
    SELECT 
        stock_code,
        industry_level_1_name,
        industry_level_2_name,
        industry_level_3_name,
        industry_level_4_name
    FROM 
        csindex_industry;
    """

    # 使用Pandas读取查询结果到DataFrame
    components_df = pd.read_sql(query_components, engine)
    industry_df = pd.read_sql(query_industry, engine)

    # 合并两个DataFrame
    com_ind_df = pd.merge(components_df, industry_df, on='stock_code', how='left')

    return com_ind_df

# 更新业绩报表数据
def update_stock_yjbb(date, engine):
    '''
    更新业绩报表数据
    输入：日期（格式：20240630）
    输出：更新结果
    '''
    # 列名映射
    column_mapping = {
        '序号': 'index',
        '股票代码': 'stock_code',
        '股票简称': 'stock_name',
        '每股收益': 'eps',
        '营业收入-营业收入': 'operating_revenue',
        '营业收入-同比增长': 'revenue_yoy_growth',
        '营业收入-季度环比增长': 'revenue_qoq_growth',
        '净利润-净利润': 'net_profit',
        '净利润-同比增长': 'net_profit_yoy_growth',
        '净利润-季度环比增长': 'net_profit_qoq_growth',
        '每股净资产': 'net_assets_per_share',
        '净资产收益率': 'roe',
        '每股经营现金流量': 'operating_cash_flow_per_share',
        '销售毛利率': 'gross_profit_margin',
        '所处行业': 'industry',
        '最新公告日期': 'latest_announcement_date'
    }

    try:
        # 获取新数据
        stock_yjbb_em_df = ak.stock_yjbb_em(date=date)
        print(f"已获取{date}的业绩报表数据")

        # 应用列名映射
        stock_yjbb_em_df.rename(columns=column_mapping, inplace=True)

        # 写入数据库
        stock_yjbb_em_df.to_sql('stock_yjbb', engine, if_exists='replace', index=False)
        print(f"{date}的数据已成功写入MySQL")

        return f"{date}的数据更新成功"
    except Exception as e:
        return f"更新数据时发生错误: {e}"

# 数据更新：更新所有股票的股价数据、计算价格、重新计算行业得分
def data_upate():
    
    messages = []
    
    # 1. 更新股票基础信息
    msg = get_stock_info(engine)
    messages.append(msg)

    # 2. 获取当前日期
    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    # 3. 更新所有股票的股价数据
    msg = get_stock_history(engine, end_date)
    messages.append(msg)

    # 4. 计算价格
    msg = calculate_price(engine)
    messages.append(msg)

    # # 5. 重新计算行业得分
    msg = score_industry(fetch_com_ind_relation(engine),engine)
    messages.append(msg)

    # 更新业绩报表数据
    # msg = update_stock_yjbb(config.YJBB_DATE, engine)
    # messages.append(msg)

    return messages



# if __name__ == "__main__":
#     messages = data_upate()
#     for msg in messages:
#         print(msg)