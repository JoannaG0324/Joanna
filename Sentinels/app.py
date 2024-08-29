from flask import Flask, render_template, request, jsonify
import pandas as pd
from sqlalchemy import create_engine
import math
import time
import numpy as np
from datetime import datetime
from flask import Response, stream_with_context
from StockUpdate import data_upate
import utils.config as config

# 定义数据库连接信息
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

# 初始化 Flask 应用
app = Flask(__name__)

@app.route('/')
def index():
    # 获取 use_level_2 参数
    use_level_2 = request.args.get('use_level_2', 'false').lower() == 'true'
    
    # 获取并处理数据
    score_industry_df = get_processed_score_industry_data(use_level_2)

    # 计算并设置 Index Score
    index_scores = score_industry_df.groupby('date').agg({
        'industry_score': 'sum',
        'industry_score_sum': 'sum'
    })
    index_scores['Index Score'] = index_scores['industry_score'] / index_scores['industry_score_sum']
    index_scores = index_scores[['Index Score']]
    index_scores.index = pd.to_datetime(index_scores.index)
        
    # 创建数据透视表，按日期降序
    industry_level_column = 'industry_level_2_name' if use_level_2 else 'industry_level_1_name'
    heatmap_data = score_industry_df.pivot_table(
        index='date', 
        columns=industry_level_column, 
        values=['industry_score_pct', 'industry_score'],
        fill_value=0
    )

    # 将索引转换为 datetime 类型，并按日期降序排列
    heatmap_data.index = pd.to_datetime(heatmap_data.index)
    heatmap_data = heatmap_data.sort_index(ascending=False)
        
    # 计算分页数据
    page_size = 60
    page = request.args.get('page', 1, type=int)
    total_rows = heatmap_data.shape[0]
    total_pages = (total_rows + page_size - 1) // page_size
    start = (page - 1) * page_size
    end = start + page_size
    paginated_data = heatmap_data.iloc[start:end]

    # 获取分页后的索引，并确保与 index_scores 一致
    paginated_index = paginated_data.index
    paginated_index_scores = index_scores.loc[paginated_index]

    return render_template('index.html', 
                           heatmap_data=paginated_data, 
                           index_scores=paginated_index_scores, 
                           total_pages=total_pages, 
                           current_page=page,
                           use_level_2=use_level_2)

# 获取处理后的行业数据
def get_processed_score_industry_data(use_level_2):
    '''
    获取处理后的行业数据: 
    1. 根据 use_level_2 参数选择查询的列
    2. 将查询结果转换为 DataFrame
    3. 将 DataFrame 转换为分组后的 DataFrame
    4. 返回分组后的 DataFrame
    '''
    # 根据 use_level_2 参数选择查询的列
    industry_level_column = 'industry_level_2_name' if use_level_2 else 'industry_level_1_name'
    
    query = f"SELECT date, {industry_level_column}, industry_score, industry_score_sum, industry_score_pct FROM {SCORE_INDUSTRY_TABLE_NAME}"
    df = pd.read_sql(query, engine)
    
    df['industry_score_pct'] = df['industry_score_pct'].fillna(0)
    df['date'] = pd.to_datetime(df['date'])
    
    df = df.groupby(['date', industry_level_column]).agg({
        'industry_score': 'sum',
        'industry_score_sum': 'sum',
        'industry_score_pct': 'mean'
    }).reset_index()
    
    return df

# 更新数据  
@app.route('/update_data')
def update_data():
    # 调用更新数据函数并获取消息列表
    messages = data_upate()

    # 返回所有消息
    return jsonify({'messages': messages})

# 股票价格表
@app.route('/stock_price_history', methods=['GET'])
def stock_price_history():
    '''
    股票价格
    '''
    # 获取筛选条件
    industry_level = request.args.get('industry_level')
    industry_value = request.args.get('industry_value')
#     industry_level_1 = request.args.get('industry_level_1')
    range_filter = request.args.get('index')
    stock_name = request.args.get('stock_name')

    # 获取最新日期
    latest_date_query = f"SELECT MAX(date) FROM {STOCK_PRICE_RESULTS_TABLE_NAME}"
    latest_date = pd.read_sql(latest_date_query, engine).iloc[0, 0]
    
        # 获取行业层级名称
    industry_levels_query = """
        SELECT DISTINCT industry_level_1_name, industry_level_2_name, industry_level_3_name, industry_level_4_name 
        FROM csindex_industry
    """
    industry_levels = pd.read_sql(industry_levels_query, engine)

    # 构建查询条件
    conditions = [f"sp.date = '{latest_date}'"]
    if industry_level and industry_value:
        conditions.append(f"ci.{industry_level} = '{industry_value}'")
#     if industry_level_1 and industry_level_1 != 'None':
#         conditions.append(f"ci.industry_level_1_name = '{industry_level_1}'")
    if range_filter == "csindex_800":
        conditions.append(f"sp.stock_code IN (SELECT stock_code FROM csindex_800_components)")
    if stock_name and stock_name != 'None':
        conditions.append(f"ci.stock_name LIKE '%%{stock_name}%%'")

    condition_str = ' AND '.join(conditions)

    # 查询所有数据（不分页）
    query = f"""
        SELECT sp.date, sp.stock_code, ci.stock_name, 
               ci.industry_level_1_name, ci.industry_level_2_name, 
               ci.industry_level_3_name, ci.industry_level_4_name, 
               sp.20_day_ma, sp.ATR, 
               sd.open, sd.close,
               COALESCE(NULLIF(sd.close, 0) / NULLIF(sd.open, 0), 1) - 1 AS price_change_percentage,
               sy.eps, sy.net_assets_per_share, sy.roe, 
               sy.operating_cash_flow_per_share, sy.gross_profit_margin, 
               sy.latest_announcement_date
        FROM {STOCK_PRICE_RESULTS_TABLE_NAME} sp
        JOIN csindex_industry ci ON sp.stock_code = ci.stock_code
        JOIN {STOCK_DAILY_TABLE_NAME} sd ON sp.stock_code = SUBSTR(sd.stock_code, 3) AND sp.date = sd.date
        LEFT JOIN {STOCK_YJBB_TABLE_NAME} sy ON sp.stock_code = sy.stock_code
        {f'WHERE {condition_str}' if conditions else ''}
    """
    # 查询所有数据
    all_stock_prices = pd.read_sql(query, engine)
    
    # 计算 PE 和 PB，添加错误处理
    all_stock_prices['pe'] = np.where(all_stock_prices['eps'] != 0, 
                                  all_stock_prices['close'] / all_stock_prices['eps'], 
                                  0)
    all_stock_prices['pb'] = np.where(all_stock_prices['net_assets_per_share'] != 0, 
                                  all_stock_prices['close'] / all_stock_prices['net_assets_per_share'], 
                                  0)
    
    # 格式化数值
    数值字段 = ['20_day_ma', 'ATR', 'open', 'close', 'price_change_percentage', 
             'eps', 'net_assets_per_share', 'roe', 'operating_cash_flow_per_share', 'gross_profit_margin',
             'pe', 'pb']
    

    for field in 数值字段:
        if field == 'price_change_percentage':
            all_stock_prices[field] = all_stock_prices[field].apply(lambda x: round(x, 4) * 100 if pd.notna(x) else 0)
        elif field in ['eps', 'net_assets_per_share', 'roe', 'operating_cash_flow_per_share', 'gross_profit_margin']:
            all_stock_prices[field] = all_stock_prices[field].apply(lambda x: round(float(x), 2) if pd.notna(x) and x != '' else '0')
        else:
            all_stock_prices[field] = all_stock_prices[field].apply(lambda x: round(x, 2) if pd.notna(x) else 0)

    # 将 DataFrame 转换为列表或字
    stock_prices_list = all_stock_prices.to_dict(orient='records')  # 确保数据可以序列化为 JSON

    # print(industry_levels)
    
    return render_template('stock_price_history.html', 
                           stock_prices=stock_prices_list,  # 传递可序列化的数据
                           industry_levels=industry_levels
                           )
# industry_level_1_names=industry_level_1_names['industry_level_1_name'].tolist()

# 获取股票历史数据
@app.route('/get_stock_history')
def get_stock_history():
    '''
    获取股票历史数据
    '''
    # 从请求中获取简化的股票代码（不含市场代码）
    simple_stock_code = request.args.get('stock_code')

    # 使用参数化查询来防止 SQL 注入
    query = f"""
        SELECT sd.date, sd.open, sd.high, sd.low, sd.close, sd.volume, sd.amount, 
               sd.outstanding_share, sd.turnover 
        FROM {STOCK_DAILY_TABLE_NAME} sd
        JOIN {STOCK_INFO_TABLE_NAME} si 
        ON sd.stock_code = CONCAT(si.market, si.stock_code)
        WHERE si.stock_code = %s
        ORDER BY sd.date
    """
    stock_history = pd.read_sql(query, engine, params=[simple_stock_code])
    

    if stock_history.empty:
        return jsonify({"error": "Stock history not found"}), 404
    
    # 将 `date` 列转换为 pandas 的 datetime 类型并格式化为 'yyyy-MM-dd'
    stock_history['date'] = pd.to_datetime(stock_history['date']).dt.strftime('%Y-%m-%d')

    result = stock_history.to_dict(orient='records')
#     print(result)

    return jsonify(result)


# 直接在主线程中运行 Flask 应用
if __name__ == '__main__':
#     app.run(port=5000)
    app.run(host='0.0.0.0', port=8000)
#     socketio.run(app)





