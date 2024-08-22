#!/usr/bin/env python
# coding: utf-8

# In[1]:


from flask import Flask, render_template, request, jsonify
import pandas as pd
from sqlalchemy import create_engine
import math
import time
import numpy as np
from datetime import datetime
from flask import Response, stream_with_context
from StockUpdate import data_upate
# from StockUpdate import get_stock_info,fetch_stock_history,get_stock_history,calculate_price,score_industry,fetch_com_ind_relation,data_upate
# from flask_socketio import SocketIO, emit

# 定义数据表名称
STOCK_INFO_TABLE_NAME = 'stock_info'
STOCK_DAILY_TABLE_NAME = 'stock_daily_qfq' # 前复权
STOCK_PRICE_RESULTS_TABLE_NAME = 'stock_price_results_qfq'
SCORE_INDUSTRY_TABLE_NAME = 'score_industry_qfq'

# 数据库连接配置
user = 'root'
password = ''  # 替换为实际密码
host = 'localhost'
database = 'stock_db'
connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}/{database}'
    
engine = create_engine(connection_string)


# 初始化 Flask 应用
app = Flask(__name__)

@app.route('/update_data')
def update_data():
    # 调用更新数据函数并获取消息列表
    messages = data_upate()

    # 返回所有消息
    return jsonify({'messages': messages})

@app.route('/')
def index():
    # 从数据库获取 score_industry 数据
    query = "SELECT date, industry_level_1_name, industry_score, industry_score_sum, industry_score_pct FROM score_industry_qfq"
    score_industry_df = pd.read_sql(query, engine)
    
    """ Data clean """
    # 处理数据，确保 NA 值被转换为 0
    score_industry_df['industry_score_pct'] = score_industry_df['industry_score_pct'].where(score_industry_df['industry_score_pct'].notna(), 0)

    # 将日期列转换为 datetime 类型
    score_industry_df['date'] = pd.to_datetime(score_industry_df['date'])
      
    # 使用 transform 确保结果映射到原数据框
    score_industry_df['total_industry_score'] = score_industry_df.groupby('date')['industry_score'].transform('sum')
    score_industry_df['total_industry_score_sum'] = score_industry_df.groupby('date')['industry_score_sum'].transform('sum')

    # 计算 Index Score
    score_industry_df['Index Score'] = score_industry_df['total_industry_score'] / score_industry_df['total_industry_score_sum']
    
    """ Data output """
    # 最新数据日期
    latest_date = score_industry_df['date'].max().strftime('%Y-%m-%d')

    """ index score """
    # 提取并设置 `Index Score`
    index_scores = score_industry_df[['date', 'Index Score']].drop_duplicates(subset=['date'])
    index_scores.set_index('date', inplace=True)

    # 确保索引为 datetime 类型
    index_scores.index = pd.to_datetime(index_scores.index)
        
    """ heat map """
    # 创建数据透视表，按日期降序
    heatmap_data = score_industry_df.pivot_table(
        index='date', 
        columns='industry_level_1_name', 
        values=['industry_score_pct', 'industry_score'],  # 去掉 'Index Score'
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

    # 确保正确传递 paginated_index_scores
    return render_template('index.html', heatmap_data=paginated_data, index_scores=paginated_index_scores, total_pages=total_pages, current_page=page)

@app.route('/stock_price_history', methods=['GET'])
def stock_price_history():
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
        conditions.append(f"ci.stock_name LIKE '%{stock_name}%'")

    condition_str = ' AND '.join(conditions)

    # 查询所有数据（不分页）
    query = f"""
        SELECT sp.date, sp.stock_code, ci.stock_name, 
               ci.industry_level_1_name, ci.industry_level_2_name, 
               ci.industry_level_3_name, ci.industry_level_4_name, 
               sp.20_day_ma, sp.ATR, 
               sd.open, sd.close,
               COALESCE(NULLIF(sd.close, 0) / NULLIF(sd.open, 0), 1) - 1 AS price_change_percentage
        FROM {STOCK_PRICE_RESULTS_TABLE_NAME} sp
        JOIN csindex_industry ci ON sp.stock_code = ci.stock_code
        JOIN {STOCK_DAILY_TABLE_NAME} sd 
        ON sp.stock_code = SUBSTR(sd.stock_code, 3) AND sp.date = sd.date
        {f'WHERE {condition_str}' if conditions else ''}
    """

    all_stock_prices = pd.read_sql(query, engine)

    # 格式化数值
    all_stock_prices['20_day_ma'] = all_stock_prices['20_day_ma'].apply(lambda x: round(x, 2) if pd.notna(x) else 0)
    all_stock_prices['ATR'] = all_stock_prices['ATR'].apply(lambda x: round(x, 2) if pd.notna(x) else 0)
    all_stock_prices['open'] = all_stock_prices['open'].apply(lambda x: round(x, 2) if pd.notna(x) else 0)
    all_stock_prices['close'] = all_stock_prices['close'].apply(lambda x: round(x, 2) if pd.notna(x) else 0)
    all_stock_prices['price_change_percentage'] = all_stock_prices['price_change_percentage'].apply(lambda x: round(x, 4) * 100 if pd.notna(x) else 0)

    # 获取 industry_level_1_names
    industry_level_1_query = f"SELECT DISTINCT industry_level_1_name FROM {SCORE_INDUSTRY_TABLE_NAME} ORDER BY industry_level_1_name"
    industry_level_1_names = pd.read_sql(industry_level_1_query, engine)

    # 将 DataFrame 转换为列表或字典
    stock_prices_list = all_stock_prices.to_dict(orient='records')  # 确保数据可以序列化为 JSON

    print(industry_levels)
    
    return render_template('stock_price_history.html', 
                           stock_prices=stock_prices_list,  # 传递可序列化的数据
                           industry_levels=industry_levels
                           )
# industry_level_1_names=industry_level_1_names['industry_level_1_name'].tolist()


@app.route('/get_stock_history')
def get_stock_history():
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
    
#     print("Executing query:", query)
#     print("With parameter:", simple_stock_code)
#     print(stock_history)
    if stock_history.empty:
        return jsonify({"error": "Stock history not found"}), 404
    
    # 将 `date` 列转换为 pandas 的 datetime 类型并格式化为 'yyyy-MM-dd'
    stock_history['date'] = pd.to_datetime(stock_history['date']).dt.strftime('%Y-%m-%d')

    result = stock_history.to_dict(orient='records')
#     print(result)

    return jsonify(result)


# In[ ]:


# 直接在主线程中运行 Flask 应用
if __name__ == '__main__':
#     app.run(port=5000)
    app.run(host='0.0.0.0', port=8000)
#     socketio.run(app)


# In[ ]:





# In[ ]:




