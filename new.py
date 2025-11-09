# -*- coding: utf-8 -*-
"""
完整版：股票复盘与批次趋势追踪（Streamlit 应用）
- 支持多个 Excel 文件（多批次）上传
- 自动清洗 / Arrow 友好化处理
- 连续上涨判断：strict / ma_above（5日均线上）
- 保存每日结果到本地历史 stock_trend_history.csv
- 批次市场热度追踪（batch_trend.csv）
- 个股多日轨迹可视化（斜率 vs 连续上涨）
- 界面交互：侧边栏参数、阈值、选择股票等
- 所有主要分析模块支持折叠/展开
"""

import os
import logging
import datetime
from collections import Counter
import tempfile

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import streamlit as st

# ========== 基础配置 ==========
# 修复中文显示问题
try:
    # 尝试多种中文字体
    matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans', 'Arial Unicode MS', 'SimSun']
    matplotlib.rcParams['axes.unicode_minus'] = False
except:
    pass

# 配置日志记录
logging.basicConfig(filename="analysis_debug.log",
                    level=logging.DEBUG,
                    format="%(asctime)s [%(levelname)s] %(message)s")

# Streamlit 页面配置
st.set_page_config(layout="wide", page_title="股票复盘与批次追踪")
st.title("📈 股票复盘与批次追踪（整合版）")

# ========== 可调整参数 ==========

# 列名配置（若你的 Excel 表列名不同，可在这里修改）
code_col = "股票代码"
name_col = "股票简称"

# 侧边栏参数配置
st.sidebar.header("分析参数")
# 连续上涨判断模式选择
up_trend_mode = st.sidebar.selectbox(
    "连续上涨判断模式",
    ["strict", "ma_above"],
    format_func=lambda x: {
        "strict": "🔴 严格连续上涨：每日收盘价必须高于前一日",
        "ma_above": "🟢 宽松连续上涨：收盘价位于均线之上"
    }[x]
)

# 斜率阈值滑块
slope_threshold = st.sidebar.slider("最小斜率阈值(%)", 0.1, 5.0, 1.0, step=0.1)
# 收盘价天数输入
close_days = st.sidebar.number_input("收盘价天数 (用于连续判断)", value=5, min_value=2)
# 表头行数配置（用于复杂表格）
header_rows = st.sidebar.number_input("表头行数 (复杂表格用)", value=1, min_value=1)
# 跳过Excel前几行说明文字
skip_rows = st.sidebar.number_input("跳过Excel前几行说明文字", value=0, min_value=0)
# 概念列名配置
concept_col_name = st.sidebar.text_input("概念列名（可选）", value="所属概念")

# 文件路径配置
HISTORY_FILE = "stock_trend_history.csv"
LAST_BATCH_FILE = "last_batch.csv"
BATCH_TREND_FILE = "batch_trend.csv"

# ========== 工具函数 ==========

def make_arrow_safe(df: pd.DataFrame) -> pd.DataFrame:
    """修正 DataFrame 以防止 Streamlit / Arrow 报错、并做基本清洗"""
    df = df.copy()
    # 替换各种空值表示
    df.replace(['-', '--', '—', '空值', 'null', 'None', '', 'NaN', 'nan', '无'], np.nan, inplace=True)
    # 处理文本列
    for c in df.select_dtypes(include=['object']).columns:
        try:
            df[c] = df[c].astype(str).str.strip().replace({'nan': np.nan, 'None': np.nan})
        except Exception:
            logging.debug(f"make_arrow_safe strip failed for {c}", exc_info=True)
    # 数值列转换
    numeric_hint = ['%', '斜率', '占比', '涨', '跌', '价', '均线', 'close', 'price']
    for col in df.columns:
        try:
            if any(k in str(col) for k in numeric_hint):
                df[col] = pd.to_numeric(df[col], errors='coerce')
        except Exception:
            logging.debug(f"make_arrow_safe to_numeric failed for {col}", exc_info=True)
    # 最终清理
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).replace({'nan': np.nan})
    return df

def check_strict_continuous_up(closes, close_days):
    """严格连续上涨判断：每日收盘价必须高于前一日"""
    if len(closes) < close_days:
        return False, f"数据不足: {len(closes)}/{close_days}"
    if any(price <= 0 for price in closes):
        return False, "存在无效价格(<=0)"
    # 检查是否连续上涨
    is_up = all(closes[i] > closes[i-1] for i in range(1, len(closes)))
    # 生成详细判断信息
    details = [f"第{i+1}日:{closes[i]:.2f} > 第{i}日:{closes[i-1]:.2f} = {closes[i] > closes[i-1]}" for i in range(1, len(closes))]
    return is_up, '\n'.join(details)

def check_ma_above_continuous_up(closes, ma_values, close_days):
    """宽松连续上涨判断：收盘价位于均线之上"""
    if len(closes) < close_days or len(ma_values) < close_days:
        return False, f"数据不足: 收盘{len(closes)}/均线{len(ma_values)}/{close_days}"
    if any(price <= 0 for price in closes):
        return False, "存在无效价格(<=0)"
    # 检查是否都在均线之上
    is_above_ma = all(closes[i] >= ma_values[i] for i in range(len(closes)))
    # 生成详细判断信息
    details = [f"第{i+1}日: 收盘{closes[i]:.2f} ≥ 均线{ma_values[i]:.2f} = {closes[i] >= ma_values[i]}" for i in range(len(closes))]
    return is_above_ma, '\n'.join(details)

def safe_calculate_price_changes(closes):
    """安全计算价格变化百分比"""
    price_changes = []
    for i in range(1, len(closes)):
        if closes[i-1] > 0:
            change = (closes[i] - closes[i-1]) / closes[i-1] * 100
            price_changes.append(change)
        else:
            price_changes.append(0)
    return price_changes

def append_history_batch(result_df, history_file=HISTORY_FILE):
    """将当前批次结果追加到历史文件"""
    df_to_save = result_df.copy()
    df_to_save['日期'] = df_to_save['日期'].astype(str)
    if os.path.exists(history_file):
        try:
            existing = pd.read_csv(history_file, dtype=str)
        except Exception:
            existing = pd.read_csv(history_file, dtype=str, encoding='utf-8')
        # 合并并去重
        combined = pd.concat([existing, df_to_save], ignore_index=True)
        combined = combined.drop_duplicates(subset=['日期','股票代码'], keep='last')
        combined.to_csv(history_file, index=False, encoding='utf-8-sig')
        history_df = combined
    else:
        df_to_save.to_csv(history_file, index=False, encoding='utf-8-sig')
        history_df = df_to_save
    # 尝试转换斜率列为数值类型
    try:
        history_df['斜率(%)'] = pd.to_numeric(history_df['斜率(%)'], errors='coerce')
    except Exception:
        pass
    return history_df

def load_history(history_file=HISTORY_FILE):
    """加载历史数据"""
    if os.path.exists(history_file):
        try:
            h = pd.read_csv(history_file, parse_dates=['日期'], infer_datetime_format=True)
            return h
        except Exception:
            try:
                h = pd.read_csv(history_file, dtype=str)
                if '日期' in h.columns:
                    h['日期'] = pd.to_datetime(h['日期'], errors='coerce')
                return h
            except Exception:
                return pd.DataFrame()
    else:
        return pd.DataFrame()

def build_stock_data_map_from_df(df):
    """从DataFrame构建股票数据映射"""
    close_cols, ma_cols = [], []
    # 识别收盘价和均线列
    for c in df.columns[2:]:
        col_lower = str(c).lower()
        if "收盘价" in col_lower or "close" in col_lower:
            close_cols.append(c)
        elif "均线" in col_lower or "ma" in col_lower:
            ma_cols.append(c)
    
    stock_data_map = {}
    for idx, row in df.iterrows():
        try:
            code = str(row[df.columns[0]])
            name = str(row[df.columns[1]])
        except Exception:
            continue
        
        # 提取收盘价数据
        closes = []
        for c in close_cols:
            val = row.get(c, np.nan)
            if pd.notna(val):
                val_str = str(val).replace(',', '').replace('—', '').replace('--', '').strip()
                if val_str in ["", "NaN", "None", "null"]:
                    continue
                try:
                    price = float(val_str)
                    if price > 0:
                        closes.append(price)
                except:
                    continue
        closes = closes[::-1]  # 反转顺序（从旧到新）
        closes = np.array(closes, dtype=float)
        
        # 提取均线数据
        ma_values = []
        if ma_cols:
            for c in ma_cols:
                val = row.get(c, np.nan)
                if pd.notna(val):
                    val_str = str(val).replace(',', '').replace('—', '').replace('--', '').strip()
                    if val_str in ["", "NaN", "None", "null"]:
                        continue
                    try:
                        ma = float(val_str)
                        if ma > 0:
                            ma_values.append(ma)
                    except:
                        continue
            ma_values = ma_values[::-1]
            ma_values = np.array(ma_values, dtype=float)
        else:
            # 如果没有均线数据，计算局部平均值
            ma_days = min(5, len(closes))
            if len(closes) > 0:
                ma_values = np.array([np.mean(closes[max(0, i-ma_days+1):i+1]) for i in range(len(closes))])
            else:
                ma_values = np.array([])
        
        stock_data_map[code] = {'name': name, 'closes': closes.copy(), 'ma_values': ma_values.copy()}
    return stock_data_map

def generate_ths_link(stock_code):
    """生成同花顺操作指南"""
    # 判断市场类型
    if stock_code.startswith('6'):
        market_prefix = 'SH'
    else:
        market_prefix = 'SZ'
    
    # 返回操作指南，不再返回网页链接
    return f"在同花顺中输入: {stock_code} 然后按回车查看K线"

def get_chinese_font():
    """获取中文字体路径 - 修复中文显示问题"""
    # 尝试多种中文字体
    font_candidates = [
        # Windows 字体
        'C:/Windows/Fonts/simhei.ttf',  # 黑体
        'C:/Windows/Fonts/simsun.ttc',  # 宋体
        'C:/Windows/Fonts/msyh.ttc',    # 微软雅黑
        'C:/Windows/Fonts/simkai.ttf',  # 楷体
        
        # macOS 字体
        '/System/Library/Fonts/PingFang.ttc',
        '/Library/Fonts/Arial Unicode.ttf',
        '/System/Library/Fonts/STHeiti Light.ttc',
        
        # Linux 字体
        '/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf',
        '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',
        
        # 常见字体名称（通过matplotlib查找）
        'SimHei', 'Microsoft YaHei', 'SimSun', 'KaiTi', 'FangSong',
        'Arial Unicode MS', 'DejaVu Sans'
    ]
    
    for font_path in font_candidates:
        if os.path.exists(font_path):
            return font_path
        
        # 尝试通过字体名称查找
        try:
            import matplotlib.font_manager as fm
            if font_path in fm.findfont(fm.FontProperties(family=font_path)):
                return font_path
        except:
            continue
    
    # 如果都没找到，返回None，使用默认字体
    return None
	
	
def extract_date_from_column_name(column_name):
    """从列名中提取日期字符串"""
    import re
    
    # 常见的日期模式
    date_patterns = [
        r'(\d{4}\.\d{1,2}\.\d{1,2})',  # 2023.01.15
        r'(\d{4}-\d{1,2}-\d{1,2})',    # 2023-01-15
        r'(\d{4}/\d{1,2}/\d{1,2})',    # 2023/01/15
        r'(\d{8})',                     # 20230115
        r'(\d{4}年\d{1,2}月\d{1,2}日)' # 2023年1月15日
    ]
    
    col_str = str(column_name)
    
    for pattern in date_patterns:
        match = re.search(pattern, col_str)
        if match:
            return match.group(1)
    
    return None

def parse_date(date_str):
    """解析日期字符串为日期对象"""
    from datetime import datetime
    
    date_formats = [
        '%Y.%m.%d',
        '%Y-%m-%d', 
        '%Y/%m/%d',
        '%Y%m%d',
        '%Y年%m月%d日'
    ]
    
    for fmt in date_formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except:
            continue
    
    return None

	

# ========== 主流程（上传与处理） ==========
# 文件上传组件
uploaded_files = st.file_uploader("上传问财Excel文件（支持多个文件，多天数据；按时间顺序上传或文件名含日期）", type=["xlsx", "xls"], accept_multiple_files=True)

if not uploaded_files:
    st.info("请上传至少一个 Excel 文件（可以多个批次）。")
    st.stop()

# 存放结果的数据结构
all_results = []  # 所有结果数据
all_batch_dates = []  # 所有批次日期
stock_trends = {}   # {code: [(date, passed_bool), ...]} 股票趋势记录
stock_concepts = {}  # 股票概念映射
daily_dfs = {}  # 每日数据框存储

# 逐文件读取与处理
for uploaded_file in uploaded_files:
    try:
        # 根据表头行数读取Excel
        if header_rows == 1:
            df = pd.read_excel(uploaded_file, header=0, skiprows=skip_rows)
            df.columns = [str(c).strip() for c in df.columns]
        else:
            # 处理多行表头
            df_raw = pd.read_excel(uploaded_file, header=None)
            header_df = df_raw.iloc[:header_rows].ffill(axis=1)
            df = df_raw.iloc[header_rows + skip_rows:].reset_index(drop=True)
            # 构建合并列名
            columns = []
            current_prefix = ""
            for col in header_df.values.T:
                col_strs = [str(x).strip() for x in col if str(x) != "nan"]
                if len(col_strs) == 0:
                    columns.append("")
                    continue
                if "收盘价" in col_strs[0]:
                    current_prefix = "收盘价"
                elif "5日均线" in col_strs[0] or "均线" in col_strs[0]:
                    current_prefix = "5日均线"
                date_part = col_strs[-1] if len(col_strs) > 1 else col_strs[0]
                if current_prefix and "undefined" in col_strs[0]:
                    merged = f"{current_prefix}_{date_part}"
                else:
                    merged = "_".join(col_strs).strip("_")
                columns.append(merged)
            df.columns = columns
    except Exception as e:
        st.error(f"读取文件 {uploaded_file.name} 失败: {e}")
        logging.exception(f"读取文件失败 {uploaded_file.name}: {e}")
        continue

    # 基础数据清洗
    try:
        for col in df.select_dtypes(include=['object']).columns:
            try:
                df[col] = df[col].astype(str).str.strip().replace({'nan': np.nan, 'None': np.nan})
            except Exception:
                logging.debug(f"strip failed for column {col}", exc_info=True)
        # 替换各种空值符号
        replace_symbols = ["-", "—", "空值", "null", "None", "", "NaN", "--"]
        df.replace(replace_symbols, np.nan, inplace=True)
        # 处理数值列
        for col in df.columns:
            if df[col].dtype == object:
                try:
                    df[col] = df[col].astype(str).str.replace(',', '').str.replace(' ', '')
                except Exception:
                    pass
                try:
                    df[col] = pd.to_numeric(df[col], errors='ignore')
                except Exception:
                    pass
        # 特定数值列处理
        for numeric_col in ["现价(元)", "斜率(%)", "平均斜率"]:
            if numeric_col in df.columns:
                df[numeric_col] = pd.to_numeric(df[numeric_col], errors="coerce")
    except Exception as e:
        logging.exception(f"数据清洗阶段异常: {e}")

    # Arrow安全化处理
    try:
        df = make_arrow_safe(df)
    except Exception as e:
        logging.exception(f"make_arrow_safe failed: {e}")

    # 识别收盘价列 & 均线列
    close_cols, ma_cols = [], []
    for c in df.columns[2:]:
        col_lower = str(c).lower()
        if "收盘价" in col_lower or "close" in col_lower:
            close_cols.append(c)
        elif "均线" in col_lower or "ma" in col_lower:
            ma_cols.append(c)

    # 从收盘价列名提取日期（取最大日期作为批次日期）
    dates = []
    for c in close_cols:
        parts = str(c).split('_')
        if len(parts) > 1:
            date_str_raw = parts[-1]
            date_str = date_str_raw.split(' [')[0].strip()
            # 尝试多种日期格式
            for fmt in ("%Y.%m.%d", "%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"):
                try:
                    date_obj = datetime.datetime.strptime(date_str, fmt).date()
                    dates.append(date_obj)
                    break
                except Exception:
                    continue
    if dates:
        batch_date = max(dates).strftime("%Y-%m-%d")
    else:
        # 无法提取日期时使用当前日期
        batch_date = datetime.date.today().strftime("%Y-%m-%d")
        st.warning(f"无法从列名中提取日期（文件: {uploaded_file.name}），使用当前系统日期。")
    all_batch_dates.append(batch_date)
    daily_dfs[batch_date] = df  # 存储每日数据

    # 遍历每只股票进行分析
    results = []
    for idx, row in df.iterrows():
        try:
            code = str(row[df.columns[0]]).strip()
            name = str(row[df.columns[1]]).strip()
        except Exception:
            continue

        # 提取概念信息 - 修复双表头问题
        concept = "未知"
        if concept_col_name in df.columns:
            concept_val = row.get(concept_col_name, "未知")
            if pd.notna(concept_val):
                concept = str(concept_val).strip()
        else:
            # 尝试查找包含"概念"关键词的列
            concept_cols = [col for col in df.columns if "概念" in str(col)]
            if concept_cols:
                concept_val = row.get(concept_cols[0], "未知")
                if pd.notna(concept_val):
                    concept = str(concept_val).strip()
        
        if code not in stock_concepts:
            stock_concepts[code] = concept

        # 提取收盘价序列
        closes = []
        for c in close_cols:
            val = row.get(c, np.nan)
            if pd.notna(val):
                val_str = str(val).replace(',', '').replace('—', '').replace('--', '').strip()
                if val_str in ["", "NaN", "None", "null"]:
                    continue
                try:
                    price = float(val_str)
                    if price > 0:
                        closes.append(price)
                except:
                    continue
        closes = closes[::-1]  # 反转顺序（从旧到新）
        closes = np.array(closes, dtype=float)

        # 提取均线序列
        ma_values = []
        if ma_cols:
            for c in ma_cols:
                val = row.get(c, np.nan)
                if pd.notna(val):
                    val_str = str(val).replace(',', '').replace('—', '').replace('--', '').strip()
                    if val_str in ["", "NaN", "None", "null"]:
                        continue
                    try:
                        ma = float(val_str)
                        if ma > 0:
                            ma_values.append(ma)
                    except:
                        continue
            ma_values = ma_values[::-1]
            ma_values = np.array(ma_values, dtype=float)
        else:
            # 无均线数据时计算局部均值
            ma_days = min(5, len(closes))
            ma_values = np.array([np.mean(closes[max(0, i-ma_days+1):i+1]) for i in range(len(closes))]) if len(closes)>0 else np.array([])

        # 判断最近 close_days 天的趋势
        if len(closes) < close_days or len(ma_values) < close_days:
            closes_for_check = closes
            ma_for_check = ma_values
            is_up = False
            slope_perc = np.nan
            up_details = f"数据不足: {len(closes)}/{close_days}"
        else:
            closes_for_check = closes[-close_days:]  # 取最近close_days天
            ma_for_check = ma_values[-close_days:]
            # 根据模式判断是否连续上涨
            if up_trend_mode == "strict":
                is_up, up_details = check_strict_continuous_up(closes_for_check, close_days)
            else:
                is_up, up_details = check_ma_above_continuous_up(closes_for_check, ma_for_check, close_days)
            # 计算斜率
            x = np.arange(len(closes_for_check))
            try:
                slope, _ = np.polyfit(x, closes_for_check, 1)
                slope_perc = slope / closes_for_check.mean() * 100
            except Exception as e:
                slope_perc = np.nan
                logging.debug(f"计算斜率失败，{code}: {e}")

        # 构建不符合原因
        reason = []
        if not is_up:
            reason.append(up_details if isinstance(up_details, str) else f"未连续上涨({close_days}天)")
        if not np.isnan(slope_perc) and slope_perc < slope_threshold:
            reason.append(f"斜率过小({slope_perc:.2f}%)")
        passed = len(reason) == 0  # 是否通过所有条件

        # 记录结果
        results.append({
            "日期": batch_date,
            "股票代码": code,
            "股票简称": name,
            "判断模式": "严格连续上涨" if up_trend_mode == "strict" else "5日均线上",
            "连续上涨": "✅ 是" if is_up else "❌ 否",
            "斜率(%)": round(slope_perc, 3) if not np.isnan(slope_perc) else np.nan,
            "是否符合": "✅ 是" if passed else "❌ 否",
            "不符合原因": " | ".join(reason) if reason else "-"
        })

        # 记录股票趋势（用于多天分析）
        if code not in stock_trends:
            stock_trends[code] = []
        stock_trends[code].append((batch_date, passed))

    all_results.extend(results)

# 排序批次日期
all_batch_dates = sorted(set(all_batch_dates))



# ========== 共同出现股票详细分析（可折叠） ==========
with st.expander("🔄 共同出现股票详细分析", expanded=True):
    if len(all_batch_dates) > 1:
        # 构建出现情况的pivot表
        appear_df = pd.DataFrame(all_results)
        appear_df['股票代码'] = appear_df['股票代码'].astype(str).str.strip().str.upper()
        appear_df['日期'] = pd.to_datetime(appear_df['日期'], errors='coerce').dt.strftime('%Y-%m-%d')
        appear_pivot = appear_df.pivot_table(index='股票代码', columns='日期', values='是否符合', aggfunc='size')
        appear_pivot = appear_pivot.reindex(columns=all_batch_dates)

        # 判断：两个文件都出现（无论是否符合）
        common_mask = appear_pivot.notna().all(axis=1)
        common_stocks = appear_pivot[common_mask].index.tolist()

        if len(common_stocks) == 0:
            st.info("两个文件中没有共同出现的股票。")
        else:
            st.success(f"**共同出现：{len(common_stocks)} 只股票**（两个文件都有）")

            # 构建股票详细信息映射
            stock_info_map = {}
            stock_slope_map = {}
            
            # 获取最新批次的斜率数据
            latest_date = max(all_batch_dates) if all_batch_dates else None
            if latest_date:
                latest_results = [r for r in all_results if r['日期'] == latest_date]
                for result in latest_results:
                    code = result['股票代码']
                    stock_slope_map[code] = result['斜率(%)']
            
            # 构建完整的股票信息映射
            for result in all_results:
                code = result['股票代码']
                if code not in stock_info_map:
                    stock_info_map[code] = {
                        'name': result['股票简称'],
                        'concept': stock_concepts.get(code, '未知'),
                        'slope': stock_slope_map.get(code, np.nan)
                    }

            # 创建详细的共同股票信息表格
            common_stocks_details = []
            
            for code in common_stocks:
                info = stock_info_map.get(code, {})
                common_stocks_details.append({
                    '股票代码': code,
                    '股票简称': info.get('name', '未知'),
                    '所属概念': info.get('concept', '未知'),
                    '斜率(%)': info.get('slope', np.nan)
                })
            
            # 创建DataFrame并排序（按斜率降序）
            common_df = pd.DataFrame(common_stocks_details)
            if not common_df.empty and '斜率(%)' in common_df.columns:
                common_df = common_df.sort_values('斜率(%)', ascending=False)
            
            # 显示详细的共同股票表格
            st.dataframe(
                common_df.style.format({'斜率(%)': '{:.3f}'}),
                use_container_width=True
            )
            
            # 提供下载功能
            csv = common_df.to_csv(index=False, encoding="utf-8-sig")
            st.download_button(
                "下载共同股票详细信息 CSV",
                data=csv,
                file_name=f"共同股票详细信息_{pd.Timestamp('today').strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )
            
            # ========== 修正：共同股票跨文件时间跨度折线图（自动显示所有股票） ==========
            st.markdown("---")
            st.subheader("📊 共同股票跨文件时间跨度走势图")
            
            # 自动显示所有共同股票，不再使用下拉框
            for i, selected_stock in enumerate(common_stocks):
                st.markdown(f"---")
                stock_name = stock_info_map.get(selected_stock, {}).get('name', '未知')
                st.markdown(f"### {i+1}. {selected_stock} - {stock_name}")
                
                # 收集所有价格数据点（日期和收盘价）
                all_price_data = []  # 存储 (date, price, batch_date) 元组
                
                # 对每个批次日期，从对应的daily_dfs中提取该股票的价格数据
                for batch_date in sorted(all_batch_dates):
                    df_batch = daily_dfs.get(batch_date)
                    if df_batch is not None:
                        # 找到该股票在批次数据中的行
                        stock_row = None
                        for idx, row in df_batch.iterrows():
                            if str(row[df_batch.columns[0]]).strip() == selected_stock:
                                stock_row = row
                                break
                        
                        if stock_row is not None:
                            # 提取收盘价列
                            close_cols = [c for c in df_batch.columns if "收盘价" in str(c)]
                            if close_cols:
                                # 按时间顺序处理每个收盘价列
                                for c in close_cols:
                                    val = stock_row.get(c, np.nan)
                                    if pd.notna(val):
                                        val_str = str(val).replace(',', '').replace('—', '').replace('--', '').strip()
                                        if val_str not in ["", "NaN", "None", "null"]:
                                            try:
                                                price = float(val_str)
                                                if price > 0:
                                                    # 从列名提取日期
                                                    col_name = str(c)
                                                    date_str = extract_date_from_column_name(col_name)
                                                    if date_str:
                                                        # 转换为日期对象
                                                        date_obj = parse_date(date_str)
                                                        if date_obj:
                                                            # 检查是否为交易日（周一至周五）
                                                            if date_obj.weekday() < 5:  # 0-4 表示周一到周五
                                                                all_price_data.append({
                                                                    'date': date_obj,
                                                                    'price': price,
                                                                    'batch': batch_date,
                                                                    'column_name': col_name
                                                                })
                                            except:
                                                continue
                
                # 按日期排序并去重
                if all_price_data:
                    # 按日期排序
                    all_price_data.sort(key=lambda x: x['date'])
                    
                    # 去重：同一天只保留一个价格（取最后一个）
                    unique_dates = {}
                    for item in all_price_data:
                        date_key = item['date'].strftime('%Y-%m-%d')
                        unique_dates[date_key] = item
                    
                    all_price_data = list(unique_dates.values())
                    all_price_data.sort(key=lambda x: x['date'])
                    
                    # 准备绘图数据
                    dates = [item['date'] for item in all_price_data]
                    prices = [item['price'] for item in all_price_data]
                    batches = [item['batch'] for item in all_price_data]
                    
                    # 创建折线图
                    fig, ax = plt.subplots(figsize=(12, 6))
                    
                    # 设置中文字体
                    chinese_font = get_chinese_font()
                    if chinese_font:
                        plt.rcParams['font.sans-serif'] = [chinese_font] + plt.rcParams['font.sans-serif']
                        plt.rcParams['axes.unicode_minus'] = False
                    
                    # 绘制主折线
                    ax.plot(dates, prices, marker='o', linewidth=2, color='blue', markersize=6)
                    
                    # 用不同颜色标记不同批次的数据点
                    unique_batches = list(set(batches))
                    colors = ['red', 'green', 'orange', 'purple', 'brown']
                    batch_colors = {}
                    
                    for i, batch in enumerate(unique_batches):
                        batch_colors[batch] = colors[i % len(colors)]
                    
                    # 标记不同批次的数据点
                    for i, (date, price, batch) in enumerate(zip(dates, prices, batches)):
                        color = batch_colors[batch]
                        # 只在第一次出现该批次时添加图例
                        label = batch if batch not in [batches[j] for j in range(i)] else ""
                        ax.scatter(date, price, color=color, s=80, zorder=5, label=label)
                    
                    # 添加价格标签（每隔几个点显示一次，避免太拥挤）
                    n = max(1, len(dates) // 8)  # 每8个点左右显示一个标签
                    for i, (date, price) in enumerate(zip(dates, prices)):
                        if i % n == 0 or i == len(dates) - 1:
                            ax.annotate(f'{price:.2f}', 
                                      (date, price),
                                      textcoords="offset points",
                                      xytext=(0, 10),
                                      ha='center',
                                      fontsize=8,
                                      bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.7))
                    
                    # 图表美化
                    ax.set_title(f'{selected_stock} {stock_name} - 价格走势图（仅显示交易日）', 
                               fontsize=14, fontweight='bold')
                    ax.set_xlabel('日期', fontsize=10)
                    ax.set_ylabel('收盘价 (元)', fontsize=10)
                    
                    # 设置X轴日期格式
                    ax.xaxis.set_major_formatter(plt.matplotlib.dates.DateFormatter('%Y-%m-%d'))
                    # 根据数据点数量调整刻度间隔
                    if len(dates) > 10:
                        interval = max(1, len(dates) // 10)
                        ax.xaxis.set_major_locator(plt.matplotlib.dates.DayLocator(interval=interval))
                    else:
                        ax.xaxis.set_major_locator(plt.matplotlib.dates.DayLocator(interval=1))
                    
                    plt.xticks(rotation=45)
                    
                    # 添加图例
                    handles, labels = ax.get_legend_handles_labels()
                    by_label = dict(zip(labels, handles))  # 去重
                    if by_label:
                        ax.legend(by_label.values(), by_label.keys(), title="数据批次", fontsize=8)
                    
                    ax.grid(True, alpha=0.3)
                    plt.tight_layout()
                    st.pyplot(fig)
                    plt.close()
                    
                    # 显示统计信息
                    col1, col2, col3, col4 = st.columns(4)
                    
                    with col1:
                        st.metric("交易日数量", f"{len(dates)}个")
                    
                    with col2:
                        date_range = f"{dates[0].strftime('%Y-%m-%d')} 至 {dates[-1].strftime('%Y-%m-%d')}"
                        st.metric("时间跨度", date_range)
                    
                    with col3:
                        start_price = prices[0]
                        end_price = prices[-1]
                        total_change_pct = ((end_price - start_price) / start_price * 100) if start_price > 0 else 0
                        st.metric("期间涨跌幅", f"{total_change_pct:+.2f}%")
                    
                    with col4:
                        st.metric("涉及批次", f"{len(unique_batches)}个")
                    
                    # 显示详细数据表格（可折叠）
                    with st.expander(f"📈 查看 {selected_stock} 详细价格数据", expanded=False):
                        # 创建详细数据表格
                        detail_data = []
                        for item in all_price_data:
                            detail_data.append({
                                '日期': item['date'].strftime('%Y-%m-%d'),
                                '收盘价': f"{item['price']:.2f}",
                                '数据批次': item['batch'],
                                '星期': ['周一', '周二', '周三', '周四', '周五', '周六', '周日'][item['date'].weekday()]
                            })
                        
                        detail_df = pd.DataFrame(detail_data)
                        st.dataframe(
                            detail_df,
                            use_container_width=True
                        )
                        
                        # 提供价格数据下载
                        csv_price = detail_df.to_csv(index=False, encoding="utf-8-sig")
                        st.download_button(
                            f"下载 {selected_stock} 价格数据 CSV",
                            data=csv_price,
                            file_name=f"{selected_stock}_价格数据_{pd.Timestamp('today').strftime('%Y%m%d')}.csv",
                            mime="text/csv",
                            key=f"download_{selected_stock}"
                        )
                else:
                    st.warning(f"未找到股票 {selected_stock} 在多个文件中的完整价格数据")
    else:
        st.info("需要至少两个批次的文件才能进行共同出现分析。")
		

# ========== 符合条件股票走势图（可折叠） ==========
with st.expander("📈 符合条件股票走势图（最新批次 - 按斜率降序排列）", expanded=False):
    latest_date = max(all_batch_dates) if all_batch_dates else None
    if latest_date:
        # 筛选最新批次中符合条件的股票，并按斜率降序排序
        passed_stocks_df = pd.DataFrame(all_results)
        passed_stocks = passed_stocks_df[
            (passed_stocks_df["日期"] == latest_date) & 
            (passed_stocks_df["是否符合"] == "✅ 是")
        ].sort_values("斜率(%)", ascending=False)  # 按斜率从大到小排序
        
        if not passed_stocks.empty:
            st.success(f"最新批次符合条件股票数量：{len(passed_stocks)} 只，按斜率从高到低展示")
            
            # 显示排序后的股票列表
            st.write("### 股票排序列表（斜率从高到低）")
            sorted_list = passed_stocks[["股票代码", "股票简称", "斜率(%)"]].reset_index(drop=True)
            st.dataframe(sorted_list.style.format({'斜率(%)': '{:.3f}%'}), use_container_width=True)
            
            df_latest = daily_dfs[latest_date]
            stock_data_map_latest = build_stock_data_map_from_df(df_latest)
            
            # 按排序后的顺序绘制走势图
            for idx, (_, row) in enumerate(passed_stocks.iterrows(), 1):
                code = row["股票代码"]
                name = row["股票简称"]
                slope = row["斜率(%)"]
                
                st.markdown(f"---")
                st.markdown(f"### #{idx} - {code} {name} (斜率: {slope:.3f}%)")
                
                if code in stock_data_map_latest:
                    closes = stock_data_map_latest[code]['closes']
                    ma_values = stock_data_map_latest[code]['ma_values']
                    
                    if len(closes) >= 2:
                        # 创建双子图
                        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
                        
                        # 左图：价格走势
                        ax1.plot(range(len(closes)), closes, marker="o", linewidth=2, color='blue', label='收盘价')
                        ax1.axhline(np.mean(closes), linestyle='--', linewidth=1.5, 
                                   label=f'{len(closes)}日均线', alpha=0.8, color='orange')
                        
                        # 添加价格标注
                        for i, price in enumerate(closes):
                            ax1.annotate(f'{price:.2f}', (i, price), 
                                       textcoords="offset points", xytext=(0, 8), 
                                       ha='center', fontsize=8)
                        
                        # 如果使用均线模式，绘制均线
                        if up_trend_mode == "ma_above" and len(ma_values) == len(closes):
                            ax1.plot(range(len(closes)), ma_values, marker="s", linestyle="-", 
                                   label='5日均线', linewidth=1.5, color='red')
                        
                        ax1.set_title(f"{code} {name}\n斜率: {slope:.3f}% (最近{close_days}天)", fontsize=14)
                        ax1.legend()
                        ax1.grid(True, alpha=0.3)
                        ax1.set_xlabel("交易日")
                        ax1.set_ylabel("价格")

                        # 右图：涨跌幅柱状图
                        price_changes = safe_calculate_price_changes(closes)
                        if price_changes:
                            colors = ['green' if x > 0 else 'red' for x in price_changes]
                            bars = ax2.bar(range(1, len(closes)), price_changes, color=colors, alpha=0.7)
                            
                            # 添加涨跌幅标注
                            for bar, ch in zip(bars, price_changes):
                                h = bar.get_height()
                                ax2.text(bar.get_x() + bar.get_width()/2., 
                                        h + (0.5 if h >= 0 else -0.5), 
                                        f'{ch:+.2f}%', 
                                        ha='center', va='bottom' if h >= 0 else 'top', 
                                        fontsize=8, fontweight='bold')
                            
                            ax2.axhline(0, color='black', linewidth=0.8)
                            ax2.set_title("每日涨跌幅", fontsize=14)
                            ax2.set_xlabel("交易日")
                            ax2.set_ylabel("涨跌幅(%)")
                            ax2.grid(True, alpha=0.3)
                        else:
                            ax2.text(0.5, 0.5, "无涨跌幅数据", 
                                   ha='center', va='center', transform=ax2.transAxes, fontsize=12)
                        
                        plt.tight_layout()
                        st.pyplot(fig)
                        plt.close()
                        
                        # 显示该股票的详细信息
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("当前价格", f"{closes[-1]:.2f}" if len(closes) > 0 else "N/A")
                        with col2:
                            start_price = closes[0] if len(closes) > 0 else 0
                            end_price = closes[-1] if len(closes) > 0 else 0
                            total_change = ((end_price - start_price) / start_price * 100) if start_price > 0 else 0
                            st.metric("总涨跌幅", f"{total_change:+.2f}%")
                        with col3:
                            st.metric("分析天数", f"{len(closes)}天")
                else:
                    st.warning(f"未找到股票 {code} 的详细数据")
        else:
            st.info("最新批次无符合条件的股票。")
    else:
        st.warning("无法确定最新批次日期")

# ========== 所属概念涨幅排名（可折叠） ==========
with st.expander("📊 所属概念涨幅排名（跨批次 · 多概念拆分）", expanded=False):
    if not daily_dfs:
        st.warning("请先上传 Excel 文件并解析批次数据。")
    else:
        stock_tracker = {}  # 跟踪每只股票的首尾价格

        # 按日期排序处理每个批次
        for date_str, df in sorted(daily_dfs.items()):
            close_cols = [c for c in df.columns if "收盘价" in str(c)]
            if not close_cols: continue
            close_col = close_cols[-1]  # 使用最新的收盘价列
            code_col = df.columns[0]
            concept_col = "所属概念"

            # 检查概念列是否存在
            if concept_col not in df.columns:
                # 尝试查找包含"概念"关键词的列
                concept_cols = [col for col in df.columns if "概念" in str(col)]
                if concept_cols:
                    concept_col = concept_cols[0]
                    st.warning(f"批次 {date_str} 使用 '{concept_col}' 作为概念列")
                else:
                    st.warning(f"批次 {date_str} 缺少概念列，标记为 '未知'")
                    df[concept_col] = "未知"

            # 处理每只股票
            for _, row in df.iterrows():
                code = str(row[code_col]).strip()
                try:
                    price = float(str(row[close_col]).replace(',', ''))
                except:
                    continue

                concept_str = str(row[concept_col]).strip()
                if concept_str in ['', 'nan', 'NaN'] or pd.isna(row[concept_col]):
                    concept_str = "未知"

                # 跟踪股票价格变化
                if code not in stock_tracker:
                    stock_tracker[code] = {
                        "first_price": price, "last_price": price,
                        "concept": concept_str,
                        "first_date": date_str, "last_date": date_str
                    }
                else:
                    stock_tracker[code]["last_price"] = price
                    stock_tracker[code]["last_date"] = date_str

        # 计算每只股票的涨幅
        gain_records = []
        for code, data in stock_tracker.items():
            if data["first_price"] == 0: continue
            gain_pct = (data["last_price"] - data["first_price"]) / data["first_price"] * 100
            gain_records.append({
                "股票代码": code,
                "所属概念": data["concept"],  # 原始字符串，如 "注册制次新股;专精特新;..."
                "起始价": round(data["first_price"], 2),
                "结束价": round(data["last_price"], 2),
                "涨幅%": round(gain_pct, 2)
            })

        if not gain_records:
            st.info("未找到跨批次有完整首尾价格的股票数据。")
        else:
            gain_df = pd.DataFrame(gain_records)

            # === 关键：拆分多概念（支持 301585 等）===
            gain_df['所属概念'] = gain_df['所属概念'].astype(str)
            gain_df = gain_df.assign(所属概念=gain_df['所属概念'].str.split(';')).explode('所属概念')
            gain_df['所属概念'] = gain_df['所属概念'].str.strip()
            gain_df = gain_df[gain_df['所属概念'].str.len() > 0]
            gain_df = gain_df[~gain_df['所属概念'].isin(['', 'nan', '未知', 'NaN'])]
            # =========================================

            # 按概念聚合计算统计指标
            ranking = (
                gain_df.groupby("所属概念")
                .agg(
                    股票数量=("股票代码", "nunique"),
                    平均涨幅=("涨幅%", "mean"),
                    最高涨幅=("涨幅%", "max"),
                    最低涨幅=("涨幅%", "min")
                )
                .round(2)
                .sort_values("平均涨幅", ascending=False)
                .reset_index()
            )

            # 展示排名结果
            st.dataframe(
                ranking.style
                .bar(subset=["平均涨幅"], color="#5fba7d")  # 平均涨幅条形图
                .bar(subset=["股票数量"], color="#4c78a8")   # 股票数量条形图
                .format({"平均涨幅": "{:.2f}%", "最高涨幅": "{:.2f}%", "最低涨幅": "{:.2f}%"}),
                use_container_width=True
            )

            # 提供下载功能
            csv = ranking.to_csv(index=False, encoding="utf-8-sig")
            st.download_button(
                "下载 所属概念涨幅排名 CSV",
                data=csv,
                file_name=f"所属概念涨幅排名_{pd.Timestamp('today').strftime('%Y%m%d')}.csv",
                mime="text/csv"
            )

            # 明细查看（支持搜索特定股票）
            with st.expander("查看个股明细（支持搜索 301585 等）"):
                search_code = st.text_input("搜索股票代码（如 301585）", "")
                detail_df = gain_df[gain_df["股票代码"].str.contains(search_code, na=False)] if search_code else gain_df
                st.dataframe(
                    detail_df[["股票代码", "所属概念", "起始价", "结束价", "涨幅%"]].sort_values("涨幅%", ascending=False),
                    use_container_width=True
                )