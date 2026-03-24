import streamlit as st
import pandas as pd
import datetime
from datetime import timedelta
import tushare as ts
import numpy as np

# ====================== 1. 页面配置与超级美化 CSS ======================
st.set_page_config(layout="wide", page_title="强势股排名动态追踪", page_icon="🚀")
pro = ts.pro_api(st.secrets["tushare"]["token"])
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stDataFrame { border-radius: 12px; overflow: hidden; box-shadow: 0 8px 24px rgba(0,0,0,0.05); }
    
    .title {
        background: linear-gradient(90deg, #1e3a8a, #3b82f6);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 2.8rem;
        font-weight: 700;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    
    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 16px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.06);
        border: 1px solid #f0f0f0;
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

# ====================== 【新增】celue2 的实时股价获取函数 ======================
def get_ts_code(code):
    code = str(code).zfill(6)
    if code.startswith('6'):
        return code + '.SH'
    elif code.startswith(('0', '3', '8')):
        return code + '.SZ'
    return code + '.SH'

@st.cache_data(ttl=30)
def get_real_time_price(code, target_date=None):
    """完全复用 celue2 的多接口实时价（pro.quote → sina → dc → 1min → 历史兜底）"""
    ts_code = get_ts_code(code)
    token = st.secrets["tushare"]["token"]
    pro = ts.pro_api(token)
    today_str = datetime.date.today().strftime("%Y%m%d")
    is_today = (target_date is None or str(target_date).replace("-", "") == today_str)

    interfaces = [
        ("pro.quote", lambda: pro.quote(ts_code=ts_code), 'price'),
        ("rt_k", lambda: pro.rt_k(ts_code=ts_code), 'close'),
        ("realtime_sina", lambda: ts.realtime_quote(ts_code=ts_code, src='sina'), 'PRICE'),
        ("realtime_dc", lambda: ts.realtime_quote(ts_code=ts_code, src='dc'), 'PRICE'),
        ("1min", lambda: pro.min(ts_code=ts_code, freq='1min', start_date=today_str, end_date=today_str), 'close'),
    ]
    
    for name, func, col in interfaces:
        try:
            df = func()
            if df is not None and not df.empty:
                price = float(df.iloc[-1][col])
                if price > 0:
                    return round(price, 2)
        except:
            continue
    # 历史兜底
    try:
        df = pro.daily(ts_code=ts_code, trade_date=today_str if is_today else str(target_date).replace("-", ""), fields='close')
        return round(float(df['close'].iloc[0]), 2) if not df.empty else None
    except:
        return None

def batch_get_realtime_prices(codes):
    """批量获取（更快）"""
    prices = {}
    for code in codes:
        p = get_real_time_price(code)
        if p:
            prices[code] = p
    return prices
# ====================== 【实时股价函数结束】 ======================

# ====================== 2. 初始化 Session State ======================
if "current_date" not in st.session_state:
    st.session_state.current_date = datetime.date.today()

# ====================== 3. 数据核心函数（保持原样） ======================
@st.cache_data(ttl=3600*24)
def get_stock_info():
    try:
        pro = ts.pro_api(st.secrets["tushare"]["token"])
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name,industry')
        return {row['ts_code']: {'name': row['name'], 'industry': row['industry'] or "其他"} 
                for _, row in df.iterrows()}
    except Exception as e:
        st.error(f"获取股票信息失败: {e}")
        return {}

@st.cache_data(ttl=3600*12)
def get_concept_combined(ts_code_list):
    pro = ts.pro_api(st.secrets["tushare"]["token"])
    concept_map = {code: [] for code in ts_code_list}
    try:
        for code in ts_code_list:
            df = pro.concept_detail(ts_code=code, fields='concept_name')
            if not df.empty:
                concept_map[code].extend(df['concept_name'].tolist()[:4])
    except:
        pass
    for code in ts_code_list:
        if not concept_map[code]:
            try:
                df = pro.index_member(ts_code=code, fields='index_name')
                if not df.empty:
                    concept_map[code].extend(df['index_name'].str.replace('指数','').tolist()[:2])
            except:
                pass
    for code in ts_code_list:
        if not concept_map[code]:
            try:
                df = pro.stock_company(ts_code=code, fields='main_business')
                if not df.empty and df.iloc[0]['main_business']:
                    biz = df.iloc[0]['main_business'][:20].replace('、',',')
                    concept_map[code] = [biz + "..."]
            except:
                concept_map[code] = ["-"]
    return {k: " / ".join(v) if v else "常规概念" for k, v in concept_map.items()}

@st.cache_data(ttl=3600*24)
def get_trading_calendar(end_date):
    pro = ts.pro_api(st.secrets["tushare"]["token"])
    start_point = (end_date - timedelta(days=365)).strftime("%Y%m%d")
    end_point = (end_date + timedelta(days=10)).strftime("%Y%m%d")
    df = pro.trade_cal(exchange='', start_date=start_point, end_date=end_point, is_open='1')
    return sorted(pd.to_datetime(df['cal_date']).dt.date.tolist())

def get_needed_dates(current_date, window_days):
    all_dates = get_trading_calendar(current_date)
    past_dates = [d for d in all_dates if d <= current_date]
    needed_n = window_days + 2
    return past_dates[-needed_n:] if len(past_dates) >= needed_n else past_dates

@st.cache_data(ttl=3600*12)
def fetch_daily_snapshot(trade_date):
    pro = ts.pro_api(st.secrets["tushare"]["token"])
    return pro.daily(trade_date=trade_date.strftime("%Y%m%d"), fields='ts_code,trade_date,close')

def calculate_top_n(target_date, full_df, window_days, top_n):
    available_dates = sorted(full_df['trade_date'].unique())
    target_dt = pd.Timestamp(target_date)
    past_dates = [d for d in available_dates if d <= target_dt]
    if len(past_dates) < window_days + 1:
        return pd.DataFrame()
    end_d, start_d = past_dates[-1], past_dates[-(window_days + 1)]
    df_end = full_df[full_df['trade_date'] == end_d][['ts_code', 'close']].rename(columns={'close': 'close_end'})
    df_start = full_df[full_df['trade_date'] == start_d][['ts_code', 'close']].rename(columns={'close': 'close_start'})
    merged = pd.merge(df_end, df_start, on='ts_code')
    merged['pct_chg'] = (merged['close_end'] - merged['close_start']) / merged['close_start'] * 100
    top_df = merged.sort_values('pct_chg', ascending=False).head(top_n).reset_index(drop=True)
    top_df['排名'] = top_df.index + 1
    return top_df

# ====================== 4. 侧边栏 ======================
with st.sidebar:
    st.header("⚙️ 控制面板")
    st.divider()
    zoom_level = st.slider("🔍 界面缩放（手机推荐）", 0.7, 1.5, 1.0, 0.05)
    window_days = st.number_input("统计周期 (天)", 5, 60, 10)
    top_n = st.number_input("显示数量", 10, 100, 40)
    st.divider()
    picked_date = st.date_input("手动选择观察日期", value=st.session_state.current_date)
    if picked_date != st.session_state.current_date:
        st.session_state.current_date = picked_date
        st.rerun()

stock_info_map = get_stock_info()
needed_dates = get_needed_dates(st.session_state.current_date, window_days)

if len(needed_dates) < window_days + 1:
    st.warning("所选日期的历史数据不足。")
    st.stop()

with st.spinner(f"正在分析 {needed_dates[-1]} 的概念与排名数据..."):
    all_snaps = [fetch_daily_snapshot(d) for d in needed_dates if not fetch_daily_snapshot(d).empty]
    market_df = pd.concat(all_snaps, ignore_index=True)
    market_df['trade_date'] = pd.to_datetime(market_df['trade_date'])

    df_today = calculate_top_n(needed_dates[-1], market_df, window_days, top_n)
    df_yesterday = calculate_top_n(needed_dates[-2], market_df, window_days, top_n)
    
    top_codes = df_today['ts_code'].tolist()
    concept_map = get_concept_combined(top_codes)

# ====================== 6. 界面呈现 ======================
st.markdown('<h1 class="title">🚀 强势股概念排名动态追踪</h1>', unsafe_allow_html=True)

if not df_today.empty:
    # ---------- Top 指标卡片 ----------
    y_rank_map = {row['ts_code']: row['排名'] for _, row in df_yesterday.iterrows()} if not df_yesterday.empty else {}
    
    col_a, col_b, col_c, col_d = st.columns(4)
    with col_a:
        top1 = df_today.iloc[0]
        info1 = stock_info_map.get(top1['ts_code'], {})
        st.markdown(f'<div class="metric-card"><h4>🏆 榜首龙头</h4><h3>{info1.get("name","-")}</h3><p style="color:#22c55e; font-size:1.8rem; margin:0;">+{top1["pct_chg"]:.2f}%</p></div>', unsafe_allow_html=True)
    with col_b: st.metric(f"Top{top_n} 均幅", f"{df_today['pct_chg'].mean():.2f}%")
    with col_c: st.metric("新晋上榜", sum(1 for c in df_today['ts_code'] if c not in y_rank_map))
    with col_d: st.metric("排名上升", sum(1 for _, r in df_today.iterrows() if y_rank_map.get(r['ts_code'], 999) > r['排名']))

    # ---------- 日期导航 ----------
    st.markdown("---")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c1:
        if st.button("⬅️ 前一交易日", use_container_width=True):
            st.session_state.current_date = needed_dates[-2]
            st.rerun()
    with c2:
        st.subheader(f"📅 **数据日期：{needed_dates[-1]}**")
    with c3:
        full_cal = get_trading_calendar(st.session_state.current_date)
        future_dates = [d for d in full_cal if d > needed_dates[-1]]
        if future_dates and st.button("后一交易日 ➡️", use_container_width=True):
            st.session_state.current_date = future_dates[0]
            st.rerun()

    # ========== 【新增】批量获取实时价 ==========
    top_codes_6 = [code[:6] for code in df_today['ts_code'].tolist()]
    realtime_map = {}
    if st.session_state.current_date == datetime.date.today():
        with st.spinner("📡 正在拉取实时股价..."):
            realtime_map = batch_get_realtime_prices(top_codes_6)
    else:
        for _, row in df_today.iterrows():
            realtime_map[row['ts_code'][:6]] = round(row['close_end'], 2)

    # ---------- 数据整理 ----------
    report_list = []
    for _, row in df_today.iterrows():
        ts_code = row['ts_code']
        code6 = ts_code[:6]
        info = stock_info_map.get(ts_code, {'name':'未知','industry':'其他'})
        today_rank = int(row['排名'])
        y_rank = y_rank_map.get(ts_code)
        delta = y_rank - today_rank if y_rank else 0
        trend_label = f"↑ {delta}" if delta > 0 else f"↓ {abs(delta)}" if delta < 0 else ("🆕 新榜" if not y_rank else "持平")
        current_price = realtime_map.get(code6, "-")
        
        report_list.append({
            "排名": today_rank,
            "代码": code6,
            "名称": info['name'],
            "所属行业": info['industry'],
            "所属概念": concept_map.get(ts_code, "-"),
            f"{window_days}日涨幅": round(row['pct_chg'], 2),
            "实时价": current_price,
            "变动值": delta,
            "趋势": trend_label,
        })
    
    final_df = pd.DataFrame(report_list)

    def apply_style(df):
        def highlight_trend(row):
            styles = [''] * len(row)
            delta = row['变动值']
            trend_idx = df.columns.get_loc('趋势')
            if "🆕" in row['趋势']:
                styles[trend_idx] = 'background-color: rgba(139, 92, 246, 0.6); color: white; font-weight: bold;'
            elif delta > 0:
                alpha = min(0.3 + (delta / 20), 0.9)
                styles[trend_idx] = f'background-color: rgba(34, 197, 94, {alpha}); color: white; font-weight: bold;'
            elif delta < 0:
                alpha = min(0.3 + (abs(delta) / 20), 0.9)
                styles[trend_idx] = f'background-color: rgba(239, 68, 68, {alpha}); color: white; font-weight: bold;'
            return styles
        return df.style.apply(highlight_trend, axis=1)

    # ---------- 表格与搜索 ----------
    search = st.text_input("🔍 搜索关键词 (股票、概念或行业)", "")
    display_df = final_df[final_df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)] if search else final_df

    st.dataframe(
        apply_style(display_df),
        column_config={
            "变动值": None,
            "排名": st.column_config.NumberColumn("排名", width=50),
            "代码": st.column_config.TextColumn("代码", width=70),
            "名称": st.column_config.TextColumn("名称", width=130),
            "所属行业": st.column_config.TextColumn("行业", width=90),
            "所属概念": st.column_config.TextColumn("所属概念", width=220),
            f"{window_days}日涨幅": st.column_config.ProgressColumn(
                "涨幅", format="%.2f%%", min_value=0, max_value=final_df[f"{window_days}日涨幅"].max()
            ),
            "实时价": st.column_config.NumberColumn("实时价", format="%.2f", width=100),  # ← 新增列
            "趋势": st.column_config.TextColumn("趋势", width=80)
        },
        use_container_width=True, height=600, hide_index=True
    )

    # ---------- 行业分布 ----------
    st.divider()
    st.subheader("🏭 强势股行业热度分布")
    industry_count = final_df['所属行业'].value_counts()
    st.bar_chart(industry_count, color="#3b82f6", use_container_width=True)
    
    st.download_button("📥 导出分析结果 (CSV)", final_df.to_csv(index=False).encode('utf-8'), f"Rank_{needed_dates[-1]}.csv", "text/csv")

st.caption("注：概念获取顺序：Tushare概念库 > 指数成员标签 > 公司主营业务关键字。实时价使用 celue2 多接口机制（当天实时刷新）")

# ====================== 移动端优化 CSS ======================
st.markdown(f"""
<style>
    @media (max-width: 768px) {{
        .stApp {{ zoom: {zoom_level}; }}
        .main .block-container {{ padding-right: 0 !important; padding-left: 0 !important; max-width: 100% !important; }}
        [data-testid="stDataFrame"], .stDataFrame {{ width: 100% !important; max-width: 100vw !important; }}
        .stDataFrame table {{ font-size: 15px !important; width: max-content !important; min-width: 980px !important; }}
    }}
</style>
""", unsafe_allow_html=True)
