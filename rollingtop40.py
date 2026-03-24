import streamlit as st
import pandas as pd
import datetime
from datetime import timedelta
import tushare as ts
import numpy as np

# ====================== 1. 页面配置 ======================
st.set_page_config(layout="wide", page_title="强势股排名动态追踪", page_icon="🚀")
pro = ts.pro_api(st.secrets["tushare"]["token"])
st.markdown("""
    <style>
    .main { background-color: #f8f9fa; }
    .stDataFrame { border-radius: 12px; overflow: hidden; box-shadow: 0 8px 24px rgba(0,0,0,0.05); }
    .title { background: linear-gradient(90deg, #1e3a8a, #3b82f6); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2.8rem; font-weight: 700; text-align: center; margin-bottom: 0.5rem; }
    .metric-card { background: white; padding: 20px; border-radius: 16px; box-shadow: 0 4px 20px rgba(0,0,0,0.06); border: 1px solid #f0f0f0; text-align: center; }
    </style>
""", unsafe_allow_html=True)

# ====================== 2. Session State & 核心函数 ======================
if "current_date" not in st.session_state:
    st.session_state.current_date = datetime.date.today()

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
    except: pass
    for code in ts_code_list:
        if not concept_map[code]:
            try:
                df = pro.index_member(ts_code=code, fields='index_name')
                if not df.empty:
                    concept_map[code].extend(df['index_name'].str.replace('指数','').tolist()[:2])
            except: pass
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
    needed_n = window_days + 30   # 加大缓冲，保证昨天有足够数据
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
    window_days = st.number_input("统计周期 (天)", 5, 60, 10, key="window_days")
    top_n = st.number_input("显示数量", 10, 100, 40, key="top_n")
    st.divider()
    picked_date = st.date_input("手动选择观察日期", value=st.session_state.current_date)
    if picked_date != st.session_state.current_date:
        st.session_state.current_date = picked_date
        st.rerun()

# ====================== 5. 主逻辑 ======================
stock_info_map = get_stock_info()
needed_dates = get_needed_dates(st.session_state.current_date, st.session_state.window_days)

if len(needed_dates) < st.session_state.window_days + 1:
    st.warning("所选日期的历史数据不足。")
    st.stop()

with st.spinner(f"正在分析 {st.session_state.current_date} ..."):
    all_snaps = [fetch_daily_snapshot(d) for d in needed_dates if not fetch_daily_snapshot(d).empty]
    market_df = pd.concat(all_snaps, ignore_index=True) if all_snaps else pd.DataFrame()
    market_df['trade_date'] = pd.to_datetime(market_df['trade_date'])

    # ================ 新增：关键修复 ================
    if market_df.empty:
        st.error("无法获取任何交易数据，请稍后重试")
        st.stop()

    # 真正有数据的最新日期（这就是今天数据还没回来时要用的日期）
    effective_date = pd.to_datetime(market_df['trade_date']).dt.date.max()

    # 如果用户选的日期比实际数据新，就提示并使用实际日期
    if st.session_state.current_date > effective_date:
        st.warning(f"⚠️ {st.session_state.current_date} 数据尚未返回，实际使用最新可用日期 **{effective_date}** 计算排名")

    # 用 effective_date 计算今天的排名
    df_today = calculate_top_n(effective_date, market_df, st.session_state.window_days, st.session_state.top_n)

    # ================ 新增：更准确的“昨天”查找 ================
    # 从 market_df 里所有可用日期里找出 effective_date 的前一个交易日
    available_dates = sorted(market_df['trade_date'].dt.date.unique())
    yesterday_date = None
    df_yesterday = pd.DataFrame()
    
    idx = available_dates.index(effective_date) if effective_date in available_dates else -1
    if idx > 0:
        yesterday_date = available_dates[idx - 1]
        df_yesterday = calculate_top_n(yesterday_date, market_df, st.session_state.window_days, st.session_state.top_n)
    # ================================================
    
    top_codes = df_today['ts_code'].tolist()
    concept_map = get_concept_combined(top_codes)

# ====================== 6. 界面呈现 ======================
st.markdown('<h1 class="title">🚀 强势股概念排名动态追踪</h1>', unsafe_allow_html=True)

if not df_today.empty:
    y_rank_map = {}
    if not df_yesterday.empty:
        for _, row in df_yesterday.iterrows():
            code6 = row['ts_code'][:6]
            y_rank_map[code6] = row['排名']

    col_a, col_b, col_c, col_d = st.columns(4)
    with col_a:
        top1 = df_today.iloc[0]
        info1 = stock_info_map.get(top1['ts_code'], {})
        st.markdown(f'<div class="metric-card"><h4>🏆 榜首龙头</h4><h3>{info1.get("name","-")}</h3><p style="color:#1e3a8a; font-size:1.8rem; margin:0;">+{top1["pct_chg"]:.2f}%</p></div>', unsafe_allow_html=True)
    with col_b: st.metric(f"Top{st.session_state.top_n} 均幅", f"{df_today['pct_chg'].mean():.2f}%")
    with col_c: st.metric("新晋上榜", sum(1 for c in df_today['ts_code'] if c[:6] not in y_rank_map))
    with col_d: st.metric("排名上升", sum(1 for _, r in df_today.iterrows() if y_rank_map.get(r['ts_code'][:6], 999) > r['排名']))

    st.markdown("---")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c1:
        if st.button("⬅️ 前一交易日", use_container_width=True):
            st.session_state.current_date = needed_dates[-2]
            st.rerun()
    with c2:
        st.subheader(f"📅 **数据日期：{effective_date}**")
        st.caption(f"昨天对比日期：**{yesterday_date or '暂无'}** | 昨天数据：**{'✅ 成功' if not df_yesterday.empty else '❌ 暂缺'}**")
    with c3:
        full_cal = get_trading_calendar(st.session_state.current_date)
        future_dates = [d for d in full_cal if d > needed_dates[-1]]
        if future_dates and st.button("后一交易日 ➡️", use_container_width=True):
            st.session_state.current_date = future_dates[0]
            st.rerun()

    # 收盘价（已经收盘，就用历史收盘价）
    realtime_map = {row['ts_code'][:6]: round(row['close_end'], 2) for _, row in df_today.iterrows()}

    report_list = []
    for _, row in df_today.iterrows():
        ts_code = row['ts_code']
        code6 = ts_code[:6]
        info = stock_info_map.get(ts_code, {'name':'未知','industry':'其他'})
        today_rank = int(row['排名'])
        y_rank = y_rank_map.get(code6)
        
        if y_rank is None:
            trend_label = "🆕 新榜"
            delta = 0
        else:
            delta = y_rank - today_rank
            trend_label = f"↑ {delta}" if delta > 0 else f"↓ {abs(delta)}" if delta < 0 else "持平"
        
        report_list.append({
            "排名": today_rank,
            "代码": code6,
            "名称": info['name'],
            "所属行业": info['industry'],
            "所属概念": concept_map.get(ts_code, "-"),
            f"{st.session_state.window_days}日涨幅": round(row['pct_chg'], 2),
            "实时价": realtime_map.get(code6, "-"),
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
            f"{st.session_state.window_days}日涨幅": st.column_config.ProgressColumn("涨幅", format="%.2f%%", min_value=0, max_value=final_df[f"{st.session_state.window_days}日涨幅"].max()),
            "实时价": st.column_config.NumberColumn("收盘价", format="%.2f", width=100),
            "趋势": st.column_config.TextColumn("趋势", width=90)
        },
        use_container_width=True, height=600, hide_index=True
    )

    st.divider()
    st.subheader("🏭 强势股行业热度分布")
    industry_count = final_df['所属行业'].value_counts()
    st.bar_chart(industry_count, color="#3b82f6", use_container_width=True)
    
    st.download_button("📥 导出分析结果 (CSV)", final_df.to_csv(index=False).encode('utf-8'), f"Rank_{needed_dates[-1]}.csv", "text/csv")

st.caption("注：已切换为纯收盘价模式（收盘后使用历史数据计算排名），排名对比正常。")

# ====================== 移动端优化 ======================
st.markdown(f"""
<style>
    @media (max-width: 768px) {{
        .stApp {{ zoom: {zoom_level}; }}
        .main .block-container {{ padding-right: 0 !important; padding-left: 0 !important; max-width: 100% !important; }}
        .stDataFrame table {{ font-size: 15px !important; width: max-content !important; min-width: 980px !important; }}
    }}
</style>
""", unsafe_allow_html=True)
