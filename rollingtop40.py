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

# ====================== 2. 初始化 Session State ======================
if "current_date" not in st.session_state:
    st.session_state.current_date = datetime.date.today()

# ====================== 3. 数据核心函数 ======================

@st.cache_data(ttl=3600*24)
def get_stock_info():
    """获取股票基础信息（名称、行业）"""
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
    """
    三级降级机制获取概念：
    1. Tushare标准概念库 -> 2. 指数成分标签 -> 3. 主营业务关键词提取
    """
    pro = ts.pro_api(st.secrets["tushare"]["token"])
    concept_map = {code: [] for code in ts_code_list}
    
    # --- 策略 1: 标准概念明细 ---
    try:
        for code in ts_code_list:
            df = pro.concept_detail(ts_code=code, fields='concept_name')
            if not df.empty:
                concept_map[code].extend(df['concept_name'].tolist()[:4])
    except:
        pass

    # --- 策略 2: 指数成员信息补充 ---
    for code in ts_code_list:
        if not concept_map[code]:
            try:
                df = pro.index_member(ts_code=code, fields='index_name')
                if not df.empty:
                    concept_map[code].extend(df['index_name'].str.replace('指数','').tolist()[:2])
            except:
                pass

    # --- 策略 3: 主营业务兜底 ---
    for code in ts_code_list:
        if not concept_map[code]:
            try:
                df = pro.stock_company(ts_code=code, fields='main_business')
                if not df.empty and df.iloc[0]['main_business']:
                    # 提取主营业务前20个字作为概念描述
                    biz = df.iloc[0]['main_business'][:20].replace('、',',')
                    concept_map[code] = [biz + "..."]
            except:
                concept_map[code] = ["-"]

    return {k: " / ".join(v) if v else "常规概念" for k, v in concept_map.items()}

@st.cache_data(ttl=3600*24)
def get_trading_calendar(end_date):
    """获取交易日历"""
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
    """获取单日行情快照"""
    pro = ts.pro_api(st.session_state.tushare_token)
    return pro.daily(trade_date=trade_date.strftime("%Y%m%d"), fields='ts_code,trade_date,close')

def calculate_top_n(target_date, full_df, window_days, top_n):
    """计算滚动周期内的涨幅排名"""
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
    st.caption("左右滑动即可放大/缩小整个界面")
    
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
    
    # 批量获取当前Top N个股的概念
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

    # ---------- 日期导航按钮 (找回) ----------
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

    # ---------- 数据整理与颜色逻辑 (修复) ----------
    report_list = []
    for _, row in df_today.iterrows():
        ts_code = row['ts_code']
        info = stock_info_map.get(ts_code, {'name':'未知','industry':'其他'})
        today_rank = int(row['排名'])
        y_rank = y_rank_map.get(ts_code)
        
        delta = y_rank - today_rank if y_rank else 0
        trend_label = f"↑ {delta}" if delta > 0 else f"↓ {abs(delta)}" if delta < 0 else ("🆕 新榜" if not y_rank else "持平")
        
        report_list.append({
            "排名": today_rank,
            "代码": ts_code[:6],
            "名称": info['name'],
            "所属行业": info['industry'],
            "所属概念": concept_map.get(ts_code, "-"), # 新增列
            f"{window_days}日涨幅": round(row['pct_chg'], 2),
            "变动值": delta, # 辅助渲染列
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
                # 绿色深浅随幅度变化
                alpha = min(0.3 + (delta / 20), 0.9)
                styles[trend_idx] = f'background-color: rgba(34, 197, 94, {alpha}); color: white; font-weight: bold;'
            elif delta < 0:
                # 红色深浅随幅度变化
                alpha = min(0.3 + (abs(delta) / 20), 0.9)
                styles[trend_idx] = f'background-color: rgba(239, 68, 68, {alpha}); color: white; font-weight: bold;'
            return styles
        return df.style.apply(highlight_trend, axis=1)

    # ---------- 表格与搜索 ----------
    search = st.text_input("🔍 搜索关键词 (股票、概念或行业)", "")
    # 搜索逻辑增强
    display_df = final_df[final_df.astype(str).apply(lambda x: x.str.contains(search, case=False)).any(axis=1)] if search else final_df

    st.dataframe(
        apply_style(display_df),
        column_config={
            "变动值": None,  # 隐藏辅助列
            "排名": st.column_config.NumberColumn("排名", width=50),
            "代码": st.column_config.TextColumn("代码", width=70),
            "名称": st.column_config.TextColumn("名称", width=130),
            "所属行业": st.column_config.TextColumn("行业", width=90),
            "所属概念": st.column_config.TextColumn("所属概念", width=220),
            f"{window_days}日涨幅": st.column_config.ProgressColumn(
                "涨幅", format="%.2f%%", min_value=0, max_value=final_df[f"{window_days}日涨幅"].max()
            ),
            "趋势": st.column_config.TextColumn("趋势", width=80)
        },
        use_container_width=True, height=600, hide_index=True
    )

    # ---------- 行业分布 (另起一行) ----------
    st.divider()
    st.subheader("🏭 强势股行业热度分布")
    industry_count = final_df['所属行业'].value_counts()
    st.bar_chart(industry_count, color="#3b82f6", use_container_width=True)
    
    # 导出按钮
    st.download_button("📥 导出分析结果 (CSV)", final_df.to_csv(index=False).encode('utf-8'), f"Rank_{needed_dates[-1]}.csv", "text/csv")

st.caption("注：概念获取顺序：Tushare概念库 > 指数成员标签 > 公司主营业务关键字。")

# ====================== 移动端缩放 CSS ======================
st.markdown(f"""
<style>
    @media (max-width: 768px) {{
        .stApp {{
            zoom: {zoom_level};
        }}
        .stDataFrame table {{
            font-size: 15px !important;
        }}
        .stDataFrame {{
            overflow-x: auto;
        }}
    }}
</style>
""", unsafe_allow_html=True)
