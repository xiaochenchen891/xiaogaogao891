import streamlit as st
import pandas as pd
from datetime import date, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import tushare as ts

st.set_page_config(page_title="做T交易记录 & 收益计算", layout="wide")
st.title("📈 股票做T交易记录与收益计算 App")
st.markdown("**v5.2 交易金额显示版** | 涨红跌绿版 | 蜡烛图：涨=红 跌=绿 | 成交量同步修改 | 所有功能完整保留")

# ==================== 左侧边栏 - Tushare Token ====================
with st.sidebar:
    st.header("🔑 Tushare 配置")
    if "tushare_token" not in st.session_state:
        st.session_state.tushare_token = ""
    
    token_input = st.text_input(
        "Tushare Pro Token", 
        value=st.session_state.tushare_token, 
        type="password",
        help="去 https://tushare.pro 免费注册获取 Token"
    )
    if st.button("💾 保存 Token"):
        st.session_state.tushare_token = token_input
        if token_input.strip():
            st.success("✅ Token 已保存！")
            st.rerun()
        else:
            st.warning("Token 不能为空")
    st.caption("Token 只保存在浏览器，本地运行安全")

# ==================== 初始化 session_state ====================
if "trades" not in st.session_state:
    st.session_state.trades = pd.DataFrame(columns=[
        "交易日期", "交易类型", "股票代码", "买入价格", "卖出价格", 
        "股数", "佣金率", "买入佣金", "卖出佣金", "印花税", 
        "毛利润", "净利润", "备注", "交易金额"
    ])

if "last_stock_code" not in st.session_state:
    st.session_state.last_stock_code = ""

if "total_funds" not in st.session_state:
    st.session_state.total_funds = 100000.0  # 默认10万总仓位资金

df = st.session_state.trades

# ==================== 辅助函数 ====================
def calc_profit(buy_price, sell_price, qty, comm_rate=0.0003):
    if qty <= 0 or buy_price <= 0 or sell_price <= 0:
        return 0, 0, 0, 0, 0
    gross = (sell_price - buy_price) * qty
    buy_comm = max(5.0, buy_price * qty * comm_rate)
    sell_comm = max(5.0, sell_price * qty * comm_rate)
    stamp_tax = sell_price * qty * 0.001
    net_profit = gross - buy_comm - sell_comm - stamp_tax
    return gross, buy_comm, sell_comm, stamp_tax, net_profit

def calc_transaction_amount(row):
    """计算当前交易的总金额"""
    qty = row["股数"]
    if pd.isna(qty) or qty <= 0:
        return 0.0
    t_type = row.get("交易类型", "")
    buy_p = row.get("买入价格")
    sell_p = row.get("卖出价格")
    if t_type == "完整做T (买+卖)":
        return round((buy_p or 0) * qty + (sell_p or 0) * qty, 2)
    elif t_type == "仅买入":
        return round((buy_p or 0) * qty, 2)
    elif t_type == "仅卖出":
        return round((sell_p or 0) * qty, 2)
    return 0.0

@st.cache_data(ttl=3600)
def get_kline_data(stock_code: str, days: int = 90):
    if not st.session_state.get("tushare_token", "").strip():
        return None, "请先在左侧边栏输入并保存 Tushare Token"
    try:
        pro = ts.pro_api(st.session_state.tushare_token)
        code = stock_code.strip().upper()
        
        if len(code) == 6:
            if code.startswith('6') or code.startswith('688'):
                ts_code = code + '.SH'
            else:
                ts_code = code + '.SZ'
        else:
            ts_code = code
        
        end_date = date.today().strftime('%Y%m%d')
        start_date = (date.today() - timedelta(days=days + 40)).strftime('%Y%m%d')
        
        df_k = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        
        if df_k.empty:
            return None, f"""❌ 未获取到 **{ts_code}** 的数据

可能原因：
1. 该股票为**科创板（688开头）**，免费Token积分不足
2. 股票代码输入错误（请确认是6位数字）
3. Tushare数据暂未更新
4. Token权限/网络问题

当前查询代码：**{ts_code}**"""
        
        df_k['trade_date'] = pd.to_datetime(df_k['trade_date'], format='%Y%m%d')
        df_k['date_str'] = df_k['trade_date'].dt.strftime('%Y-%m-%d')
        df_k = df_k.sort_values('trade_date').reset_index(drop=True)
        return df_k, None
    except Exception as e:
        return None, f"Tushare错误: {str(e)}"

def plot_kline_with_trades(stock_code: str, trades_df: pd.DataFrame):
    k_data, error = get_kline_data(stock_code)
    if error:
        st.error(error)
        return
    
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.75, 0.25],
        subplot_titles=(f"{stock_code} 近3个月K线图 + 做T买卖点标记", "成交量（量能）")
    )
    
    fig.add_trace(
        go.Candlestick(
            x=k_data['date_str'],
            open=k_data['open'], high=k_data['high'],
            low=k_data['low'], close=k_data['close'],
            name="日K线",
            increasing_line_color='#ef5350',
            decreasing_line_color='#26a69a'
        ),
        row=1, col=1
    )
    
    stock_trades = trades_df[trades_df["股票代码"] == stock_code].copy()
    if not stock_trades.empty:
        stock_trades['date_str'] = pd.to_datetime(stock_trades["交易日期"]).dt.strftime('%Y-%m-%d')
        
        buys = stock_trades[stock_trades["交易类型"].isin(["完整做T (买+卖)", "仅买入"])]
        if not buys.empty:
            fig.add_trace(go.Scatter(
                x=buys['date_str'], y=buys['买入价格'],
                mode='markers+text',
                marker=dict(symbol='triangle-up', size=16, color='#00ff88', line=dict(width=2, color='white')),
                text=buys.apply(lambda x: f"买{x['股数']}", axis=1),
                textposition="bottom center",
                name="买入点",
                hovertemplate="买入: %{y:.3f}<br>日期: %{x}<extra></extra>"
            ), row=1, col=1)
        
        sells = stock_trades[stock_trades["交易类型"].isin(["完整做T (买+卖)", "仅卖出"])]
        if not sells.empty:
            fig.add_trace(go.Scatter(
                x=sells['date_str'], y=sells['卖出价格'],
                mode='markers+text',
                marker=dict(symbol='triangle-down', size=16, color='#ff4444', line=dict(width=2, color='white')),
                text=sells.apply(lambda x: f"卖{x['股数']}", axis=1),
                textposition="top center",
                name="卖出点",
                hovertemplate="卖出: %{y:.3f}<br>日期: %{x}<extra></extra>"
            ), row=1, col=1)
    
    colors = ['#ef5350' if o < c else '#26a69a' for o, c in zip(k_data['open'], k_data['close'])]
    fig.add_trace(
        go.Bar(
            x=k_data['date_str'],
            y=k_data['vol'],
            name="成交量",
            marker_color=colors,
            opacity=0.8
        ),
        row=2, col=1
    )
    
    fig.update_layout(
        height=750,
        xaxis_rangeslider_visible=False,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
        hovermode="x unified"
    )
    
    fig.update_xaxes(type="category", tickangle=45)
    fig.update_yaxes(title_text="价格 (元)", row=1, col=1)
    fig.update_yaxes(title_text="成交量 (股)", row=2, col=1)
    
    st.plotly_chart(fig, use_container_width=True, key=f"kline_vol_{stock_code}")

# 新增：当前仓位计算函数（最小插入，不影响原有逻辑）
def get_current_positions(trades_df: pd.DataFrame):
    if trades_df.empty:
        return pd.DataFrame(columns=["股票代码", "持仓股数", "平均成本", "最新价", "持仓金额", "浮动盈亏", "仓位占比"])
    
    positions = []
    for stock in trades_df["股票代码"].unique():
        stock_trades = trades_df[trades_df["股票代码"] == stock].sort_values("交易日期")
        shares = 0
        total_cost = 0.0
        for _, row in stock_trades.iterrows():
            t_type = row.get("交易类型", "")
            qty = row["股数"]
            if t_type == "仅买入":
                shares += qty
                if pd.notna(row["买入价格"]):
                    total_cost += row["买入价格"] * qty
            elif t_type == "仅卖出":
                shares = max(0, shares - qty)
        if shares > 0:
            avg_cost = round(total_cost / shares, 3)
            k_data, _ = get_kline_data(stock)
            latest_price = round(k_data['close'].iloc[-1], 3) if k_data is not None and not k_data.empty else avg_cost
            position_value = round(latest_price * shares, 2)
            unrealized = round((latest_price - avg_cost) * shares, 2)
            ratio = round((position_value / st.session_state.total_funds) * 100, 1) if st.session_state.total_funds > 0 else 0
            positions.append({
                "股票代码": stock,
                "持仓股数": int(shares),
                "平均成本": avg_cost,
                "最新价": latest_price,
                "持仓金额": position_value,
                "浮动盈亏": unrealized,
                "仓位占比": f"{ratio}%"
            })
    return pd.DataFrame(positions)

# ==================== Tabs ====================
tab1, tab2, tab3, tab4 = st.tabs(["➕ 新增交易", "📋 交易记录", "📊 收益统计", "📈 K线查看"])

with tab1:
    st.subheader("记录一次做T交易")
    with st.form("new_trade"):
        trade_type = st.selectbox(
            "交易类型", 
            ["完整做T (买+卖)", "仅买入", "仅卖出"], 
            index=0,
            help="当天可能只有买或只有卖，现在支持单独记录"
        )
        
        col1, col2, col3 = st.columns(3)
        with col1:
            trade_date = st.date_input("交易日期", value=date.today())
            stock_code = st.text_input("股票代码", placeholder="600519 或 688001").upper()
        with col2:
            buy_price = None
            sell_price = None
            if trade_type in ["完整做T (买+卖)", "仅买入"]:
                buy_price = st.number_input("买入价格 (元)", min_value=0.01, value=10.0, step=0.01)
            if trade_type in ["完整做T (买+卖)", "仅卖出"]:
                sell_price = st.number_input("卖出价格 (元)", min_value=0.01, value=10.5, step=0.01)
            qty = st.number_input("股数", min_value=100, value=100, step=100)
            comm_rate = st.number_input("佣金率 (默认万3)", min_value=0.0001, max_value=0.01, value=0.0003, step=0.0001, format="%.4f")
        with col3:
            pass
        notes = st.text_input("备注（可选）", placeholder="例如：早盘低开拉升")
        submitted = st.form_submit_button("✅ 提交本次做T记录")
        if submitted:
            if trade_type == "完整做T (买+卖)":
                gross, buy_comm, sell_comm, stamp, net = calc_profit(buy_price, sell_price, qty, comm_rate)
            else:
                gross = buy_comm = sell_comm = stamp = net = 0.0
            new_row = pd.DataFrame([{
                "交易日期": trade_date, 
                "交易类型": trade_type,
                "股票代码": stock_code,
                "买入价格": round(buy_price, 3) if buy_price is not None else None, 
                "卖出价格": round(sell_price, 3) if sell_price is not None else None,
                "股数": int(qty), "佣金率": comm_rate,
                "买入佣金": round(buy_comm, 2), "卖出佣金": round(sell_comm, 2),
                "印花税": round(stamp, 2), "毛利润": round(gross, 2),
                "净利润": round(net, 2), "备注": notes,
                "交易金额": calc_transaction_amount({"交易类型": trade_type, "买入价格": buy_price, "卖出价格": sell_price, "股数": qty})
            }])
            st.session_state.trades = pd.concat([st.session_state.trades, new_row], ignore_index=True)
            st.session_state.last_stock_code = stock_code
            st.success(f"✅ 记录成功！{'本次净利润 **' + f'{net:.2f} 元**' if trade_type == '完整做T (买+卖)' else '已记录（非完整做T不计入当日收益）'}")
            st.rerun()

with tab2:
    st.subheader("所有交易记录")
    if len(df) == 0:
        st.info("还没有记录任何交易～")
    else:
        display_df = df.copy()
        display_df["交易金额"] = display_df.apply(calc_transaction_amount, axis=1)
        
        edited_df = st.data_editor(
            display_df.sort_values("交易日期", ascending=False),
            hide_index=True,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "交易日期": st.column_config.DateColumn(format="YYYY-MM-DD"),
                "净利润": st.column_config.NumberColumn(format="%.2f"),
                "交易金额": st.column_config.NumberColumn(format="%.2f", help="当前交易总金额（元）"),
            }
        )
        # 保存回 session_state 并重新计算交易金额
        st.session_state.trades = edited_df.drop(columns=["交易金额"], errors="ignore")
        st.session_state.trades["交易金额"] = st.session_state.trades.apply(calc_transaction_amount, axis=1)
        
        col_dl, col_ul = st.columns(2)
        with col_dl:
            csv = st.session_state.trades.to_csv(index=False).encode('utf-8')
            st.download_button("📥 下载为 CSV", csv, "doT_trades.csv", "text/csv")
        with col_ul:
            uploaded = st.file_uploader("📤 上传 CSV 恢复数据", type=["csv"])
            if uploaded:
                new_df = pd.read_csv(uploaded)
                st.session_state.trades = pd.concat([st.session_state.trades, new_df]).drop_duplicates(subset=["交易日期", "股票代码"]).reset_index(drop=True)
                st.success("✅ CSV 已导入")
                st.rerun()

with tab3:
    st.subheader("📊 做T 总体收益统计")
    
    st.subheader("💰 当前仓位总览")
    col_f1, col_f2 = st.columns([2, 1])
    with col_f1:
        new_total_funds = st.number_input(
            "设置总仓位资金（元）", 
            min_value=1000.0, 
            value=st.session_state.total_funds, 
            step=1000.0,
            help="输入你分配给做T的总资金，用于计算仓位占比"
        )
    with col_f2:
        if st.button("💾 保存总仓位资金", type="primary"):
            st.session_state.total_funds = new_total_funds
            st.success(f"✅ 总仓位资金已更新为 {new_total_funds:,.0f} 元")
    
    pos_df = get_current_positions(df)
    if not pos_df.empty:
        total_position_value = pos_df["持仓金额"].sum()
        total_unrealized = pos_df["浮动盈亏"].sum()
        col_p1, col_p2, col_p3 = st.columns(3)
        col_p1.metric("总持仓市值", f"{total_position_value:,.2f} 元")
        col_p2.metric("总浮动盈亏", f"{total_unrealized:,.2f} 元", delta=f"{total_unrealized:,.2f}")
        col_p3.metric("总仓位占比", f"{(total_position_value / st.session_state.total_funds * 100):.1f}%")
        st.dataframe(pos_df, use_container_width=True, hide_index=True)
    else:
        st.info("当前没有持仓～（仅买入/仅卖出记录才会产生仓位）")
    
    if len(df) == 0:
        st.warning("还没有数据，请先去「新增交易」页面记录")
    else:
        complete_df = df[df["交易类型"] == "完整做T (买+卖)"] if "交易类型" in df.columns else df
        total_net = complete_df["净利润"].sum()
        total_trades = len(df)
        win_trades = len(complete_df[complete_df["净利润"] > 0])
        win_rate = (win_trades / len(complete_df) * 100) if len(complete_df) > 0 else 0
        avg_profit = complete_df["净利润"].mean() if not complete_df.empty else 0
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("总净利润", f"{total_net:,.2f} 元")
        col2.metric("交易次数", total_trades)
        col3.metric("胜率", f"{win_rate:.1f}%")
        col4.metric("单笔平均收益", f"{avg_profit:,.2f} 元")
        df_sorted = complete_df.sort_values("交易日期")
        df_sorted["累计收益"] = df_sorted["净利润"].cumsum()
        fig = px.line(df_sorted, x="交易日期", y="累计收益", 
                      title="做T 累计收益曲线（权益曲线）", markers=True)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        win_loss = complete_df["净利润"].apply(lambda x: "盈利" if x > 0 else "亏损")
        fig_pie = px.pie(values=win_loss.value_counts().values, 
                         names=win_loss.value_counts().index,
                         title="盈利 vs 亏损分布")
        st.plotly_chart(fig_pie, use_container_width=True)
        st.dataframe(
            df[["交易日期", "交易类型", "股票代码", "买入价格", "卖出价格", "股数", "净利润", "备注", "交易金额"]].sort_values("交易日期", ascending=False),
            use_container_width=True
        )

with tab4:
    st.subheader("📈 近3个月K线图（量价结合）")
    if len(df) == 0:
        st.info("请先在 tab1 添加交易记录后再查看K线图")
    elif not st.session_state.get("tushare_token", "").strip():
        st.warning("👈 请在左侧边栏输入并保存 Tushare Token")
    else:
        unique_stocks = sorted(df["股票代码"].unique())
        default_index = 0
        if st.session_state.last_stock_code and st.session_state.last_stock_code in unique_stocks:
            default_index = unique_stocks.index(st.session_state.last_stock_code)
        
        selected_stock = st.selectbox(
            "选择要查看的股票（tab1 最近输入的已自动选中）", 
            unique_stocks, 
            index=default_index
        )
        
        if st.button("🔄 生成/刷新K线图", type="primary"):
            plot_kline_with_trades(selected_stock, df)

st.caption("💡 小贴士：v5.2 已新增「交易金额」列（自动计算当前交易总金额），表格编辑后实时更新。所有原有功能完整保留，有其他需求随时告诉我！")