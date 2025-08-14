import pandas as pd
import matplotlib
matplotlib.use("Agg")  # 非交互式后端
import matplotlib.pyplot as plt
from matplotlib import rcParams, font_manager
from calculate_indicator import ComputeIndicator
import matplotlib.dates as mdates


def set_chinese_font():
    font_list = [
        "PingFang SC", "PingFang HK", "STHeiti",
        "Microsoft YaHei", "SimHei", "Heiti SC"
    ]
    available_fonts = set(f.name for f in font_manager.fontManager.ttflist)
    for f in font_list:
        if f in available_fonts:
            rcParams['font.sans-serif'] = [f]
            rcParams['axes.unicode_minus'] = False
            print(f"✅ 使用字体: {f}")
            return
    print("⚠ 未找到中文字体，可能会出现方块字")


def get_indicator_data(data, index):
    ci = ComputeIndicator()

    nav = (pd.Series(data, index=index, name='data') / 100 + 1).cumprod()
    nav.sort_index(inplace=True)
    nav_df = nav.to_frame()

    indicator_list = ['annualized_return_ratio', 'max_drawdown', 'max_drawdown_duration', 'sharpe_ratio', 'win_ratio',
                      'downside_vol',
                      'profit_loss_ratio', 'calmar_ratio', 'off_last_high']
    indicator_6 = nav_df.apply(ci.multi_indicator, indicator_list=indicator_list).T
    indicator_dict = indicator_6.to_dict(orient='index')
    return indicator_dict['data']


# ===== 图表绘制封装函数 =====
def plot_single_y(symbol_col, benchmark_col, metrics_data, title, output_file):
    fig, ax = plt.subplots(figsize=(12, 6))

    # 绘制主线和基准线
    ax.plot(df['date'], df[symbol_col], label=symbol_col)
    ax.plot(df['date'], df[benchmark_col], label=benchmark_col, linestyle="--", alpha=0.7)

    # 图例
    ax.legend(loc="upper left")
    ax.set_xlabel("日期")
    ax.set_ylabel(symbol_col)
    ax.set_title(title)
    ax.grid(True, linestyle="--", alpha=0.5)

    # 给右边留出空间
    x_min, x_max = ax.get_xlim()
    ax.set_xlim(x_min, x_max + (x_max - x_min) * 0.25)

    # ===== 在右侧空白加绩效指标 =====
    metrics_lines = [f"{k}：{v:.4f}" if isinstance(v, float) else f"{k}：{v}" for k, v in metrics_data.items()]

    metrics_text = "\n".join(metrics_lines)

    plt.gcf().text(
        0.76, 0.76, metrics_text, fontsize=9, va='center',
        bbox=dict(facecolor='white', alpha=0.8, edgecolor='black', boxstyle='round,pad=0.5')
    )

    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"📊 图表已保存到 {output_file}")


set_chinese_font()

excel_file = "./chart_task/账户情况.xlsx"
sheet_name = "Sheet2"

df = pd.read_excel(excel_file, sheet_name=sheet_name)
df.columns = df.columns.str.strip()

if df.empty:
    raise ValueError("❌ 读取的数据为空，请检查Excel文件和Sheet名称")

if 'date' not in df.columns:
    raise ValueError("❌ 缺少 'date' 列")
df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

# ===== 生成六张图 =====
symbols = ["1000", "2000", "3000"]
benchmark = "BTC"

# NAV 图
for sym in symbols:
    nav_col_1 = f"{sym}nav"
    nav_col_2 = f"{benchmark}nav"
    date_list = df[nav_col_1].tolist()
    index_list = df['date'].tolist()
    indicator_dict = get_indicator_data(date_list, index_list)

    plot_single_y(
        symbol_col=nav_col_1,
        benchmark_col=nav_col_2,
        metrics_data=indicator_dict,
        title=f"{sym} NAV vs {benchmark} NAV",
        output_file=f"./chart_task/nav_{sym}_vs_{benchmark}.png"
    )


# 绘制回撤图（单Y轴）
for sym in symbols:
    dd_col_1 = f"{sym}nav回撤"
    dd_col_2 = f"{benchmark}nav回撤"
    if dd_col_1 not in df.columns or dd_col_2 not in df.columns:
        print(f"⚠ 缺少 {sym} 或 {benchmark} 的 回撤 列，跳过")
        continue
    if df[dd_col_1].isna().all() and df[dd_col_2].isna().all():
        print(f"⚠ {sym} 和 {benchmark} 回撤数据均为空，跳过")
        continue

    plt.figure(figsize=(12,6))
    plt.plot(df['date'], df[dd_col_1], label=f"{sym} 回撤", color="tab:blue")
    plt.plot(df['date'], df[dd_col_2], label=f"{benchmark} 回撤", color="tab:orange", linestyle="--")
    plt.xlabel("日期", fontsize=12)
    plt.ylabel("回撤", fontsize=12)
    plt.title(f"{sym} 回撤 与 {benchmark} 回撤", fontsize=16)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend(loc="upper left")

    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.gcf().autofmt_xdate()

    output_img = f"./chart_task/drawdown_{sym}_vs_{benchmark}.png"
    plt.savefig(output_img, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"📊 图表已保存到 {output_img}")
