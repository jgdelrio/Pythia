from math import floor, pi
from bokeh.plotting import figure, show, output_file
from bokeh.io import output_notebook, push_notebook
import matplotlib.pyplot as plt
from matplotlib import colors

from src.utils import in_ipynb


TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
COLORS = ['#A6CEE3', '#B2DF8A', '#33A02C', '#FB9A99']


def colors_gen(n):
    if n <= 256:
        cm = plt.get_cmap("viridis")
        crange = len(cm.colors) - 1
        step = floor(crange/(n-1)) if n > 1 else 0
        pos = 0
        while pos < n:
            yield colors.to_hex(cm.colors[pos * step])
            pos += 1


def show_candlestick(df, title=None, save=False, width=None, height=None):
    is_notebook = in_ipynb()
    if is_notebook:
        output_notebook()
    if width is None:
        width = 700 if is_notebook else 1000
    if height is None:
        height = 350 if is_notebook else 500

    inc = df.close > df.open
    dec = df.open > df.close
    w = 12 * 60 * 60 * 1000  # half day in ms
    graph_title = title + ' Chart' if title is not None else 'Chart'

    p = figure(x_axis_type="datetime", tools=TOOLS, title=graph_title, plot_width=width, plot_height=height)
    p.xaxis.major_label_orientation = pi / 4
    p.grid.grid_line_alpha = 0.3
    p.xaxis.axis_label = 'Date'
    p.yaxis.axis_label = 'Price'

    p.segment(df.index, df.high, df.index, df.low, color="black")
    p.vbar(df.index[inc], w, df.open[inc], df.close[inc], fill_color='#B2DF8A', line_color="green")
    p.vbar(df.index[dec], w, df.open[dec], df.close[dec], fill_color="#FA8072", line_color="red")

    if save and not is_notebook:
        # Store as a HTML file
        output_file("stock_information.html", title="candlestick.py")
    # Display
    show(p)
    if is_notebook:
        push_notebook()


def show_stocklines(series, names, title=None, save=False):
    if not isinstance(series, list):
        raise TypeError("series must be a list")
    if not isinstance(names, list):
        raise TypeError("names must be a list")
    colors = colors_gen(len(series))
    graph_title = title + ' Chart' if title is not None else 'Chart'
    p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1000, title=graph_title)
    p.xaxis.major_label_orientation = pi / 4
    p.grid.grid_line_alpha = 0.3
    p.xaxis.axis_label = 'Date'
    p.yaxis.axis_label = 'Price'

    for val, name in zip(series, names):
        p.line(val.index, val.values, color=next(colors), legend_label=name)
    p.legend.location = "top_left"

    if in_ipynb():
        p.width, p.height = 750, 400
        output_notebook()
    elif save:
        # Store as a HTML file
        output_file("stock_lines.html", title="stocklines.py")
    show(p)


if __name__ == "__main__":
    cg = colors_gen(5)
