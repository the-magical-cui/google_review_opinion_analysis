from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

plt.rcParams["font.sans-serif"] = [
    "Microsoft JhengHei",
    "PingFang TC",
    "Noto Sans CJK TC",
    "Arial Unicode MS",
    "sans-serif",
]
plt.rcParams["axes.unicode_minus"] = False


def save_bar_chart(df: pd.DataFrame, *, x: str, y: str, title: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(10, 4.5))
    plt.bar(df[x].astype(str), df[y])
    plt.xticks(rotation=45, ha="right")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def save_line_chart(df: pd.DataFrame, *, x: str, y: str, title: str, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(10, 4.5))
    plt.plot(df[x].astype(str), df[y], marker="o")
    plt.xticks(rotation=45, ha="right")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def save_multi_line_chart(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    series: str,
    title: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(10, 4.8))
    for series_name, subset in df.groupby(series):
        plt.plot(subset[x].astype(str), subset[y], marker="o", label=str(series_name))
    plt.xticks(rotation=45, ha="right")
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def save_horizontal_bar_chart(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plt.figure(figsize=(9, max(4.0, 0.55 * len(df))))
    plt.barh(df[y].astype(str), df[x])
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def save_scatter_with_regression_line(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    clean_df = df[[x, y]].dropna().copy()
    if clean_df.empty:
        return

    plt.figure(figsize=(7.2, 5.2))
    plt.scatter(clean_df[x], clean_df[y], alpha=0.45)
    if len(clean_df) >= 2:
        coeffs = pd.Series(clean_df[y]).astype(float)
        slope, intercept = pd.Series(clean_df[x]).astype(float).corr(coeffs), None
        poly = None
        try:
            import numpy as np

            poly = np.polyfit(clean_df[x].astype(float), clean_df[y].astype(float), 1)
        except Exception:
            poly = None
        if poly is not None:
            x_values = clean_df[x].astype(float).sort_values()
            y_values = poly[0] * x_values + poly[1]
            plt.plot(x_values, y_values, color="#d62728", linewidth=2)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.title(title)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def save_sentiment_distribution_chart(
    df: pd.DataFrame,
    *,
    label_col: str,
    positive_col: str,
    neutral_col: str,
    negative_col: str,
    title: str,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    plot_df = df[[label_col, positive_col, neutral_col, negative_col]].copy()
    plot_df = plot_df.sort_values(positive_col, ascending=False).reset_index(drop=True)

    plt.figure(figsize=(10, max(4.0, 0.8 * len(plot_df))))
    y_pos = np.arange(len(plot_df))
    positive = plot_df[positive_col].astype(float).to_numpy()
    neutral = plot_df[neutral_col].astype(float).to_numpy()
    negative = plot_df[negative_col].astype(float).to_numpy()

    plt.barh(y_pos, positive, color="#2E8B57", label="正向")
    plt.barh(y_pos, neutral, left=positive, color="#B0B0B0", label="中性")
    plt.barh(y_pos, negative, left=positive + neutral, color="#D9534F", label="負向")

    plt.yticks(y_pos, plot_df[label_col].astype(str))
    plt.xlim(0, 1)
    ticks = np.linspace(0, 1, 6)
    plt.xticks(ticks, [f"{int(value * 100)}%" for value in ticks])
    plt.xlabel("比例")
    plt.title(title)
    plt.legend()
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def save_common_aspect_grid(
    df: pd.DataFrame,
    *,
    aspect_col: str,
    series_col: str,
    value_col: str,
    title: str,
    output_path: Path,
    top_n: int = 6,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if df.empty:
        return

    top_aspects = (
        df.groupby(aspect_col)["mention_count"]
        .sum()
        .sort_values(ascending=False)
        .head(top_n)
        .index.tolist()
    )
    plot_df = df[df[aspect_col].isin(top_aspects)].copy()
    if plot_df.empty:
        return

    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    axes = axes.flatten()
    colors = ["#355C7D", "#6C9BD2", "#E84A5F"]

    for index, aspect_name in enumerate(top_aspects):
        ax = axes[index]
        aspect_slice = plot_df[plot_df[aspect_col] == aspect_name].copy()
        ax.bar(
            aspect_slice[series_col].astype(str),
            aspect_slice[value_col].astype(float),
            color=colors[: len(aspect_slice)],
        )
        ax.set_title(str(aspect_name))
        ax.tick_params(axis="x", rotation=20)
        ax.set_ylabel(value_col)

    for index in range(len(top_aspects), len(axes)):
        axes[index].axis("off")

    fig.suptitle(title)
    fig.tight_layout()
    fig.subplots_adjust(top=0.9)
    fig.savefig(output_path, dpi=150)
    plt.close(fig)
