from __future__ import annotations

import html
import sys
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from google_place_review.app_queries import PlaceSummary, ReviewFilters, ReviewQueryService


DB_PATH = Path("data/processed/google_place_reviews.db")
PAGE_SIZE = 20
MAX_VISIBLE_PAGE_NUMBERS = 7
GREEN = "#2E8B57"
RED = "#D14B4B"
GRAY = "#D1D5DB"
STAR_COLORS = ["#D73027", "#FC8D59", "#FEE08B", "#91CF60", "#4575B4"]


@st.cache_resource
def get_query_service() -> ReviewQueryService:
    return ReviewQueryService(DB_PATH)


def main() -> None:
    st.set_page_config(page_title="Google Place Reviews Explorer", layout="wide")
    apply_page_style()
    st.title("Google Place Reviews Explorer")

    if not DB_PATH.exists():
        st.error(f"找不到 SQLite 資料庫：{DB_PATH}")
        return

    query_service = get_query_service()
    places = query_service.get_available_places()
    if not places:
        st.warning("目前沒有可顯示的店家資料。")
        return

    selected_place = render_global_place_selector(query_service)
    if selected_place is None:
        st.warning("請先選擇要查看的店家。")
        return

    summary = query_service.get_place_summary(selected_place.place_id)
    if summary is None:
        st.error("無法讀取這家店的摘要資訊。")
        return

    render_place_summary(summary)
    render_sidebar_charts(query_service, summary)

    reviews_tab, analysis_tab = st.tabs(["Reviews Explorer", "Opinion Analysis"])
    with reviews_tab:
        render_reviews_list(query_service, summary.place_id)
    with analysis_tab:
        analysis_mode = st.selectbox(
            "分析模式",
            options=["單店意見分析", "跨店意見分析"],
            index=0,
            key="analysis_mode_select",
        )
        if analysis_mode == "單店意見分析":
            render_single_store_analysis(query_service, summary)
        else:
            render_cross_store_analysis(query_service, summary)


def apply_page_style() -> None:
    st.markdown(
        """
        <style>
        section[data-testid="stSidebar"] { background: #f3f4f7; }
        .review-card {
            border: 1px solid #e4e7eb;
            border-radius: 12px;
            padding: 1rem 1rem 0.85rem 1rem;
            margin-bottom: 0.9rem;
            background: white;
        }
        .review-card-title { font-weight: 700; margin-bottom: 0.2rem; }
        .review-card-meta { color: #6b7280; font-size: 0.92rem; margin-bottom: 0.6rem; }
        .owner-reply {
            border-left: 4px solid #9ca3af;
            padding-left: 0.8rem;
            margin-top: 0.9rem;
            color: #374151;
        }
        .pagination-note { text-align: center; color: #6b7280; margin-top: 0.4rem; }
        .scroll-review-box {
            max-height: 480px;
            overflow-y: auto;
            padding: 0.25rem 0.5rem 0.25rem 0;
            margin: 1rem 0 2rem 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_global_place_selector(query_service: ReviewQueryService):
    search_keyword = st.text_input(
        "搜尋地點",
        placeholder="輸入店名或 place_id，例如 Rebirth",
        key="global_place_search",
    )
    options = query_service.search_places(search_keyword)
    if not options:
        return None

    labels = [option.label for option in options]
    selected_place_id = st.session_state.get("selected_place_id")
    default_index = 0
    if selected_place_id:
        for idx, option in enumerate(options):
            if option.place_id == selected_place_id:
                default_index = idx
                break

    selected_label = st.selectbox("建議查詢地點", labels, index=default_index, key="global_place_select")
    selected_place = next(option for option in options if option.label == selected_label)
    st.session_state["selected_place_id"] = selected_place.place_id
    return selected_place


def render_place_summary(summary: PlaceSummary) -> None:
    left, right = st.columns([3, 1])
    with left:
        if summary.source_url:
            st.markdown(f"[{summary.place_name}_google review]({summary.source_url})")
        else:
            st.write(summary.place_name)
    with right:
        st.metric("總評論筆數", summary.total_reviews)

    info_left, info_right = st.columns(2)
    with info_left:
        st.caption(f"目前店家：{summary.place_name}")
    with info_right:
        st.caption(f"最近一次 run：{summary.latest_scrape_run_id or 'N/A'}")


def render_sidebar_charts(query_service: ReviewQueryService, summary: PlaceSummary) -> None:
    st.sidebar.markdown(f"### 目前店家：{summary.place_name}")
    chart_options = [
        "近一年每月評論數",
        "近一年每月平均星數",
        "近一年前一個完整月份星數分布",
        "每年評論數",
        "每年平均星數",
        "每年星數分布",
    ]
    selected = st.sidebar.multiselect(
        "選擇其他統計圖",
        options=chart_options,
        default=[],
        placeholder="選擇其他統計圖",
        key="selected_extra_charts",
    )
    charts_to_render = selected or ["近一年每月評論數", "近一年每月平均星數"]
    for chart_name in charts_to_render:
        st.sidebar.subheader(chart_name)
        if chart_name == "近一年每月評論數":
            render_sidebar_bar_chart(query_service.get_monthly_review_counts(summary.place_id, months=12), x="period", y="review_count", y_title="評論數")
        elif chart_name == "近一年每月平均星數":
            render_sidebar_line_chart(query_service.get_monthly_avg_stars(summary.place_id, months=12), x="period", y="avg_star", y_title="平均星數")
        elif chart_name == "近一年前一個完整月份星數分布":
            render_sidebar_bar_chart(query_service.get_previous_month_star_distribution(summary.place_id), x="star_rating", y="review_count", y_title="評論數")
        elif chart_name == "每年評論數":
            render_sidebar_bar_chart(query_service.get_yearly_review_counts(summary.place_id), x="period", y="review_count", y_title="評論數")
        elif chart_name == "每年平均星數":
            render_sidebar_line_chart(query_service.get_yearly_avg_stars(summary.place_id), x="period", y="avg_star", y_title="平均星數")
        elif chart_name == "每年星數分布":
            render_sidebar_stacked_star_chart(query_service.get_yearly_star_distribution(summary.place_id), x="period", y="review_count", color="star_rating", x_title=None, height=240)


def render_reviews_list(query_service: ReviewQueryService, place_id: str) -> None:
    st.subheader("全部評論")
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        likes_sort_desc = st.checkbox("按其他用戶按讚數降序", value=False)
        only_owner_response = st.checkbox("只顯示有店家回覆", value=False)
    with filter_col2:
        likes_min_text = st.text_input("其他用戶按讚數 >=", value="", placeholder="請輸入任意整數")
        time_sort_desc = st.checkbox("按時間降序", value=True)
    with filter_col3:
        star_ratings = st.multiselect("按星數篩選", options=[1, 2, 3, 4, 5], default=[])

    likes_min = parse_optional_int(likes_min_text)
    filters = ReviewFilters(
        likes_sort_desc=likes_sort_desc,
        likes_min=likes_min,
        only_owner_response=only_owner_response,
        time_sort_desc=time_sort_desc,
        star_ratings=tuple(star_ratings),
    )

    filter_signature = (place_id, likes_sort_desc, likes_min, only_owner_response, time_sort_desc, tuple(star_ratings))
    if st.session_state.get("review_filter_signature") != filter_signature:
        st.session_state["review_filter_signature"] = filter_signature
        st.session_state["review_page"] = 1

    page = st.session_state.get("review_page", 1)
    review_page = query_service.get_reviews_page(place_id, filters=filters, page=page, page_size=PAGE_SIZE)
    st.caption(f"目前條件下共有 {review_page.total_count} 則評論")
    if not review_page.rows:
        st.info("目前沒有符合篩選條件的評論。")
        return

    for row in review_page.rows:
        render_review_card(row)
    render_horizontal_pagination(review_page.total_pages, review_page.page)


def render_review_card(row) -> None:
    title_parts = [row.reviewer_name or "匿名使用者"]
    if row.star_rating is not None:
        title_parts.append(f"{row.star_rating:.1f}★")

    meta_parts = []
    if row.review_date_text:
        meta_parts.append(row.review_date_text)
    likes_count = int(row.likes_count or 0) if row.likes_count is not None else 0
    if likes_count:
        meta_parts.append(f"其他用戶按讚 {likes_count}")

    st.markdown('<div class="review-card">', unsafe_allow_html=True)
    st.markdown(f'<div class="review-card-title">{" | ".join(title_parts)}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="review-card-meta">{" | ".join(meta_parts) if meta_parts else "沒有額外資訊"}</div>', unsafe_allow_html=True)
    st.write(row.review_text or "目前沒有評論文字。")
    if row.has_owner_response and row.owner_response_text:
        st.markdown('<div class="owner-reply">', unsafe_allow_html=True)
        st.markdown("**店家回覆**")
        if row.owner_response_date:
            st.caption(row.owner_response_date)
        st.write(row.owner_response_text)
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)


def render_single_store_analysis(query_service: ReviewQueryService, summary: PlaceSummary) -> None:
    sentiment_summary = query_service.get_single_store_sentiment_summary(summary.place_id)
    monthly_metrics = query_service.get_single_store_monthly_metrics(summary.place_id)
    yearly_metrics = query_service.get_single_store_yearly_metrics(summary.place_id)
    aspect_summary = query_service.get_single_store_aspect_summary(summary.place_id)
    lexical_terms = query_service.get_single_store_lexical_terms(summary.place_id, limit=15)
    collocations = query_service.get_single_store_collocations(summary.place_id, limit=15)

    st.markdown(f"### {summary.place_name} 單店意見分析")
    st.info("目前為第一版規則法分析，適合做趨勢比較與 sanity check，不建議把單一分數視為最終真值。")

    if not sentiment_summary.empty:
        row = sentiment_summary.iloc[0]
        metric_labels = [
            ("分析評論數", str(int(row["review_count"])), None),
            ("平均 sentiment", f"{normalize_sentiment_10(row['avg_sentiment_score']):.1f}/10", None),
            ("平均星數", f"{float(row.get('avg_star_rating', 0)):.1f}", None),
            ("平均每人消費", "N/A" if pd.isna(row.get("avg_spend_amount")) else f"{float(row['avg_spend_amount']):.1f}", f"可解析樣本數：{int(row.get('spend_sample_count', 0) or 0)}"),
            ("正向比例", f"{float(row.get('positive_ratio', 0)) * 100:.1f}%", None),
            ("餐點評分", "N/A" if pd.isna(row.get("avg_google_food_rating")) else f"{float(row['avg_google_food_rating']):.1f}/5", f"樣本數：{int(row.get('google_food_rating_count', 0) or 0)}"),
            ("服務評分", "N/A" if pd.isna(row.get("avg_google_service_rating")) else f"{float(row['avg_google_service_rating']):.1f}/5", f"樣本數：{int(row.get('google_service_rating_count', 0) or 0)}"),
            ("氣氛評分", "N/A" if pd.isna(row.get("avg_google_atmosphere_rating")) else f"{float(row['avg_google_atmosphere_rating']):.1f}/5", f"樣本數：{int(row.get('google_atmosphere_rating_count', 0) or 0)}"),
        ]
        for row_start in range(0, len(metric_labels), 4):
            metric_cols = st.columns(4)
            for col, (label, value, help_text) in zip(metric_cols, metric_labels[row_start : row_start + 4]):
                col.metric(label, value, help=help_text)

    st.markdown("#### 情緒與時間")
    if monthly_metrics.empty:
        st.info("目前沒有足夠的時間序列資料。")
    else:
        monthly_metrics = monthly_metrics.copy()
        monthly_metrics["avg_sentiment_10"] = monthly_metrics["avg_sentiment_score"].apply(normalize_sentiment_10)
        chart_cols = st.columns(3)
        chart_cols[0].altair_chart(build_simple_bar_chart(monthly_metrics, x="year_month", y="review_count", y_title="評論數", tooltip_cols=["year_month", "review_count"], height=260), use_container_width=True)
        chart_cols[1].altair_chart(build_simple_line_chart(monthly_metrics, x="year_month", y="avg_star_rating", y_title="平均星數", tooltip_cols=["year_month", "avg_star_rating"], height=260), use_container_width=True)
        chart_cols[2].altair_chart(build_simple_line_chart(monthly_metrics, x="year_month", y="avg_sentiment_10", y_title="平均 sentiment (/10)", tooltip_cols=["year_month", "avg_sentiment_10"], height=260), use_container_width=True)
        with st.expander("查看時間序列資料表", expanded=False):
            yearly_display = yearly_metrics.copy()
            if not yearly_display.empty:
                yearly_display["avg_sentiment_10"] = yearly_display["avg_sentiment_score"].apply(normalize_sentiment_10)
                yearly_display = reorder_columns(
                    yearly_display.drop(columns=[col for col in ["place_id", "avg_sentiment_score"] if col in yearly_display.columns]).rename(
                        columns={"year": "年份", "review_count": "評論數", "avg_star_rating": "平均星數", "avg_sentiment_10": "平均 sentiment (/10)", "avg_spend_amount": "平均每人消費", "spend_sample_count": "消費樣本數"}
                    ),
                    preferred=["年份", "評論數", "平均星數", "平均 sentiment (/10)", "平均每人消費", "消費樣本數"],
                )
                st.dataframe(yearly_display, use_container_width=True, hide_index=True)

    st.markdown("#### Aspect 分析")
    if aspect_summary.empty:
        st.info("目前沒有可顯示的 aspect 分析結果。")
    else:
        aspect_chart_df = aspect_summary.copy()
        aspect_event = st.altair_chart(build_single_store_aspect_chart(aspect_chart_df), use_container_width=True, on_select="rerun", selection_mode=["single_store_aspect_pick"])
        selected_aspect = extract_selection_value(aspect_event, "aspect_name")
        if selected_aspect:
            render_aspect_review_matches(query_service.get_single_store_aspect_mentions(summary.place_id, selected_aspect), heading=f"Sanity Check：{selected_aspect}", box_key=f"single_aspect_{summary.place_id}_{selected_aspect}")
        else:
            st.info("點上方長條即可查看該面向對應的原始評論。")
        st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

        render_diverging_ratio_chart(
            aspect_chart_df,
            category_col="aspect_name",
            positive_ratio_col="positive_ratio",
            neutral_ratio_col="neutral_ratio",
            negative_ratio_col="negative_ratio",
            positive_count_col="positive_mentions",
            neutral_count_col="neutral_mentions",
            negative_count_col="negative_mentions",
            title="Aspect 正負向分布",
            display_label_col="aspect_name",
        )
        with st.expander("查看 aspect 資料表", expanded=False):
            aspect_display = reorder_columns(
                aspect_chart_df.drop(columns=[col for col in ["place_id"] if col in aspect_chart_df.columns]).rename(
                    columns={"aspect_name": "面向", "mention_count": "提及次數", "avg_aspect_sentiment_score": "平均面向 sentiment", "positive_mentions": "正向提及數", "neutral_mentions": "中性提及數", "negative_mentions": "負向提及數"}
                ),
                preferred=["面向", "提及次數", "平均面向 sentiment", "正向提及數", "中性提及數", "負向提及數"],
            )
            st.dataframe(aspect_display, use_container_width=True, hide_index=True)

    st.markdown("#### 詞頻分析")
    if lexical_terms.empty:
        st.info("目前沒有可顯示的詞頻結果。")
    else:
        lexical_display = reorder_columns(lexical_terms.drop(columns=[col for col in ["place_id"] if col in lexical_terms.columns]).rename(columns={"rank": "排名", "term": "關鍵詞", "term_count": "詞頻"}), preferred=["排名", "關鍵詞", "詞頻"])
        st.altair_chart(
            alt.Chart(lexical_display).mark_bar().encode(
                x=alt.X("詞頻:Q", title="詞頻"),
                y=alt.Y("關鍵詞:N", sort="-x", title=None, axis=alt.Axis(labelLimit=1000)),
                tooltip=list(lexical_display.columns),
            ).properties(height=max(360, len(lexical_display) * 30)),
            use_container_width=True,
        )
        with st.expander("查看詞頻資料表", expanded=False):
            st.dataframe(lexical_display, use_container_width=True, hide_index=True)

    st.markdown("#### Collocation / 關鍵詞語境情緒")
    if collocations.empty:
        st.info("目前沒有可顯示的 collocation 結果。")
    else:
        render_diverging_ratio_chart(
            collocations,
            category_col="term",
            positive_ratio_col="positive_ratio",
            neutral_ratio_col=None,
            negative_ratio_col="negative_ratio",
            positive_count_col="positive_context_count",
            neutral_count_col=None,
            negative_count_col="negative_context_count",
            title="關鍵詞語境正負向分布",
            display_label_col="term",
        )
        with st.expander("查看 collocation 資料表", expanded=False):
            collocation_display = reorder_columns(
                collocations.drop(columns=[col for col in ["place_id"] if col in collocations.columns]).rename(
                    columns={"rank": "排名", "term": "關鍵詞", "mention_count": "語境命中次數", "avg_context_sentiment": "平均語境 sentiment", "positive_context_count": "正向語境數", "negative_context_count": "負向語境數", "top_positive_review_text": "正向代表原文", "top_negative_review_text": "負向代表原文"}
                ),
                preferred=["排名", "關鍵詞", "語境命中次數", "平均語境 sentiment", "正向語境數", "負向語境數", "正向代表原文", "負向代表原文"],
            )
            st.dataframe(collocation_display, use_container_width=True, hide_index=True)


def render_cross_store_analysis(query_service: ReviewQueryService, summary: PlaceSummary) -> None:
    sentiment_comparison = query_service.get_cross_store_sentiment_comparison()
    aspect_comparison = query_service.get_cross_store_aspect_comparison(min_mentions=10)
    tfidf_terms = query_service.get_cross_store_tfidf_terms(top_n=10)
    monthly_metrics = query_service.get_cross_store_monthly_metrics()
    yearly_metrics = query_service.get_cross_store_yearly_metrics()
    cross_store_star_distribution = query_service.get_cross_store_star_distribution()

    st.markdown(f"### 跨店意見分析（目前查看店家：{summary.place_name}）")
    st.info("跨店分析目前以規則式 sentiment、aspect 與 TF-IDF 特色詞為主，適合做比較與 sanity check。")

    st.markdown("#### 跨店時間趨勢")
    cross_chart_options = ["跨店每月評論數", "跨店每月平均星數", "跨店每月平均 sentiment (/10)", "跨店每年評論數", "跨店每年平均星數", "跨店每年平均 sentiment (/10)", "跨店星數分布"]
    selected_time_charts = st.multiselect("選擇跨店時間圖", options=cross_chart_options, default=[], key="cross_store_time_chart_select")
    charts_to_render = selected_time_charts or ["跨店每月評論數", "跨店每月平均星數"]
    render_cross_store_time_charts(charts_to_render, monthly_metrics, yearly_metrics, cross_store_star_distribution)

    st.markdown("#### 跨店 sentiment 比較")
    if sentiment_comparison.empty:
        st.info("目前沒有可顯示的跨店 sentiment 比較結果。")
    else:
        sentiment_comparison = sentiment_comparison.copy()
        sentiment_comparison["avg_sentiment_10"] = sentiment_comparison["avg_sentiment_score"].apply(normalize_sentiment_10)
        metric_cols = st.columns(len(sentiment_comparison))
        for idx, row in enumerate(sentiment_comparison.itertuples(index=False)):
            metric_cols[idx].metric(getattr(row, "place_name"), f"{normalize_sentiment_10(getattr(row, 'avg_sentiment_score')):.1f}/10", f"正向 {getattr(row, 'positive_ratio') * 100:.1f}%")
        render_sentiment_100_stacked_chart(sentiment_comparison, title="跨店 sentiment 分布")
        with st.expander("查看跨店 sentiment 資料表", expanded=False):
            sentiment_display = reorder_columns(
                sentiment_comparison.drop(columns=[col for col in ["place_id", "avg_sentiment_score"] if col in sentiment_comparison.columns]).rename(
                    columns={"place_name": "店家", "review_count": "評論數", "avg_sentiment_10": "平均 sentiment (/10)", "avg_star_rating": "平均星數", "avg_spend_amount": "平均每人消費", "spend_sample_count": "消費樣本數", "avg_google_food_rating": "平均餐點評分", "avg_google_service_rating": "平均服務評分", "avg_google_atmosphere_rating": "平均氣氛評分"}
                ),
                preferred=["店家", "評論數", "平均 sentiment (/10)", "平均星數", "平均每人消費", "消費樣本數", "平均餐點評分", "平均服務評分", "平均氣氛評分"],
            )
            st.dataframe(sentiment_display, use_container_width=True, hide_index=True)

    st.markdown("#### 共同面向分析")
    if aspect_comparison.empty:
        st.info("目前沒有足夠的跨店共同面向資料。")
    else:
        focus_aspects = aspect_comparison.groupby("aspect_name")["mention_count"].sum().sort_values(ascending=False).head(6).index.tolist()
        filtered_aspects = aspect_comparison[aspect_comparison["aspect_name"].isin(focus_aspects)].copy()
        st.altair_chart(
            build_cross_store_common_aspect_distribution_chart(filtered_aspects),
            use_container_width=True,
        )
        st.markdown("<div style='height: 1.25rem;'></div>", unsafe_allow_html=True)

        selector_cols = st.columns(2)
        available_aspects = focus_aspects
        available_place_names = filtered_aspects["place_name"].drop_duplicates().tolist()
        selected_aspect = selector_cols[0].selectbox(
            "選擇面向",
            options=available_aspects,
            index=0 if available_aspects else None,
            key="cross_store_common_aspect_selector",
        )
        selected_place_name = selector_cols[1].selectbox(
            "選擇店家",
            options=available_place_names,
            index=0 if available_place_names else None,
            key="cross_store_common_place_selector",
        )
        if selected_aspect and selected_place_name:
            place_id = resolve_place_id_from_name(query_service, selected_place_name)
            if place_id:
                render_aspect_review_matches(
                    query_service.get_single_store_aspect_mentions(place_id, selected_aspect),
                    heading=f"Sanity Check：{selected_place_name} / {selected_aspect}",
                    box_key=f"cross_aspect_{place_id}_{selected_aspect}",
                )
        st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)

        render_diverging_ratio_chart(
            filtered_aspects.assign(display_label=filtered_aspects["place_name"] + "｜" + filtered_aspects["aspect_name"]),
            category_col="display_label",
            positive_ratio_col="positive_ratio",
            neutral_ratio_col="neutral_ratio",
            negative_ratio_col="negative_ratio",
            positive_count_col="positive_mentions",
            neutral_count_col="neutral_mentions",
            negative_count_col="negative_mentions",
            title="共同面向正負向分布",
            display_label_col="display_label",
        )
        with st.expander("查看共同面向資料表", expanded=False):
            aspect_display = reorder_columns(
                filtered_aspects.drop(columns=[col for col in ["place_id"] if col in filtered_aspects.columns]).rename(
                    columns={"place_name": "店家", "aspect_name": "面向", "mention_count": "提及次數", "avg_aspect_sentiment_score": "平均面向 sentiment", "positive_mentions": "正向提及數", "neutral_mentions": "中性提及數", "negative_mentions": "負向提及數"}
                ),
                preferred=["店家", "面向", "提及次數", "平均面向 sentiment", "正向提及數", "中性提及數", "負向提及數"],
            )
            st.dataframe(aspect_display, use_container_width=True, hide_index=True)

    st.markdown("#### 跨店特色詞")
    if tfidf_terms.empty:
        st.info("目前沒有可顯示的 TF-IDF 特色詞。")
    else:
        place_names = tfidf_terms["place_name"].drop_duplicates().tolist()
        columns = st.columns(max(1, len(place_names)))
        for col, place_name in zip(columns, place_names):
            with col:
                st.markdown(f"**{place_name}**")
                place_terms = reorder_columns(
                    tfidf_terms[tfidf_terms["place_name"] == place_name].drop(columns=[column for column in ["place_id", "place_name"] if column in tfidf_terms.columns]).rename(columns={"rank": "排名", "term": "特色詞", "tfidf_score": "TF-IDF 分數"}),
                    preferred=["排名", "特色詞", "TF-IDF 分數"],
                )
                if "TF-IDF 分數" in place_terms.columns:
                    place_terms["TF-IDF 分數"] = place_terms["TF-IDF 分數"].map(lambda value: round(float(value), 4))
                st.dataframe(place_terms, use_container_width=True, hide_index=True)


def render_cross_store_time_charts(chart_names: list[str], monthly_metrics: pd.DataFrame, yearly_metrics: pd.DataFrame, star_distribution: pd.DataFrame) -> None:
    if not monthly_metrics.empty:
        monthly_metrics = monthly_metrics.copy()
        monthly_metrics["avg_sentiment_10"] = monthly_metrics["avg_sentiment_score"].apply(normalize_sentiment_10)
    if not yearly_metrics.empty:
        yearly_metrics = yearly_metrics.copy()
        yearly_metrics["avg_sentiment_10"] = yearly_metrics["avg_sentiment_score"].apply(normalize_sentiment_10)

    for chart_name in chart_names:
        st.markdown(f"##### {chart_name}")
        if chart_name == "跨店每月評論數":
            render_line_or_info(monthly_metrics, x="year_month", y="review_count", color="place_name", y_title="評論數", tooltip_cols=["place_name", "year_month", "review_count"])
        elif chart_name == "跨店每月平均星數":
            render_line_or_info(monthly_metrics, x="year_month", y="avg_star_rating", color="place_name", y_title="平均星數", tooltip_cols=["place_name", "year_month", "avg_star_rating"])
        elif chart_name == "跨店每月平均 sentiment (/10)":
            render_line_or_info(monthly_metrics, x="year_month", y="avg_sentiment_10", color="place_name", y_title="平均 sentiment (/10)", tooltip_cols=["place_name", "year_month", "avg_sentiment_10"])
        elif chart_name == "跨店每年評論數":
            render_line_or_info(yearly_metrics, x="year", y="review_count", color="place_name", y_title="評論數", tooltip_cols=["place_name", "year", "review_count"])
        elif chart_name == "跨店每年平均星數":
            render_line_or_info(yearly_metrics, x="year", y="avg_star_rating", color="place_name", y_title="平均星數", tooltip_cols=["place_name", "year", "avg_star_rating"])
        elif chart_name == "跨店每年平均 sentiment (/10)":
            render_line_or_info(yearly_metrics, x="year", y="avg_sentiment_10", color="place_name", y_title="平均 sentiment (/10)", tooltip_cols=["place_name", "year", "avg_sentiment_10"])
        elif chart_name == "跨店星數分布":
            render_line_or_info(star_distribution, x="star_rating", y="review_count", color="place_name", y_title="評論數", tooltip_cols=["place_name", "star_rating", "review_count"])


def render_line_or_info(frame: pd.DataFrame, *, x: str, y: str, color: str, y_title: str, tooltip_cols: list[str]) -> None:
    if frame.empty:
        st.info("目前沒有足夠資料可顯示這張圖。")
        return
    st.altair_chart(build_multi_line_chart(frame, x=x, y=y, color=color, y_title=y_title, tooltip_cols=tooltip_cols), use_container_width=True)


def render_horizontal_pagination(total_pages: int, current_page: int) -> None:
    if total_pages <= 1:
        return
    start_page, end_page = get_visible_page_window(current_page, total_pages, MAX_VISIBLE_PAGE_NUMBERS)
    visible_pages = list(range(start_page, end_page + 1))
    cols = st.columns([1] + [0.45] * len(visible_pages) + [1])
    with cols[0]:
        if st.button("上一頁", disabled=current_page <= 1, key=f"prev_{current_page}"):
            st.session_state["review_page"] = current_page - 1
            st.rerun()
    for idx, page_number in enumerate(visible_pages, start=1):
        with cols[idx]:
            if page_number == current_page:
                st.markdown(f"<div style='text-align:center;padding-top:0.35rem;font-weight:700;'>{page_number}</div>", unsafe_allow_html=True)
            elif st.button(str(page_number), key=f"page_{page_number}_{current_page}"):
                st.session_state["review_page"] = page_number
                st.rerun()
    with cols[-1]:
        if st.button("下一頁", disabled=current_page >= total_pages, key=f"next_{current_page}"):
            st.session_state["review_page"] = current_page + 1
            st.rerun()
    st.markdown(f"<div class='pagination-note'>第 {current_page} / {total_pages} 頁</div>", unsafe_allow_html=True)


def get_visible_page_window(current_page: int, total_pages: int, max_visible: int) -> tuple[int, int]:
    if total_pages <= max_visible:
        return 1, total_pages
    half = max_visible // 2
    start_page = max(1, current_page - half)
    end_page = start_page + max_visible - 1
    if end_page > total_pages:
        end_page = total_pages
        start_page = end_page - max_visible + 1
    return start_page, end_page


def render_sidebar_bar_chart(frame: pd.DataFrame, *, x: str, y: str, y_title: str) -> None:
    if frame.empty:
        st.sidebar.info("目前沒有足夠資料可顯示。")
        return
    st.sidebar.altair_chart(build_simple_bar_chart(frame, x=x, y=y, y_title=y_title, tooltip_cols=list(frame.columns), height=220), use_container_width=True)


def render_sidebar_line_chart(frame: pd.DataFrame, *, x: str, y: str, y_title: str) -> None:
    if frame.empty:
        st.sidebar.info("目前沒有足夠資料可顯示。")
        return
    st.sidebar.altair_chart(build_simple_line_chart(frame, x=x, y=y, y_title=y_title, tooltip_cols=list(frame.columns), height=220), use_container_width=True)


def render_sidebar_stacked_star_chart(frame: pd.DataFrame, *, x: str, y: str, color: str, x_title: str | None, height: int) -> None:
    if frame.empty:
        st.sidebar.info("目前沒有足夠資料可顯示。")
        return
    st.sidebar.altair_chart(build_stacked_star_chart(frame, x=x, y=y, color=color, title=None, x_title=x_title, height=height), use_container_width=True)


def render_diverging_ratio_chart(
    frame: pd.DataFrame,
    *,
    category_col: str,
    positive_ratio_col: str,
    neutral_ratio_col: str | None,
    negative_ratio_col: str,
    positive_count_col: str,
    neutral_count_col: str | None,
    negative_count_col: str,
    title: str,
    display_label_col: str | None = None,
    height: int = 320,
) -> None:
    if frame.empty:
        return
    label_col = display_label_col or category_col
    rows: list[dict] = []
    for _, row in frame.iterrows():
        label = row.get(label_col, row[category_col])
        rows.append({"category": row[category_col], "display_label": label, "sentiment_band": "負向", "ratio_value": -float(row.get(negative_ratio_col, 0) or 0), "absolute_ratio": float(row.get(negative_ratio_col, 0) or 0), "count_value": int(row.get(negative_count_col, 0) or 0)})
        if neutral_ratio_col:
            rows.append({"category": row[category_col], "display_label": label, "sentiment_band": "中性", "ratio_value": float(row.get(neutral_ratio_col, 0) or 0), "absolute_ratio": float(row.get(neutral_ratio_col, 0) or 0), "count_value": int(row.get(neutral_count_col, 0) or 0)})
        rows.append({"category": row[category_col], "display_label": label, "sentiment_band": "正向", "ratio_value": float(row.get(positive_ratio_col, 0) or 0), "absolute_ratio": float(row.get(positive_ratio_col, 0) or 0), "count_value": int(row.get(positive_count_col, 0) or 0)})
    ratio_df = pd.DataFrame(rows)
    chart_height = max(height, ratio_df["display_label"].nunique() * 30)
    chart = (
        alt.Chart(ratio_df)
        .mark_bar()
        .encode(
            x=alt.X("ratio_value:Q", title="比例", axis=alt.Axis(format=".0%", labelExpr="abs(datum.value)")),
            y=alt.Y("display_label:N", sort=alt.EncodingSortField(field="category", op="min", order="ascending"), title=None, axis=alt.Axis(labelLimit=1000)),
            color=alt.Color("sentiment_band:N", scale=alt.Scale(domain=["負向", "中性", "正向"], range=[RED, GRAY, GREEN]), legend=alt.Legend(title="情緒")),
            tooltip=[alt.Tooltip("category:N", title="類別"), alt.Tooltip("sentiment_band:N", title="情緒"), alt.Tooltip("absolute_ratio:Q", title="比例", format=".1%"), alt.Tooltip("count_value:Q", title="數量")],
        )
        .properties(title=title, height=chart_height)
    )
    st.altair_chart(chart, use_container_width=True)


def render_sentiment_100_stacked_chart(frame: pd.DataFrame, *, title: str) -> None:
    if frame.empty:
        return
    rows: list[dict] = []
    for _, row in frame.iterrows():
        rows.extend(
            [
                {
                    "place_name": row["place_name"],
                    "sentiment_band": "正向",
                    "ratio": float(row.get("positive_ratio", 0) or 0),
                    "count": int(row.get("positive", 0) or 0),
                },
                {
                    "place_name": row["place_name"],
                    "sentiment_band": "中性",
                    "ratio": float(row.get("neutral_ratio", 0) or 0),
                    "count": int(row.get("neutral", 0) or 0),
                },
                {
                    "place_name": row["place_name"],
                    "sentiment_band": "負向",
                    "ratio": float(row.get("negative_ratio", 0) or 0),
                    "count": int(row.get("negative", 0) or 0),
                },
            ]
        )
    chart_df = pd.DataFrame(rows)
    chart = (
        alt.Chart(chart_df)
        .mark_bar()
        .encode(
            x=alt.X("ratio:Q", stack="normalize", title="比例", axis=alt.Axis(format=".0%")),
            y=alt.Y("place_name:N", title=None, axis=alt.Axis(labelLimit=1000)),
            color=alt.Color(
                "sentiment_band:N",
                title="情緒",
                scale=alt.Scale(domain=["正向", "中性", "負向"], range=[GREEN, GRAY, RED]),
                sort=["正向", "中性", "負向"],
            ),
            order=alt.Order("sentiment_band:N", sort="ascending"),
            tooltip=[
                alt.Tooltip("place_name:N", title="店家"),
                alt.Tooltip("sentiment_band:N", title="情緒"),
                alt.Tooltip("ratio:Q", title="比例", format=".1%"),
                alt.Tooltip("count:Q", title="數量"),
            ],
        )
        .properties(title=title, height=max(180, frame["place_name"].nunique() * 52))
    )
    st.altair_chart(chart, use_container_width=True)


def _encoding_type_for_series(series: pd.Series) -> str:
    if pd.api.types.is_numeric_dtype(series):
        return "Q"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "T"
    return "N"


def _tooltip_encodings(frame: pd.DataFrame, columns: list[str]) -> list[alt.Tooltip]:
    tooltips: list[alt.Tooltip] = []
    for column in columns:
        if column not in frame.columns:
            continue
        enc_type = _encoding_type_for_series(frame[column])
        tooltips.append(alt.Tooltip(f"{column}:{enc_type}", title=column))
    return tooltips


def build_simple_bar_chart(frame: pd.DataFrame, *, x: str, y: str, y_title: str, tooltip_cols: list[str], height: int = 240) -> alt.Chart:
    plot_df = frame.copy()
    x_type = _encoding_type_for_series(plot_df[x])
    y_type = _encoding_type_for_series(plot_df[y])
    return (
        alt.Chart(plot_df)
        .mark_bar()
        .encode(
            x=alt.X(f"{x}:{x_type}", sort=None, title=None),
            y=alt.Y(f"{y}:{y_type}", title=y_title),
            tooltip=_tooltip_encodings(plot_df, tooltip_cols),
        )
        .properties(height=height)
    )


def build_simple_line_chart(frame: pd.DataFrame, *, x: str, y: str, y_title: str, tooltip_cols: list[str], height: int = 240) -> alt.Chart:
    plot_df = frame.copy()
    x_type = _encoding_type_for_series(plot_df[x])
    y_type = _encoding_type_for_series(plot_df[y])
    return (
        alt.Chart(plot_df)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x}:{x_type}", sort=None, title=None),
            y=alt.Y(f"{y}:{y_type}", title=y_title),
            tooltip=_tooltip_encodings(plot_df, tooltip_cols),
        )
        .properties(height=height)
    )


def build_multi_line_chart(frame: pd.DataFrame, *, x: str, y: str, color: str, y_title: str, tooltip_cols: list[str], height: int = 260) -> alt.Chart:
    plot_df = frame.copy()
    x_type = _encoding_type_for_series(plot_df[x])
    y_type = _encoding_type_for_series(plot_df[y])
    return (
        alt.Chart(plot_df)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x}:{x_type}", sort=None, title=None),
            y=alt.Y(f"{y}:{y_type}", title=y_title),
            color=alt.Color(f"{color}:N", title="店家"),
            tooltip=_tooltip_encodings(plot_df, tooltip_cols),
        )
        .properties(height=height)
    )


def build_stacked_star_chart(frame: pd.DataFrame, *, x: str, y: str, color: str, title: str | None, x_title: str | None, height: int = 260) -> alt.Chart:
    domain = sorted(frame[color].dropna().astype(int).unique().tolist())
    return (
        alt.Chart(frame)
        .mark_bar()
        .encode(
            x=alt.X(x, sort=None, title=x_title),
            y=alt.Y(y, title="評論數"),
            color=alt.Color(f"{color}:N", title="星數", scale=alt.Scale(domain=[str(value) for value in domain], range=STAR_COLORS[: len(domain)])),
            tooltip=[alt.Tooltip(f"{x}:N"), alt.Tooltip(f"{color}:Q", title="星數"), alt.Tooltip(f"{y}:Q", title="評論數")],
        )
        .properties(title=title, height=height)
    )


def build_single_store_aspect_chart(frame: pd.DataFrame) -> alt.Chart:
    selection = alt.selection_point(fields=["aspect_name"], name="single_store_aspect_pick")
    return (
        alt.Chart(frame)
        .mark_bar()
        .encode(
            x=alt.X("aspect_name:N", sort="-y", title=None),
            y=alt.Y("avg_aspect_sentiment_score:Q", title="平均面向 sentiment"),
            tooltip=[alt.Tooltip("aspect_name:N", title="面向"), alt.Tooltip("mention_count:Q", title="提及次數"), alt.Tooltip("avg_aspect_sentiment_score:Q", title="平均面向 sentiment", format=".3f"), alt.Tooltip("positive_ratio:Q", title="正向比例", format=".1%"), alt.Tooltip("negative_ratio:Q", title="負向比例", format=".1%")],
            opacity=alt.condition(selection, alt.value(1.0), alt.value(0.75)),
        )
        .add_params(selection)
        .properties(height=320)
    )


def build_cross_store_common_aspect_distribution_chart(frame: pd.DataFrame) -> alt.Chart:
    if frame.empty:
        return alt.Chart(pd.DataFrame({"x": [], "y": []})).mark_point()

    plot_rows: list[dict] = []
    for _, row in frame.iterrows():
        positive_ratio = float(row.get("positive_ratio", 0) or 0)
        negative_ratio = float(row.get("negative_ratio", 0) or 0)
        neutral_ratio = float(row.get("neutral_ratio", 0) or 0)
        plot_rows.append(
            {
                "aspect_name": row["aspect_name"],
                "place_name": row["place_name"],
                "sentiment_side": "正向",
                "ratio_signed": positive_ratio,
                "ratio_label": f"{positive_ratio * 100:.0f}%",
                "neutral_ratio": neutral_ratio,
                "mention_count": int(row.get("mention_count", 0) or 0),
                "avg_aspect_sentiment_score": float(row.get("avg_aspect_sentiment_score", 0) or 0),
                "positive_ratio": positive_ratio,
                "negative_ratio": negative_ratio,
            }
        )
        plot_rows.append(
            {
                "aspect_name": row["aspect_name"],
                "place_name": row["place_name"],
                "sentiment_side": "負向",
                "ratio_signed": -negative_ratio,
                "ratio_label": f"{negative_ratio * 100:.0f}%",
                "neutral_ratio": neutral_ratio,
                "mention_count": int(row.get("mention_count", 0) or 0),
                "avg_aspect_sentiment_score": float(row.get("avg_aspect_sentiment_score", 0) or 0),
                "positive_ratio": positive_ratio,
                "negative_ratio": negative_ratio,
            }
        )

    plot_df = pd.DataFrame(plot_rows)
    place_order = frame["place_name"].drop_duplicates().tolist()
    aspect_order = (
        frame.groupby("aspect_name")["mention_count"]
        .sum()
        .sort_values(ascending=False)
        .index.tolist()
    )

    base = alt.Chart(plot_df).encode(
        x=alt.X("place_name:N", title=None, sort=place_order, axis=alt.Axis(labelAngle=-90, labelLimit=1000)),
        y=alt.Y(
            "ratio_signed:Q",
            title="比例",
            scale=alt.Scale(domain=[-1, 1]),
            axis=alt.Axis(format=".0%"),
        ),
        tooltip=[
            alt.Tooltip("aspect_name:N", title="面向"),
            alt.Tooltip("place_name:N", title="店家"),
            alt.Tooltip("positive_ratio:Q", title="正向比例", format=".1%"),
            alt.Tooltip("neutral_ratio:Q", title="中性比例", format=".1%"),
            alt.Tooltip("negative_ratio:Q", title="負向比例", format=".1%"),
            alt.Tooltip("mention_count:Q", title="提及次數"),
            alt.Tooltip("avg_aspect_sentiment_score:Q", title="平均面向 sentiment", format=".3f"),
        ],
    )

    bars = base.mark_bar(size=20, stroke="#90A4AE", strokeWidth=1).encode(
        color=alt.Color(
            "sentiment_side:N",
            title="方向",
            scale=alt.Scale(domain=["正向", "負向"], range=["#B9F6CA", "#FFCDD2"]),
            legend=None,
        )
    )

    positive_text = (
        base.transform_filter(alt.datum.sentiment_side == "正向")
        .mark_text(dy=-8, fontSize=11, color="#2E7D32")
        .encode(text="ratio_label:N")
    )
    negative_text = (
        base.transform_filter(alt.datum.sentiment_side == "負向")
        .mark_text(dy=10, baseline="top", fontSize=11, color="#C62828")
        .encode(text="ratio_label:N")
    )

    chart = (
        alt.layer(bars, positive_text, negative_text)
        .facet(
            facet=alt.Facet("aspect_name:N", title=None, sort=aspect_order),
            columns=3,
        )
        .resolve_scale(y="shared")
        .properties(title="共同面向分析")
        .configure_view(stroke=None)
        .configure_facet(spacing=16)
        .configure_axis(labelColor="#4F5B72", titleColor="#4F5B72")
        .configure_header(
            labelFontSize=16,
            labelFontWeight="bold",
            labelColor="#253041",
            labelOrient="top",
            title=None,
        )
    )
    return chart


def extract_selection_value(event: object, field_name: str) -> str | None:
    if event is None:
        return None
    payload = event
    if hasattr(event, "selection"):
        payload = getattr(event, "selection")
    if isinstance(payload, dict) and "selection" in payload:
        payload = payload["selection"]

    def _search(node: object) -> str | None:
        if isinstance(node, dict):
            if field_name in node and node[field_name] not in (None, ""):
                return str(node[field_name])
            for value in node.values():
                result = _search(value)
                if result is not None:
                    return result
        elif isinstance(node, list):
            for item in node:
                result = _search(item)
                if result is not None:
                    return result
        return None

    return _search(payload)


def render_aspect_review_matches(frame: pd.DataFrame, *, heading: str, box_key: str) -> None:
    clean_heading = heading.replace("Sanity Check：", "").replace("Sanity Check:", "").strip()
    st.markdown(f"##### {clean_heading}")
    if frame.empty:
        st.info("目前沒有可顯示的原始評論。")
        return

    display = frame.copy().sort_values(["likes_count", "review_date_estimated"], ascending=[False, False])
    review_cards: list[str] = []
    for row in display.itertuples(index=False):
        reviewer_name = html.escape(str(getattr(row, "reviewer_name", "") or "匿名使用者"))
        star_rating = getattr(row, "star_rating", None)
        title = reviewer_name
        if pd.notna(star_rating):
            title += f" | {float(star_rating):.1f}★"
        meta_parts: list[str] = []
        review_date_text = str(getattr(row, "review_date_text", "") or "")
        if review_date_text:
            meta_parts.append(html.escape(review_date_text))
        likes = int(getattr(row, "likes_count", 0) or 0)
        if likes:
            meta_parts.append(f"其他用戶按讚 {likes}")
        review_text = html.escape(str(getattr(row, "review_text", "") or ""))
        owner_response_value = getattr(row, "owner_response_text", "")
        owner_reply = ""
        if pd.notna(owner_response_value):
            owner_reply = html.escape(str(owner_response_value or "").strip())
        card_html = ["<div class='review-card'>", f"<div class='review-card-title'>{title}</div>", f"<div class='review-card-meta'>{' | '.join(meta_parts) if meta_parts else '沒有額外資訊'}</div>", f"<div>{review_text}</div>"]
        if owner_reply:
            card_html.append("<div class='owner-reply'><strong>店家回覆</strong><br/>")
            card_html.append(owner_reply)
            card_html.append("</div>")
        card_html.append("</div>")
        review_cards.append("".join(card_html))
    container_html = f"<div id='{box_key}' class='scroll-review-box'>{''.join(review_cards)}</div>"
    st.markdown(container_html, unsafe_allow_html=True)


def reorder_columns(frame: pd.DataFrame, *, preferred: list[str]) -> pd.DataFrame:
    existing_preferred = [column for column in preferred if column in frame.columns]
    remaining = [column for column in frame.columns if column not in existing_preferred]
    return frame[existing_preferred + remaining]


def parse_optional_int(value: str) -> int | None:
    text = value.strip()
    if not text:
        return None
    try:
        return max(0, int(text))
    except ValueError:
        return None


def normalize_sentiment_10(value: float | int | None) -> float:
    if value is None or pd.isna(value):
        return 5.0
    normalized = float(value) + 5.0
    return max(0.0, min(10.0, normalized))


def resolve_place_id_from_name(query_service: ReviewQueryService, place_name: str) -> str | None:
    for place in query_service.get_available_places():
        if place.place_name == place_name:
            return place.place_id
    return None


if __name__ == "__main__":
    main()
