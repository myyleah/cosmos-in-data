"""
Cosmos in Data — NASA APOD Analytics Dashboard
================================================
MSBA 692: Pipeline to Insights

Interactive Plotly Dash dashboard that connects to Supabase PostgreSQL
and visualizes trends in NASA's Astronomy Picture of the Day archive.

Features:
    - Hero section: most recent APOD entry with full image display
    - Clickable timeline: click any point to see that entry's image and details
    - 4 interactive charts: entries over time, media type by year,
      top keywords, explanation length trend
    - 4 KPI cards: total entries, image/video split, avg word count
    - Filters: year range slider + media type dropdown

Run:
    python app.py
    Open http://127.0.0.1:8050 in your browser

Required packages:
    pip install dash plotly pandas sqlalchemy psycopg2-binary python-dotenv dash-bootstrap-components
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, Input, Output, callback, dcc, html, no_update
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

import dash_bootstrap_components as dbc


# ── Database connection ────────────────────────────────────────────────────────

def get_engine():
    """Build SQLAlchemy engine from .env credentials."""
    load_dotenv()
    direct_url = os.getenv("SUPABASE_DB_URL")
    if direct_url:
        return create_engine(direct_url)
    password = os.getenv("DB_PASSWORD")
    db_ref   = os.getenv("DB_REF")
    if not password or not db_ref:
        raise RuntimeError(
            "Set SUPABASE_DB_URL or both DB_PASSWORD and DB_REF in your .env file."
        )
    return create_engine(
        f"postgresql+psycopg2://postgres:{password}@db.{db_ref}.supabase.co:5432/postgres"
    )


# ── Data loading ───────────────────────────────────────────────────────────────

def load_data(engine) -> pd.DataFrame:
    """Pull full APOD dataset from PostgreSQL with media type joined."""
    query = """
        SELECT
            ae.entry_id,
            ae.entry_date,
            mt.media_type_name,
            ae.title,
            ae.explanation,
            ae.url,
            ae.hdurl,
            ae.copyright,
            ae.word_count,
            ae.char_count,
            EXTRACT(YEAR  FROM ae.entry_date)::INTEGER AS entry_year,
            EXTRACT(MONTH FROM ae.entry_date)::INTEGER AS entry_month
        FROM public.apod_entry ae
        JOIN public.media_type mt ON ae.media_type_id = mt.media_type_id
        ORDER BY ae.entry_date
    """
    df = pd.read_sql(query, engine)
    df["entry_date"] = pd.to_datetime(df["entry_date"])
    df["year_month"] = df["entry_date"].dt.to_period("M").astype(str)
    return df


# ── Colors & style ─────────────────────────────────────────────────────────────

COLORS = {
    "bg":         "#0a0e1a",
    "surface":    "#111827",
    "surface2":   "#1a2235",
    "border":     "#1e3a5f",
    "accent":     "#4f9cf9",
    "accent2":    "#f9a94f",
    "text":       "#e8edf5",
    "text_muted": "#7a8fb0",
    "image":      "#4f9cf9",
    "video":      "#f97b4f",
    "grid":       "#1a2a45",
}

FONT      = "DM Mono, Courier New, monospace"
FONT_SANS = "DM Sans, Helvetica Neue, sans-serif"

PLOT_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family=FONT_SANS, color=COLORS["text"], size=12),
    margin=dict(l=20, r=20, t=40, b=20),
    xaxis=dict(
        gridcolor=COLORS["grid"],
        linecolor=COLORS["border"],
        tickfont=dict(color=COLORS["text_muted"], size=11),
    ),
    yaxis=dict(
        gridcolor=COLORS["grid"],
        linecolor=COLORS["border"],
        tickfont=dict(color=COLORS["text_muted"], size=11),
    ),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(color=COLORS["text_muted"], size=11)),
)

CARD_STYLE = {
    "background":   COLORS["surface"],
    "border":       f"1px solid {COLORS['border']}",
    "borderRadius": "8px",
    "padding":      "20px",
}

LABEL_STYLE = {
    "fontFamily":    FONT,
    "fontSize":      "10px",
    "letterSpacing": "0.12em",
    "color":         COLORS["text_muted"],
    "margin":        "0 0 4px",
}

SUBTITLE_STYLE = {
    "fontSize":   "14px",
    "fontWeight": "500",
    "margin":     "0 0 16px",
}


# ── App init ───────────────────────────────────────────────────────────────────

engine   = get_engine()
df_full  = load_data(engine)
min_year = int(df_full["entry_year"].min())
max_year = int(df_full["entry_year"].max())

# Most recent image entry for hero section
hero_row = (
    df_full[df_full["media_type_name"] == "image"]
    .sort_values("entry_date", ascending=False)
    .iloc[0]
)

app = Dash(
    __name__,
    external_stylesheets=[
        dbc.themes.BOOTSTRAP,
        "https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=DM+Sans:wght@300;400;500;600&display=swap",
    ],
    title="Cosmos in Data — APOD Analytics",
)


# ── Component helpers ──────────────────────────────────────────────────────────

def kpi_card(label, value, sub="", accent=COLORS["accent"]):
    return html.Div(
        style={
            "background":   COLORS["surface"],
            "border":       f"1px solid {COLORS['border']}",
            "borderTop":    f"3px solid {accent}",
            "borderRadius": "8px",
            "padding":      "20px 24px",
            "flex":         "1",
            "minWidth":     "160px",
        },
        children=[
            html.P(label, style={
                "color": COLORS["text_muted"], "fontSize": "11px",
                "fontFamily": FONT, "letterSpacing": "0.1em",
                "textTransform": "uppercase", "margin": "0 0 8px 0",
            }),
            html.P(value, style={
                "color": COLORS["text"], "fontSize": "28px",
                "fontWeight": "600", "fontFamily": FONT_SANS,
                "margin": "0 0 4px 0", "lineHeight": "1",
            }),
            html.P(sub, style={
                "color": COLORS["text_muted"], "fontSize": "12px",
                "fontFamily": FONT_SANS, "margin": "0",
            }) if sub else html.Div(),
        ]
    )


def chart_card(label, subtitle, graph_id, col_span=False):
    style = {**CARD_STYLE}
    if col_span:
        style["gridColumn"] = "1 / -1"
    return html.Div(
        style=style,
        children=[
            html.P(label, style=LABEL_STYLE),
            html.P(subtitle, style=SUBTITLE_STYLE),
            dcc.Graph(id=graph_id, config={"displayModeBar": False}),
        ]
    )


def entry_card_content(row):
    """Build the image/video entry card from a DataFrame row."""
    is_video = row["media_type_name"] == "video"
    date_str = pd.to_datetime(row["entry_date"]).strftime("%B %d, %Y")
    copyright_str = f"© {row['copyright']}" if pd.notna(row.get("copyright")) and str(row.get("copyright", "")).strip() not in ("", "nan") else "Public domain"

    # Media element — embed image or show video link
    if is_video:
        media_el = html.Div(
            style={
                "background":   COLORS["surface2"],
                "border":       f"1px solid {COLORS['border']}",
                "borderRadius": "8px",
                "height":       "280px",
                "display":      "flex",
                "alignItems":   "center",
                "justifyContent": "center",
                "flexDirection": "column",
                "gap":          "12px",
                "marginBottom": "16px",
            },
            children=[
                html.Div("▶", style={"fontSize": "40px", "color": COLORS["video"]}),
                html.A(
                    "Watch Video →",
                    href=row["url"],
                    target="_blank",
                    style={
                        "color":          COLORS["accent"],
                        "fontSize":       "13px",
                        "fontFamily":     FONT,
                        "textDecoration": "none",
                        "border":         f"1px solid {COLORS['accent']}",
                        "padding":        "6px 16px",
                        "borderRadius":   "4px",
                    }
                ),
            ]
        )
    else:
        img_url = row.get("hdurl") if pd.notna(row.get("hdurl")) and str(row.get("hdurl", "")).strip() not in ("", "nan") else row["url"]
        media_el = html.A(
            href=img_url,
            target="_blank",
            children=[
                html.Img(
                    src=row["url"],
                    style={
                        "width":        "100%",
                        "maxHeight":    "320px",
                        "objectFit":    "cover",
                        "borderRadius": "6px",
                        "display":      "block",
                        "marginBottom": "16px",
                        "cursor":       "pointer",
                    },
                    title="Click to open full resolution",
                )
            ]
        )

    explanation = str(row["explanation"])
    short_exp   = explanation[:400] + "..." if len(explanation) > 400 else explanation

    return [
        html.Div(
            style={
                "display":        "flex",
                "justifyContent": "space-between",
                "alignItems":     "flex-start",
                "marginBottom":   "12px",
                "gap":            "8px",
            },
            children=[
                html.P(date_str, style={
                    "fontFamily": FONT,
                    "fontSize":   "11px",
                    "color":      COLORS["text_muted"],
                    "margin":     "0",
                }),
                html.Span(
                    row["media_type_name"].upper(),
                    style={
                        "fontSize":      "9px",
                        "fontFamily":    FONT,
                        "letterSpacing": "0.1em",
                        "color":         COLORS["video"] if is_video else COLORS["image"],
                        "border":        f"1px solid {COLORS['video'] if is_video else COLORS['image']}",
                        "padding":       "2px 8px",
                        "borderRadius":  "3px",
                        "whiteSpace":    "nowrap",
                    }
                ),
            ]
        ),
        html.H3(row["title"], style={
            "fontSize":      "16px",
            "fontWeight":    "600",
            "margin":        "0 0 14px",
            "lineHeight":    "1.3",
            "letterSpacing": "-0.01em",
        }),
        media_el,
        html.P(short_exp, style={
            "fontSize":   "12px",
            "color":      COLORS["text_muted"],
            "lineHeight": "1.7",
            "margin":     "0 0 10px",
        }),
        html.P(copyright_str, style={
            "fontSize":   "11px",
            "fontFamily": FONT,
            "color":      COLORS["text_muted"],
            "margin":     "0",
            "opacity":    "0.6",
        }),
    ]


# ── Layout ────────────────────────────────────────────────────────────────────

app.layout = html.Div(
    style={"minHeight": "100vh", "backgroundColor": COLORS["bg"],
           "fontFamily": FONT_SANS, "color": COLORS["text"]},
    children=[

        # ── Header ────────────────────────────────────────────────────────
        html.Div(
            style={
                "borderBottom": f"1px solid {COLORS['border']}",
                "padding":      "24px 40px 20px",
                "display":      "flex",
                "alignItems":   "center",
                "gap":          "16px",
            },
            children=[
                html.Div("✦", style={"fontSize": "28px", "color": COLORS["accent"], "lineHeight": "1"}),
                html.Div([
                    html.H1("Cosmos in Data", style={
                        "margin": "0", "fontSize": "22px", "fontWeight": "600",
                        "fontFamily": FONT_SANS, "letterSpacing": "-0.02em",
                    }),
                    html.P("NASA Astronomy Picture of the Day — Analytics Dashboard", style={
                        "margin": "2px 0 0", "fontSize": "12px",
                        "color": COLORS["text_muted"], "fontFamily": FONT,
                    }),
                ]),
                html.Div(style={"flex": "1"}),
                html.Div("MSBA 692 · Pipeline to Insights", style={
                    "fontSize": "11px", "color": COLORS["text_muted"],
                    "fontFamily": FONT, "letterSpacing": "0.08em",
                }),
            ]
        ),

        # ── Hero section ───────────────────────────────────────────────────
        html.Div(
            style={
                "display":    "grid",
                "gridTemplateColumns": "1fr 420px",
                "gap":        "0",
                "borderBottom": f"1px solid {COLORS['border']}",
                "maxHeight":  "480px",
                "overflow":   "hidden",
            },
            children=[
                # Hero image
                html.Div(
                    style={"position": "relative", "overflow": "hidden"},
                    children=[
                        html.Img(
                            src=hero_row["url"],
                            style={
                                "width":      "100%",
                                "height":     "480px",
                                "objectFit":  "cover",
                                "display":    "block",
                                "filter":     "brightness(0.75)",
                            }
                        ),
                        # Gradient overlay
                        html.Div(style={
                            "position":   "absolute",
                            "bottom":     "0",
                            "left":       "0",
                            "right":      "0",
                            "height":     "60%",
                            "background": "linear-gradient(to top, rgba(10,14,26,0.95), transparent)",
                        }),
                        # Text overlay on image
                        html.Div(
                            style={
                                "position": "absolute",
                                "bottom":   "28px",
                                "left":     "32px",
                                "right":    "32px",
                            },
                            children=[
                                html.P("TODAY'S FEATURED IMAGE", style={
                                    "fontFamily":    FONT,
                                    "fontSize":      "10px",
                                    "letterSpacing": "0.15em",
                                    "color":         COLORS["accent"],
                                    "margin":        "0 0 8px",
                                }),
                                html.H2(hero_row["title"], style={
                                    "fontSize":      "26px",
                                    "fontWeight":    "600",
                                    "margin":        "0 0 6px",
                                    "lineHeight":    "1.2",
                                    "letterSpacing": "-0.02em",
                                    "textShadow":    "0 2px 8px rgba(0,0,0,0.8)",
                                }),
                                html.P(
                                    pd.to_datetime(hero_row["entry_date"]).strftime("%B %d, %Y"),
                                    style={
                                        "fontFamily": FONT,
                                        "fontSize":   "12px",
                                        "color":      COLORS["text_muted"],
                                        "margin":     "0",
                                    }
                                ),
                            ]
                        ),
                    ]
                ),
                # Hero explanation panel
                html.Div(
                    style={
                        "padding":    "32px 28px",
                        "background": COLORS["surface"],
                        "display":    "flex",
                        "flexDirection": "column",
                        "gap":        "16px",
                        "overflowY":  "auto",
                        "borderLeft": f"1px solid {COLORS['border']}",
                    },
                    children=[
                        html.P("ABOUT THIS IMAGE", style={
                            "fontFamily":    FONT,
                            "fontSize":      "10px",
                            "letterSpacing": "0.12em",
                            "color":         COLORS["text_muted"],
                            "margin":        "0",
                        }),
                        html.P(
                            str(hero_row["explanation"])[:600] + "..."
                            if len(str(hero_row["explanation"])) > 600
                            else str(hero_row["explanation"]),
                            style={
                                "fontSize":   "13px",
                                "lineHeight": "1.75",
                                "color":      COLORS["text_muted"],
                                "margin":     "0",
                                "flex":       "1",
                            }
                        ),
                        html.Div(style={"marginTop": "auto"}, children=[
                            html.P(
                                f"© {hero_row['copyright']}"
                                if pd.notna(hero_row.get("copyright"))
                                and str(hero_row.get("copyright", "")).strip() not in ("", "nan")
                                else "Public domain",
                                style={
                                    "fontSize":   "11px",
                                    "fontFamily": FONT,
                                    "color":      COLORS["text_muted"],
                                    "opacity":    "0.5",
                                    "margin":     "0 0 10px",
                                }
                            ),
                            html.A(
                                "View full resolution →",
                                href=hero_row.get("hdurl") or hero_row["url"],
                                target="_blank",
                                style={
                                    "color":          COLORS["accent"],
                                    "fontSize":       "12px",
                                    "fontFamily":     FONT,
                                    "textDecoration": "none",
                                }
                            ),
                        ]),
                    ]
                ),
            ]
        ),

        # ── Controls ───────────────────────────────────────────────────────
        html.Div(
            style={
                "padding":         "20px 40px",
                "borderBottom":    f"1px solid {COLORS['border']}",
                "display":         "flex",
                "alignItems":      "center",
                "gap":             "32px",
                "flexWrap":        "wrap",
                "backgroundColor": COLORS["surface"],
            },
            children=[
                html.Div([
                    html.Label("YEAR RANGE", style={
                        "fontSize": "10px", "fontFamily": FONT,
                        "letterSpacing": "0.12em", "color": COLORS["text_muted"],
                        "display": "block", "marginBottom": "10px",
                    }),
                    dcc.RangeSlider(
                        id="year-slider",
                        min=min_year, max=max_year, step=1,
                        value=[min_year, max_year],
                        marks={y: {"label": str(y), "style": {"color": COLORS["text_muted"], "fontSize": "11px"}}
                               for y in range(min_year, max_year + 1)},
                        tooltip={"placement": "bottom", "always_visible": False},
                    ),
                ], style={"flex": "2", "minWidth": "300px"}),

                html.Div([
                    html.Label("MEDIA TYPE", style={
                        "fontSize": "10px", "fontFamily": FONT,
                        "letterSpacing": "0.12em", "color": COLORS["text_muted"],
                        "display": "block", "marginBottom": "8px",
                    }),
                    dcc.Dropdown(
                        id="media-dropdown",
                        options=[
                            {"label": "All Types",    "value": "all"},
                            {"label": "Images only",  "value": "image"},
                            {"label": "Videos only",  "value": "video"},
                        ],
                        value="all",
                        clearable=False,
                        style={
                            "backgroundColor": COLORS["surface2"],
                            "color":           COLORS["text"],
                            "border":          f"1px solid {COLORS['border']}",
                            "borderRadius":    "6px",
                            "minWidth":        "180px",
                        },
                    ),
                ], style={"flex": "0 0 200px"}),
            ]
        ),

        # ── KPI row ────────────────────────────────────────────────────────
        html.Div(
            id="kpi-row",
            style={"display": "flex", "gap": "16px", "padding": "24px 40px", "flexWrap": "wrap"}
        ),

        # ── Charts + entry card ────────────────────────────────────────────
        html.Div(
            style={
                "padding":             "0 40px 40px",
                "display":             "grid",
                "gridTemplateColumns": "1fr 340px",
                "gap":                 "20px",
            },
            children=[

                # Left column: charts
                html.Div(
                    style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "20px"},
                    children=[

                        # Timeline — full width
                        html.Div(
                            style={**CARD_STYLE, "gridColumn": "1 / -1"},
                            children=[
                                html.P("ENTRIES OVER TIME", style=LABEL_STYLE),
                                html.P("Monthly APOD publication count — click a point to explore that entry",
                                       style=SUBTITLE_STYLE),
                                dcc.Graph(
                                    id="chart-timeline",
                                    config={"displayModeBar": False},
                                ),
                            ]
                        ),

                        # Media type by year
                        chart_card("MEDIA TYPE BY YEAR",
                                   "Image vs video entries per year",
                                   "chart-media"),

                        # Top keywords
                        chart_card("TOP KEYWORDS",
                                   "Most frequent words in APOD titles",
                                   "chart-keywords"),

                        # Word count trend — full width
                        html.Div(
                            style={**CARD_STYLE, "gridColumn": "1 / -1"},
                            children=[
                                html.P("EXPLANATION LENGTH OVER TIME", style=LABEL_STYLE),
                                html.P("Average word count per explanation by year",
                                       style=SUBTITLE_STYLE),
                                dcc.Graph(id="chart-wordcount",
                                          config={"displayModeBar": False}),
                            ]
                        ),
                    ]
                ),

                # Right column: entry detail card
                html.Div(
                    style={
                        **CARD_STYLE,
                        "position":  "sticky",
                        "top":       "20px",
                        "alignSelf": "start",
                        "maxHeight": "85vh",
                        "overflowY": "auto",
                    },
                    children=[
                        html.P("ENTRY DETAIL", style=LABEL_STYLE),
                        html.P("Click any point on the timeline to explore",
                               style={**SUBTITLE_STYLE, "color": COLORS["text_muted"],
                                      "fontSize": "12px"}),
                        html.Div(id="entry-card", children=[
                            # Default state — show most recent entry
                            *entry_card_content(hero_row)
                        ]),
                    ]
                ),
            ]
        ),

        # ── Footer ─────────────────────────────────────────────────────────
        html.Div(
            style={
                "borderTop": f"1px solid {COLORS['border']}",
                "padding":   "16px 40px",
                "display":   "flex",
                "gap":       "24px",
            },
            children=[
                html.P("Data source: NASA APOD API (api.nasa.gov)", style={
                    "fontSize": "11px", "color": COLORS["text_muted"],
                    "fontFamily": FONT, "margin": "0",
                }),
                html.P("Database: Supabase PostgreSQL", style={
                    "fontSize": "11px", "color": COLORS["text_muted"],
                    "fontFamily": FONT, "margin": "0",
                }),
            ]
        ),
    ]
)


# ── Callbacks ──────────────────────────────────────────────────────────────────

def filter_df(year_range, media_type):
    df = df_full.copy()
    df = df[(df["entry_year"] >= year_range[0]) & (df["entry_year"] <= year_range[1])]
    if media_type != "all":
        df = df[df["media_type_name"] == media_type]
    return df


@callback(
    Output("kpi-row", "children"),
    Input("year-slider", "value"),
    Input("media-dropdown", "value"),
)
def update_kpis(year_range, media_type):
    df = filter_df(year_range, media_type)
    if df.empty:
        return [kpi_card("No data", "—", "Adjust filters")]
    date_min  = df["entry_date"].min().strftime("%b %Y")
    date_max  = df["entry_date"].max().strftime("%b %Y")
    img_pct   = (df["media_type_name"] == "image").mean()
    avg_words = df["word_count"].mean()
    return [
        kpi_card("Total Entries",  f"{len(df):,}", f"{date_min} – {date_max}", COLORS["accent"]),
        kpi_card("Images",         f"{img_pct:.0%}", f"{int(img_pct * len(df)):,} entries", COLORS["image"]),
        kpi_card("Videos",         f"{1-img_pct:.0%}", f"{len(df)-int(img_pct*len(df)):,} entries", COLORS["video"]),
        kpi_card("Avg Word Count", f"{avg_words:.0f}", "words per explanation", COLORS["accent2"]),
    ]


@callback(
    Output("chart-timeline", "figure"),
    Input("year-slider", "value"),
    Input("media-dropdown", "value"),
)
def update_timeline(year_range, media_type):
    df = filter_df(year_range, media_type)
    if df.empty:
        return go.Figure(layout={**PLOT_LAYOUT, "title": "No data"})
    monthly = df.groupby("year_month").size().reset_index(name="count")
    fig = px.area(monthly, x="year_month", y="count",
                  labels={"year_month": "", "count": "Entries"})
    fig.update_traces(
        line_color=COLORS["accent"],
        fillcolor="rgba(79,156,249,0.12)",
        hovertemplate="<b>%{x}</b><br>%{y} entries<extra></extra>",
    )
    fig.update_layout(**PLOT_LAYOUT, height=240)
    fig.update_xaxes(tickangle=-45, nticks=20)
    return fig


@callback(
    Output("entry-card", "children"),
    Input("chart-timeline", "clickData"),
    Input("year-slider", "value"),
    Input("media-dropdown", "value"),
)
def update_entry_card(click_data, year_range, media_type):
    """
    When the user clicks a point on the timeline, find a random image
    entry from that month and display it in the entry card.
    Falls back to the most recent image entry if nothing is clicked.

    Handles two Plotly click data formats:
      - x as a year_month string e.g. "2022-03"
      - x as a point index integer (area chart sometimes returns this)
    """
    df = filter_df(year_range, media_type)
    if df.empty:
        return [html.P("No entries for selected filters.",
                       style={"color": COLORS["text_muted"], "fontSize": "13px"})]

    # Default: show most recent image entry in filtered set
    if click_data is None:
        img_df = df[df["media_type_name"] == "image"]
        row = (img_df if not img_df.empty else df).sort_values("entry_date", ascending=False).iloc[0]
        return entry_card_content(row)

    point      = click_data["points"][0]
    clicked_x  = point.get("x", "")

    # If x is a string in YYYY-MM format, use it directly
    if isinstance(clicked_x, str) and len(clicked_x) == 7 and "-" in clicked_x:
        month_df = df[df["year_month"] == clicked_x]
    else:
        # x may be a point index — use the curve number to look up the month
        point_idx = point.get("pointIndex", point.get("pointNumber", None))
        if point_idx is None:
            return no_update
        monthly = df.groupby("year_month").size().reset_index(name="count")
        if point_idx >= len(monthly):
            return no_update
        clicked_label = monthly.iloc[point_idx]["year_month"]
        month_df = df[df["year_month"] == clicked_label]

    if month_df.empty:
        return no_update

    # Prefer image entries; pick a random one from the month
    img_month = month_df[month_df["media_type_name"] == "image"]
    pool = img_month if not img_month.empty else month_df
    row  = pool.sample(1).iloc[0]
    return entry_card_content(row)


@callback(
    Output("chart-media", "figure"),
    Input("year-slider", "value"),
    Input("media-dropdown", "value"),
)
def update_media(year_range, media_type):
    df = filter_df(year_range, media_type)
    if df.empty:
        return go.Figure(layout={**PLOT_LAYOUT, "title": "No data"})
    by_year = df.groupby(["entry_year", "media_type_name"]).size().reset_index(name="count")
    fig = px.bar(by_year, x="entry_year", y="count", color="media_type_name",
                 color_discrete_map={"image": COLORS["image"], "video": COLORS["video"]},
                 barmode="stack",
                 labels={"entry_year": "Year", "count": "Entries", "media_type_name": "Type"})
    fig.update_traces(hovertemplate="<b>%{x}</b><br>%{y} entries<extra></extra>")
    fig.update_layout(**PLOT_LAYOUT, height=300)
    fig.update_xaxes(dtick=1)
    return fig


@callback(
    Output("chart-keywords", "figure"),
    Input("year-slider", "value"),
    Input("media-dropdown", "value"),
)
def update_keywords(year_range, media_type):
    df = filter_df(year_range, media_type)
    if df.empty:
        return go.Figure(layout={**PLOT_LAYOUT, "title": "No data"})
    stopwords = {
        "a","an","the","and","or","of","in","to","is","it","its","on","at",
        "for","with","as","by","from","this","that","be","was","are","were",
        "has","have","had","not","but","so","do","if","than","then","into",
        "over","our","your","we","they","he","she","i","you","s","de",
    }
    words = (df["title"].str.lower().str.replace(r"[^a-z\s]","",regex=True)
             .str.split().explode().dropna())
    top = (words[~words.isin(stopwords)].value_counts().head(20)
           .reset_index())
    top.columns = ["word", "count"]
    top = top.sort_values("count", ascending=True)
    fig = px.bar(top, x="count", y="word", orientation="h",
                 labels={"count": "Appearances", "word": ""},
                 color="count",
                 color_continuous_scale=[[0,COLORS["surface2"]],[0.5,COLORS["accent"]],[1,"#a8d4ff"]])
    fig.update_traces(hovertemplate="<b>%{y}</b><br>%{x} appearances<extra></extra>")
    fig.update_layout(**PLOT_LAYOUT, height=300, coloraxis_showscale=False)
    return fig


@callback(
    Output("chart-wordcount", "figure"),
    Input("year-slider", "value"),
    Input("media-dropdown", "value"),
)
def update_wordcount(year_range, media_type):
    df = filter_df(year_range, media_type)
    if df.empty:
        return go.Figure(layout={**PLOT_LAYOUT, "title": "No data"})
    by_year = (df.groupby("entry_year")
               .agg(avg_words=("word_count","mean"),
                    min_words=("word_count","min"),
                    max_words=("word_count","max"))
               .round(1).reset_index())
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=list(by_year["entry_year"]) + list(by_year["entry_year"])[::-1],
        y=list(by_year["max_words"]) + list(by_year["min_words"])[::-1],
        fill="toself", fillcolor="rgba(79,156,249,0.08)",
        line=dict(color="rgba(0,0,0,0)"), hoverinfo="skip", showlegend=False,
    ))
    fig.add_trace(go.Scatter(
        x=by_year["entry_year"], y=by_year["avg_words"],
        mode="lines+markers",
        line=dict(color=COLORS["accent"], width=2.5),
        marker=dict(size=7, color=COLORS["accent"], line=dict(width=1.5, color=COLORS["bg"])),
        hovertemplate="<b>%{x}</b><br>Avg: %{y:.0f} words<extra></extra>",
        showlegend=False,
    ))
    fig.update_layout(**PLOT_LAYOUT, height=220)
    fig.update_xaxes(dtick=1)
    fig.update_yaxes(title_text="Avg Words")
    return fig


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True)