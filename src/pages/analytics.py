import dash
import pandas as pd
from dash import html, dcc, callback, Output, Input
import dash_ag_grid as dag
import aws
import os
import config as cfg
from pathlib import Path
environment = cfg.get_config().env
SRC_DIR = os.path.dirname(os.path.dirname(os.path.abspath("RealityStats")))

ROOT_DIR = Path(__file__).resolve().parents[2]
data_folder = f"{ROOT_DIR}/data/"

def get_display_data(env=environment):
    if env == 'dev':
        data_dir = "data/analytics_page.csv"
        data_dir = os.path.join(ROOT_DIR, data_dir)
        return pd.read_csv(data_dir)# S3 bucket prefix
    else:
        print("Getting data from S3")
        return aws.get_s3_file("data/analytics_page.csv")

def parse_followers(value: str) -> float:
    """Turn strings like '18.1K' or '4505' into a numeric follower count."""
    if pd.isna(value):
        return 0.0
    s = str(value).strip().upper().replace(",", "")
    match s[-1]:
        case "K":
            return float(s[:-1]) * 1000
        case "M":
            return float(s[:-1]) * 1000000
        case _:
            return float(s)

@callback(
    Output("person-details", "children"),
    Input("my-table", "selectedRows"),
)
def show_person_details(selected_rows):
    if not selected_rows:
        return "Select a row to see details"
    row = selected_rows[0]
    name = row.get("Contestant", "Unknown")

    df = pd.read_csv(data_folder + "reality_contestants.csv")
    filtered_df = df[df['name'] == name][["hometown", "state", "job", "show"]].fillna("unknown")
    hometown = filtered_df.iloc[0]["hometown"]
    state = filtered_df.iloc[0]["state"]
    job = filtered_df.iloc[0]["job"]
    show = filtered_df.iloc[0]["show"]

    return html.Div([
        html.H3(row.get("Contestant")),
        html.P(f"Rookie Season: {show}"),
        html.P(f"Hometown: {hometown}, {state}"),
        html.P(f"Civilian job: {job}"),
    ])

@callback(
    Output("my-table", "rowData"),
    #Input("grid-search-input", "value"),
    Input("show-filter", "value"),
    Input("season-filter", "value")
)
def update_table(selected_shows, selected_seasons):

    df = get_display_data()
    # Filter by Search Text
    # if search_text:
    #     filtered_df = filtered_df[filtered_df["Contestant"].str.contains(search_text, case=False, na=False)]

    # Filter by Show (Handles multiple selections)
    if selected_shows:
        filtered_df = df[df["Show"].isin(selected_shows)]

    # Filter by Season
    if selected_seasons:
        filtered_df = df[df["Season"].isin(selected_seasons)]

    return filtered_df.to_dict("records")

dash.register_page(__name__, path='/')
main_grid_df = get_display_data(env=environment)

### Page HTML
layout = html.Div([

    html.Div([

        ########### LEFT: FILTERS
        html.Div([
            html.H4("Filters", style={"color": "#8E6E75", "marginTop": "0"}),

            # --- SHOW FILTER ---
            html.Label("Show", style={"fontWeight": "bold", "color": "#8E6E75", "fontSize": "13px"}),
            dcc.Dropdown(
                id="show-filter",
                options=[{"label": s, "value": s} for s in main_grid_df["Show"].unique()],
                placeholder="Select Show",
                multi=True, # Allows selecting multiple shows at once
                style={"marginBottom": "20px"}
            ),

            # --- SEASON FILTER ---
            html.Label("Season", style={"fontWeight": "bold", "color": "#8E6E75", "fontSize": "13px"}),
            dcc.Dropdown(
                id="season-filter",
                options=[{"label": f"Season {s}", "value": s} for s in sorted(main_grid_df["show"].str.split().str[-1].unique())],
                placeholder="Select Season",
                multi=True,
                style={"marginBottom": "20px"}
            ),

        # Add more filters here easily!
        ], style={
                "width": "250px",  # Fixed width for sidebar
                "backgroundColor": "white",
                "padding": "25px",
                "borderRight": "1px solid #E5D1D5",
                "display": "flex",
                "flexDirection": "column",
                "height": "90%"
            }
        ),
        ##### END OF LEFT


        ### MIDDLE: TABLE
        dag.AgGrid(
            id="my-table",
            rowData=main_grid_df.to_dict("records"),      # The data
            columnDefs=[
                {"field": "Contestant"},
                {"field": "Show"},
                {"field": "Season"},

                {
                    "field": "IG Username",
                    "cellRenderer": "markdown",
                    "cellRendererParams": {"linkTarget": "_blank"},
                },
                
            ],

            columnSize="sizeToFit",
            columnSizeOptions={
                "defaultMinWidth": 100,
                "columnLimits": [{"key": "Contestant", "minWidth": 150}],
            },
            dashGridOptions={
                "pagination": True,
                "responsiveSizeToFit": True, # This is the magic toggle
                "rowSelection": "single"
            }, style={"height": "90vh", "width": "95%"}
        ),


    ], style={
                "display": "flex",           # Enable Flexbox
                "justifyContent": "center",  # Center children horizontally within the 50% width
                "alignItems": "center",      # Center children vertically
                "height": "calc(100vh - 80px)",
                "width": "60%",              # Occupy exactly half the container width
                "backgroundColor": "#FAF3F4",
                "boxSizing": "border-box",   # Ensures padding doesn't push width past 50%
                "float": "left"              # Optional: Aligns the div to the left side
            },


    ),
    html.Div(
            id="person-details",
            style={
                "flex": 1,
                "border": "1px solid #ddd",
                "padding": "10px",
                "borderRadius": "4px",
                "minWidth": "1000",

            },
            children="Select a row to see details",
    ),
], style={"margin": "0", "padding": "0", "display": "flex", "gap": "24px", "width": "100%"},
)
