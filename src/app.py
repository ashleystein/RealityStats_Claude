import dash
import pandas as pd
import os
from dash import Dash, html, dcc
from pathlib import Path
src_path = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = Path(__file__).parent

app = Dash(__name__,
           use_pages=True,
           assets_folder=os.path.join(src_path, 'dagster_code/defs/assets'))
server = app.server
# Updated styles for a compact, centered look
tab_style = {
    "padding": "0 20px",        # Horizontal padding creates the "breathing room"
    "lineHeight": "40px",
    "border": "none",
    "backgroundColor": "transparent",
    "color": "white",
    "cursor": "pointer",
    "flex": "0 1 auto",         # IMPORTANT: Prevents stretching, allows content-based width
    "minWidth": "fit-content",  # Ensures the tab is at least as wide as the word
    "whiteSpace": "nowrap"      # Prevents words from wrapping to a second line
}

tab_selected_style = {
    **tab_style,
    "fontWeight": "bold",
    "borderBottom": "3px solid white"
}
app.layout = html.Div([
html.Div(
        style={
            "backgroundColor": "#D8A7B1",  # Your muted pink background
            "display": "flex",             # Puts children side-by-side
            "flexDirection": "row",
            "alignItems": "center",        # Vertically aligns title and tabs
            "padding": "0 20px",           # Horizontal padding
            "height": "60px"               # Fixed height for a sleek bar
        },
        children=[
    html.H1('Reality Stats',
            style={
                "color": "white",
                "margin": "0",
                "marginRight": "40px",  # Space between title and tabs
                "fontFamily": "sans-serif",
                "fontSize": "24px"  # Slightly smaller to fit the bar
            }),



        # dcc.Tabs(
        #     id="stats-tabs",
        #     value="tab-table",
        #     children=[
        #         dcc.Tab(label= html.A(
        #                 "Dashboard",
        #                 href="/",  # path from dash.register_page
        #                 target="_blank",
        #                 style={"marginLeft": "40px", "color": "white", "textDecoration": "none"}
        #             ),
        #                 value="tab-table",
        #                 style=tab_style, selected_style=tab_selected_style),
        #         dcc.Tab(label= html.A(
        #             "Network Graph",
        #             href="/network-graph",  # path from dash.register_page
        #             target="_blank",
        #             style={"marginLeft": "40px", "color": "white", "textDecoration": "none"},
        #         ),
        #             value="tab-charts",
        #             style=tab_style,
        #             selected_style=tab_selected_style),
        #
        #     ],
        #     # Centering logic happens here
        #     style={
        #         "height": "40px",          # Overall height of the tab bar
        #         "display": "flex",
        #         "flexDirection": "row",
        #         "justifyContent": "center", # Centers the tabs horizontally
        #         "width": "100%"            # Ensure it takes full width of container to center correctly
        #     }
        # ),
        
        ]),

    # This component renders the content of the current page
    dash.page_container
])

if __name__ == '__main__':

    app.run(host='0.0.0.0', port=8050, debug=True)