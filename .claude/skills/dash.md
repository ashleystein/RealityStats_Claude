## Analytics Page (Dash)

### Adding a new filter dropdown

```python
# src/pages/analytics.py — inside the filters Div
html.Label("Network", style={"fontWeight": "bold", "color": "#8E6E75", "fontSize": "13px"}),
dcc.Dropdown(
    id="network-filter",
    options=[{"label": n, "value": n} for n in main_grid_df["Network"].unique()],
    placeholder="Select Network",
    multi=True,
    style={"marginBottom": "20px"}
),
```

### Wiring a new filter into the table callback

```python
@callback(
    Output("my-table", "rowData"),
    Input("show-filter", "value"),
    Input("season-filter", "value"),
    Input("network-filter", "value"),   # <-- add new Input
)
def update_table(selected_shows, selected_seasons, selected_networks):
    df = get_display_data()
    if selected_shows:
        df = df[df["Show"].isin(selected_shows)]
    if selected_seasons:
        df = df[df["Season"].isin(selected_seasons)]
    if selected_networks:
        df = df[df["Network"].isin(selected_networks)]
    return df.to_dict("records")
```

### Adding a new column to the AG Grid

```python
# Inside the columnDefs list in analytics.py
{"field": "Followers", "valueFormatter": {"function": "d3.format(',.0f')(params.value)"}},
```

---