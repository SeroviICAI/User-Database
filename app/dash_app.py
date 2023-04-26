from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc

import dash_mantine_components as dmc
from app.figures import *
from utils.database import connect_to_mysql, connect_to_mongodb

from datetime import datetime
import random

app = Dash(__name__, external_stylesheets=[dbc.themes.SPACELAB, dbc.icons.BOOTSTRAP],
           suppress_callback_exceptions=True)

# Connect to MongoDB and MySQL databases
MONGO_CLIENT = connect_to_mongodb()
MySQL_CONN = connect_to_mysql()

mongo_collection = MONGO_CLIENT['amz_reviews']['reviews']
cursor = MySQL_CONN.cursor()
cursor.execute('USE amz_reviews')

# Select different categories
categories = mongo_collection.distinct('category')
item_ids = mongo_collection.distinct('item_id')
user_ids = mongo_collection.distinct('reviewer_id')

card_users = dbc.Card(
    dbc.CardBody(
        [
            html.Label([html.I(className="bi bi-people-fill"), html.Strong(" Num. Users: "),
                       str(len(user_ids))],
                       className="text-nowrap", style={'font-size': '20px'}),
        ], className="border-start border-success border-5"
    ),
    className="text-center m-4"
)


card_items = dbc.Card(
    dbc.CardBody(
        [
            html.Label([html.I(className="bi bi-cart-fill"), html.Strong(" Num. Items: "),
                       str(len(item_ids))], className="text-nowrap",
                       style={'font-size': '20px'})
        ], className="border-start border-danger border-5"
    ),
    className="text-center m-4",
)

card_reviews = dbc.Card(
    dbc.CardBody(
        [
            html.Label([html.I(className="bi bi-journal-text"), html.Strong(" Num. Reviews: "),
                       str(mongo_collection.count_documents({}))],
                       className="text-nowrap", style={'font-size': '20px'})
        ], className="border-start border-primary border-5"
    ),
    className="text-center m-4",
)

app.layout = html.Div([
    html.Div([
        html.H1(children='AMZ reviews'),
        html.Label(
            "This dashboard provides an overview of Amazon customer reviews from multiple databases including MongoDB, "
            "MySQL, and Neo4j. It allows you to explore and analyze customer feedback on various products and "
            "categories. With interactive visualizations and real-time data updates, you can gain insights into "
            "customer sentiment and trends. Use this dashboard to make data-driven decisions and improve your "
            "understanding of your customers' needs and preferences.",
            style={'color': 'black'}),
        html.Img(src=app.get_asset_url('comillas.png'), style={'position': 'relative', 'width': '140%',
                                                               'left': '-40px', 'top': '-10px'})
    ], className='side_bar'),

    html.Div([
        html.Div([
            dbc.Container(
                dbc.Row(
                    [dbc.Col(card_users), dbc.Col(card_items), dbc.Col(card_reviews)],
                ),
                fluid=True,
            ),

            html.Div([
                html.Div([
                    html.Label(html.Strong("1. Number of Reviews by Category and Year"),
                               style={'font-size': 'medium'}),
                    html.Br(),
                    html.Label('This figure displays the number of reviews for each category in the given collection, '
                               'grouped by year. The data is obtained by aggregating the collection and grouping by '
                               'year and category. The resulting data is then plotted as a stacked bar chart, with '
                               'each category represented by a different color. The x-axis shows the years and the '
                               'y-axis shows the number of reviews. The figure provides a visual representation of '
                               'the distribution of reviews across categories and years.', style={'font-size': '12px'}),
                    html.Br(),
                    html.Br(),
                    html.Label(html.Strong("Select Categories:"), style={'font-size': '14px'}),
                    dcc.Dropdown(
                        id='categories-dropdown',
                        options=[{'label': category, 'value': category} for category in categories],
                        value=categories,
                        multi=True
                    ),
                    html.Br(),
                    dcc.Graph(id='fig1')
                ], className='box', style={'width': '40%'}),
                html.Div([
                    html.Label(html.Strong("2. Number of Good and Bad Reviews by Item"),
                               style={'font-size': 'medium'}),
                    html.Br(),
                    html.Label('This figure displays the number of good and bad reviews for each item in the given '
                               'collection.The data is obtained by aggregating the collection and grouping by item '
                               'and overall rating.The resulting data is then plotted as a stacked horizontal bar '
                               'chart, with good reviews represented by green bars and bad reviews represented by '
                               'red bars.The y-axis shows the items and the x-axis shows the number of reviews.The '
                               'figure provides a visual representation of the distribution of good and bad reviews '
                               'across items.', style={'font-size': '12px'}),
                    html.Br(),
                    html.Br(),
                    html.Label(html.Strong("Enter item limit:"), style={'font-size': '14px'}),
                    html.Br(),
                    dcc.Input(
                        id='item-limit',
                        type='number',
                        value=20
                    ),
                    html.Br(),
                    html.Div([
                        dcc.Graph(id='fig2')
                    ], style={'overflow-y': 'scroll', 'height': '480px'})
                ], className='box', style={'width': '63%'})
            ], style={'display': 'flex'}),

            html.Div([
                html.Div([
                    html.Label(html.Strong("3. Number of Reviews by Rating and Category/Item"),
                               style={'font-size': 'medium'}),
                    html.Br(),
                    html.Label('This figure displays the number of reviews for each category/item in the given '
                               'collection, grouped by rating. The data is obtained by aggregating the collection and '
                               'grouping by rating and category. The resulting data is then plotted as a donut chart, '
                               'with each category represented by a different color. The inner ring shows the '
                               'distribution of reviews across categories and the outer ring shows the distribution of '
                               'reviews across ratings. The figure provides a visual representation of the '
                               'distribution of reviews across categories and ratings.', style={'font-size': '12px'}),
                    html.Br(),
                    html.Br(),
                    html.Label(html.Strong('Select Search Field:'), style={'font-size': '14px'}),
                    html.Br(),
                    dcc.Dropdown(
                        id='search-field-dropdown',
                        options=[
                            {'label': 'Item ID', 'value': 'item_id'},
                            {'label': 'Category', 'value': 'category'}
                        ],
                        value='category'
                    ),
                    html.Br(),
                    html.Label(html.Strong('Select Values:'), style={'font-size': '14px'}),
                    html.Br(),
                    dcc.Dropdown(
                        id='values-dropdown',
                        options=[],
                        value=None,
                        multi=True
                    ),
                    dcc.Input(
                        id='input-value',
                        type='text',
                        value='',
                        placeholder='Add option...'
                    ),
                    html.Button('Submit Option',
                                id='submit-button',
                                n_clicks=0),
                    html.Br(),
                    html.Br(),
                    dcc.Graph(id='fig3')
                ], className='box', style={'width': '40%'}),
                html.Div([
                    html.Label(html.Strong("4. Cumulative Number of Reviews by Date and Category"),
                                           style={'font-size': 'medium'}),
                    html.Br(),
                    html.Label('This figure displays the cumulative number of reviews for each category in the given '
                               'collection over time. The data is obtained by aggregating the collection and grouping '
                               'by date. The resulting data is then plotted as a line chart, with the x-axis '
                               'representing the date in unix time and the y-axis representing the cumulative number '
                               'of reviews. The figure provides a visual representation of the growth in the number of '
                               'reviews for each category over time.', style={'font-size': '12px'}),
                    html.Br(),
                    html.Br(),
                    html.Label(html.Strong("Select Categories:"), style={'font-size': '14px'}),
                    dcc.Dropdown(
                        id='categories-dropdown-2',
                        options=[{'label': category, 'value': category} for category in categories],
                        value=['Video games', 'Digital music'],
                        multi=True,
                        style={'width': '450px'}
                    ),
                    html.Br(),
                    html.Div([
                        html.Div([
                                html.Label(html.Strong("Select Date Range:"), style={'font-size': '14px'}),
                                dmc.DateRangePicker(
                                    id="date-range-picker",
                                    description="Select your desired data range",
                                    style={"width": 330},
                                ),
                            ]
                        ),
                        dmc.Space(h=10),
                        dmc.Text(id="selected-date-date-range-picker"),
                        ]
                    ),
                    html.Br(),
                    html.Br(),
                    html.Div([
                        dcc.Graph(id='fig4')
                    ])
                ], className='box', style={'width': '63%'})
            ], style={'display': 'flex'}),

            html.Div([
                html.Div([
                    html.Label(html.Strong("5. Number of Good and Bad Reviews by User"),
                               style={'font-size': 'medium'}),
                    html.Br(),
                    html.Label('This figure displays the number of good and bad reviews for each user in the given '
                               'collection. The data is obtained by aggregating the collection and grouping by '
                               'reviewer ID and overall rating. The resulting data is then plotted as a stacked bar '
                               'chart, with good reviews represented by a green bar and bad reviews represented by a '
                               'red bar. The y-axis shows the user IDs and the x-axis shows the number of reviews. '
                               'The figure provides a visual representation of the distribution of good and bad '
                               'reviews across users.', style={'font-size': '12px'}),
                    html.Br(),
                    html.Br(),
                    html.Label(html.Strong("Enter user limit:"), style={'font-size': '14px'}),
                    html.Br(),
                    dcc.Input(
                        id='user-limit',
                        type='number',
                        value=20
                    ),
                    html.Br(),
                    html.Div([
                        dcc.Graph(id='fig5')
                    ], style={'overflow-y': 'scroll', 'height': '480px'})
                ], className='box', style={'width': '63%'}),
                html.Div([
                    html.Label(html.Strong("6. Word Cloud of Reviews by Category"), style={'font-size': 'medium'}),
                    html.Br(),
                    html.Label('This figure displays a word cloud of the most frequently used words in reviews for '
                               'a given category. The data is obtained by querying the collection for all documents '
                               'with the specified category and retrieving their review text. The review text is '
                               'then processed to extract words and a word cloud is generated using the WordCloud '
                               'library. The word cloud is displayed as an image with the most frequently used words '
                               'appearing larger in size. The figure provides a visual representation of the most '
                               'common words used in reviews for the given category.', style={'font-size': '12px'}),
                    html.Br(),
                    html.Br(),
                    html.Label(html.Strong("Select Category:"), style={'font-size': '14px'}),
                    dcc.Dropdown(
                        id='category-dropdown',
                        options=[{'label': category, 'value': category} for category in categories],
                        value='Video games',
                    ),
                    html.Br(),
                    dcc.Graph(id='fig6')
                ], className='box', style={'width': '40%'}),
            ], style={'display': 'flex'}),

            html.Div([
                html.Div([
                    html.Label(html.Strong("7. Graph of User Reviews"), style={'font-size': 'medium'}),
                    html.Br(),
                    html.Label('This figure displays a graph of user reviews. The data is obtained by querying the '
                               'collection for all documents with the specified user IDs and retrieving their review '
                               'information. The review information is then used to generate a graph using the '
                               'NetworkX library. The graph is displayed using the Plotly library with users and items '
                               'represented as nodes and reviews represented as edges. The figure provides a visual '
                               'representation of the relationships between users, items, and reviews.',
                               style={'font-size': '12px'}),
                    html.Br(),
                    html.Br(),
                    html.Label(html.Strong("Select Users:"), style={'font-size': '14px'}),
                    html.Br(),
                    dcc.Dropdown(
                        id='users-dropdown',
                        options=[],
                        value=None,
                        multi=True
                    ),
                    dcc.Input(
                        id='input-value-2',
                        type='text',
                        value='',
                        placeholder='Add option...'
                    ),
                    html.Button('Submit Option',
                                id='submit-button-2',
                                n_clicks=0),
                    html.Br(),
                    html.Br(),
                    dcc.Graph(id='fig7')
                ], className='box', style={'width': '97%'}),
            ], style={'display': 'flex'}),

            html.Div([
                html.Div([
                    html.P(['Comillas ICAI', html.Br(),
                            'Sergio Rodríguez (202113624), Álvaro Pereira (202114948)'],
                           style={'font-size': '12px', 'color': 'white'}),
                ], style={'width': '60%'}),
                html.Div([
                    html.P(['Sources ', html.Br(),
                            html.A('Our website', href='https://www.comillas.edu/', target='_blank')],
                           style={'font-size': '12px', 'color': 'white'})
                ], style={'width': '37%'}),
            ], className='footer', style={'display': 'flex'}),
        ], className='main'),
    ])
])


@app.callback(
    Output('fig1', 'figure'),
    Input('categories-dropdown', 'value')
)
def update_fig1(categories_):
    return generate_fig1(mongo_collection, categories_)


@app.callback(
    Output('fig2', 'figure'),
    Input('item-limit', 'value')
)
def update_fig2(limit):
    return generate_fig2(mongo_collection, limit)


prev_search_field = None
prev_n_clicks = 0


@app.callback(
    [Output('values-dropdown', 'options'),
     Output('values-dropdown', 'value')],
    [Input('search-field-dropdown', 'value'),
     Input('submit-button', 'n_clicks'),
     Input('input-value', 'value'),
     Input('values-dropdown', 'value')]
)
def update_values_dropdown(search_field, n_clicks, input_value, values):
    global prev_search_field, prev_n_clicks
    if search_field == 'item_id':
        options = item_ids[:50] + [value for value in values if value not in categories]
        if input_value in item_ids and input_value not in options and n_clicks - prev_n_clicks > 0:
            options.append(input_value)
            values.append(input_value)
    else:
        options = categories

    options = [{'label': option, 'value': option} for option in options]
    prev_n_clicks = n_clicks

    if values is None or search_field != prev_search_field:
        prev_search_field = search_field
        values = [options[0]['value'], options[1]['value']]
    return options, values


@app.callback(
    Output('fig3', 'figure'),
    Input('search-field-dropdown', 'value'),
    Input('values-dropdown', 'value')
)
def update_fig3(search_field, values):
    return generate_fig3(mongo_collection, search_field, values)


@app.callback(
    [Output('date-range-picker', 'minDate'),
     Output('date-range-picker', 'maxDate'),
     Output('date-range-picker', 'value')],
    Input('categories-dropdown-2', 'value'))
def update_date_range_picker(categories_):
    if not categories_:
        return None, None
    match = {'category': {'$in': categories_}}
    min_date = str(mongo_collection.find(match).sort('reviewTime', 1).limit(1)[0]['reviewTime']).rsplit(' ')[0]
    max_date = str(mongo_collection.find(match).sort('reviewTime', -1).limit(1)[0]['reviewTime']).rsplit(' ')[0]
    return min_date, max_date, [min_date, max_date]


@app.callback(
    Output('fig4', 'figure'),
    [Input('categories-dropdown-2', 'value'),
     Input('date-range-picker', 'value')]
)
def update_fig4(categories_, dates):
    start_date, end_date = dates
    return generate_fig4(collection=mongo_collection, categories=categories_,
                         start_date=datetime.strptime(start_date, "%Y-%m-%d"),
                         end_date=datetime.strptime(end_date, "%Y-%m-%d"))


@app.callback(
    Output('fig5', 'figure'),
    Input('user-limit', 'value')
)
def update_fig5(limit):
    return generate_fig5(mongo_collection, limit)


@app.callback(
    Output('fig6', 'figure'),
    Input('category-dropdown', 'value')
)
def update_fig6(category):
    return generate_fig6(mongo_collection, category)


prev_n_clicks_2 = 0


@app.callback(
    [Output('users-dropdown', 'options'),
     Output('users-dropdown', 'value')],
    [Input('submit-button-2', 'n_clicks'),
     Input('input-value-2', 'value'),
     Input('users-dropdown', 'value')]
)
def update_users_dropdown(n_clicks, input_value, values):
    global prev_n_clicks_2
    options = user_ids[:100]
    if values is None:
        options = [{'label': option, 'value': option} for option in options]
        values = [option['value'] for option in random.sample(options, 15)]
    else:
        options += values
        if input_value in user_ids and input_value not in options and n_clicks - prev_n_clicks_2 > 0:
            options.append(input_value)
            values.append(input_value)

        options = [{'label': option, 'value': option} for option in options]
        prev_n_clicks_2 = n_clicks
    return options, values


@app.callback(
    Output('fig7', 'figure'),
    Input('users-dropdown', 'value'),
)
def update_fig7(user_ids_):
    return generate_fig7(mongo_collection, user_ids_)


def launch_app():
    app.run_server(debug=True)
