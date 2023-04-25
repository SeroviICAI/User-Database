import plotly.express as px
import plotly.graph_objs as go
from PIL import Image

from wordcloud import WordCloud, ImageColorGenerator
from typing import Collection
import numpy as np

import networkx as nx

__all__ = ['generate_fig1', 'generate_fig2', 'generate_fig3', 'generate_fig4', 'generate_fig5', 'generate_fig6',
           'generate_fig7']


def generate_fig1(collection, categories: Collection[str]):
    response = list(collection.aggregate([
        {'$match': {'category': {'$in': categories}}},
        {'$group': {'_id': {'year': {'$year': '$reviewTime'}, 'category': '$category'},
                    'num_reviews': {'$sum': 1}}}
    ]))
    data = {}
    for item in response:
        year = item['_id']['year']
        category = item['_id']['category']
        num_reviews = item['num_reviews']
        if category not in data:
            data[category] = {}
        data[category][year] = num_reviews
    fig = go.Figure()
    colors = ['#883000', '#CB5C0D', '#FD6A02', '#EF820D', '#FDA50F', '#FFBF00', '#F8DE7E', '#FFED83']
    for i, (category, values) in enumerate(data.items()):
        years = list(values.keys())
        num_reviews = list(values.values())
        fig.add_trace(go.Bar(x=years, y=num_reviews, name=category,
                             marker_color=colors[i % len(colors)]))
    fig.update_layout(barmode='stack', height=360, margin=dict(t=30))
    fig.update_yaxes(title='Number of reviews')
    return fig


def generate_fig2(collection, limit=None):
    if limit is not None:
        aggregation_pipeline_1 = [
            {'$group': {'_id': '$item_id', 'total_reviews': {'$sum': 1}}},
            {'$sort': {'total_reviews': -1}},
            {'$limit': limit}
        ]
        limited_items = collection.aggregate(aggregation_pipeline_1)
        limited_items = [item['_id'] for item in limited_items]
        aggregation_pipeline = [
            {'$match': {'item_id': {'$in': limited_items}}},
            {'$group': {'_id': {'item_id': '$item_id', 'overall': '$overall'}, 'num_reviews': {'$sum': 1}}},
            {'$group': {'_id': '$_id.item_id', 'reviews': {'$push': {'overall': '$_id.overall',
                                                                     'num_reviews': '$num_reviews'}},
                        'total_reviews': {'$sum': '$num_reviews'}}},
            {'$sort': {'total_reviews': -1}},
            {'$project': {'reviews': 1, '_id': 1}}
        ]
    else:
        aggregation_pipeline = [
            {'$group': {'_id': {'item_id': '$item_id', 'overall': '$overall'}, 'num_reviews': {'$sum': 1}}},
            {'$group': {'_id': '$_id.item_id', 'reviews': {'$push': {'overall': '$_id.overall',
                                                                     'num_reviews': '$num_reviews'}},
                        'total_reviews': {'$sum': '$num_reviews'}}},
            {'$sort': {'total_reviews': -1}},
            {'$project': {'reviews': 1, '_id': 1}}
        ]

    response = list(collection.aggregate(aggregation_pipeline))
    good_reviews = {}
    bad_reviews = {}
    for item in response:
        item_id = item['_id']
        reviews = item['reviews']
        for review in reviews:
            overall = review['overall']
            num_reviews = review['num_reviews']
            if overall >= 4:
                good_reviews[item_id] = good_reviews.get(item_id, 0) + num_reviews
            else:
                bad_reviews[item_id] = bad_reviews.get(item_id, 0) + num_reviews
    fig = go.Figure()
    fig.add_trace(go.Bar(y=list(good_reviews.keys()), x=list(good_reviews.values()), orientation='h',
                         name='Good reviews', marker_color='green'))
    fig.add_trace(go.Bar(y=list(bad_reviews.keys()), x=list(bad_reviews.values()), orientation='h',
                         name='Bad reviews', marker_color='red'))
    num_items = max(len(good_reviews), len(bad_reviews))
    fig.update_layout(
        height=30 * num_items if num_items > 10 else 300,
        margin=dict(t=30),
        barmode='stack'
    )
    fig.update_yaxes(title='Items', automargin=True, autorange='reversed')
    fig.update_xaxes(title='Number of reviews')
    return fig


def generate_fig3(collection, search_field: str, values: Collection[str]):
    if search_field not in ['item_id', 'category']:
        raise ValueError("Unknown search field. Available search fields are ['asin', 'category']")
    response = list(collection.aggregate([
        {'$match': {search_field: {'$in': values}}},
        {'$group': {'_id': {'overall': '$overall', search_field: f'${search_field}'},
                    'item_count': {'$sum': 1}}},
        {'$sort': {'_id.overall': 1, f'_id.{search_field}': 1}}
    ]))
    colors = ['red', 'orange', 'yellow', 'yellowgreen', 'green']
    color_ratings = [px.colors.sequential.Reds,
                     px.colors.sequential.Oranges,
                     px.colors.sequential.YlOrBr,
                     px.colors.sequential.YlGn,
                     px.colors.sequential.Greens]
    ratings = {}
    subgroup_names = []
    subgroup_reviews = []
    outer_colors = []
    inner_colors = []
    j = 2
    for data in response:
        rating_name, reviews = data.values()
        rating, name = rating_name.values()
        if rating not in ratings:
            outer_colors.append(colors[int(rating) - 1])
            ratings[rating] = 0
            j = 2
        ratings[rating] += reviews
        if len(name) > 25:
            name = f'{name[:25]}...'
        subgroup_names.append(name)
        subgroup_reviews.append(reviews)
        inner_colors.append(color_ratings[int(rating) - 1][j])
        j += 1

    # Create traces
    trace1 = go.Pie(
        hole=.5,
        sort=False,
        direction='clockwise',
        domain={'x': [0.15, 0.85], 'y': [0.15, 0.85]},
        values=subgroup_reviews,
        text=subgroup_names,
        textinfo='text',
        textposition='inside',
        marker={'line': {'color': 'white', 'width': 1},
                'colors': inner_colors},
        showlegend=False
    )
    trace2 = go.Pie(
        hole=0.7,
        sort=False,
        direction='clockwise',
        labels=list(ratings.keys()),
        values=list(ratings.values()),
        textinfo='label',
        textposition='outside',
        marker={'line': {'color': 'white', 'width': 1},
                'colors': outer_colors},
        showlegend=True
    )

    fig = go.Figure(data=[trace1, trace2])
    return fig


def generate_fig4(collection, categories, start_date=None, end_date=None):
    match = {'category': {'$in': categories}}
    if start_date:
        match['reviewTime'] = {'$gte': start_date}
    if end_date:
        if 'reviewTime' in match:
            match['reviewTime']['$lte'] = end_date
        else:
            match['reviewTime'] = {'$lte': end_date}
    response = list(collection.aggregate([
        {'$match': match},
        {'$group': {'_id': {'date': '$reviewTime', 'category': '$category'}, 'review_count': {'$sum': 1}}},
        {'$sort': {'_id.date': 1}}
    ]))
    dates = []
    num_reviews = {category: [] for category in categories}
    num_reviews['Total'] = []
    reviews_cumulative = {category: 0 for category in categories}
    reviews_cumulative['Total'] = 0
    for data in response:
        date_category, reviews = data.values()
        date, category = date_category.values()
        reviews_cumulative[category] += reviews
        reviews_cumulative['Total'] += reviews
        if date not in dates:
            dates.append(date)
            for cat in categories:
                num_reviews[cat].append(reviews_cumulative[cat])
            num_reviews['Total'].append(reviews_cumulative['Total'])

    fig = go.Figure()
    colors = ['#883000', '#CB5C0D', '#FD6A02', '#EF820D', '#FDA50F', '#FFBF00', '#F8DE7E', '#FFED83']
    if len(categories) > 1:
        for i, category in enumerate(categories + ['Total']):
            fig.add_trace(
                go.Scatter(x=dates, y=num_reviews[category], name=category, line=dict(color=colors[i % len(colors)])))
    else:
        fig.add_trace(go.Scatter(x=dates, y=num_reviews[categories[0]], name=categories[0], line=dict(color=colors[0])))
    fig.update_xaxes(title='Date')
    fig.update_yaxes(title='Number of reviews')
    return fig


def generate_fig5(collection, limit=None):
    if limit is not None:
        aggregation_pipeline_1 = [
            {'$group': {'_id': '$reviewer_id', 'total_reviews': {'$sum': 1}}},
            {'$sort': {'total_reviews': -1}},
            {'$limit': limit},
        ]
        limited_users = collection.aggregate(aggregation_pipeline_1)
        limited_users = [user['_id'] for user in limited_users]
        aggregation_pipeline = [
            {'$match': {'reviewer_id': {'$in': limited_users}}},
            {'$group': {'_id': {'reviewer_id': '$reviewer_id', 'overall': '$overall'}, 'num_reviews': {'$sum': 1}}},
            {'$group': {'_id': '$_id.reviewer_id', 'reviews': {'$push': {'overall': '$_id.overall',
                                                                         'num_reviews': '$num_reviews'}},
                        'total_reviews': {'$sum': '$num_reviews'}}},
            {'$sort': {'total_reviews': -1}},
            {'$project': {'reviews': 1, '_id': 1}}
        ]
    else:
        aggregation_pipeline = [
            {'$group': {'_id': {'reviewer_id': '$reviewer_id', 'overall': '$overall'}, 'num_reviews': {'$sum': 1}}},
            {'$group': {'_id': '$_id.reviewer_id', 'reviews': {'$push': {'overall': '$_id.overall',
                                                                         'num_reviews': '$num_reviews'}},
                        'total_reviews': {'$sum': '$num_reviews'}}},
            {'$sort': {'total_reviews': -1}},
            {'$project': {'reviews': 1, '_id': 1}}
        ]

    response = list(collection.aggregate(aggregation_pipeline))
    good_reviews = {}
    bad_reviews = {}
    for item in response:
        reviewer_id = item['_id']
        reviews = item['reviews']
        for review in reviews:
            overall = review['overall']
            num_reviews = review['num_reviews']
            if overall >= 4:
                good_reviews[reviewer_id] = good_reviews.get(reviewer_id, 0) + num_reviews
            else:
                bad_reviews[reviewer_id] = bad_reviews.get(reviewer_id, 0) + num_reviews
    fig = go.Figure()
    fig.add_trace(go.Bar(y=list(good_reviews.keys()), x=list(good_reviews.values()), orientation='h',
                         name='Good reviews', marker_color='green'))
    fig.add_trace(go.Bar(y=list(bad_reviews.keys()), x=list(bad_reviews.values()), orientation='h',
                         name='Bad reviews', marker_color='red'))
    num_users = max(len(good_reviews), len(bad_reviews))
    fig.update_layout(
        height=30 * num_users if num_users > 10 else 300,
        margin=dict(t=30),
        plot_bgcolor='rgba(0,0,0,0)',
        barmode='stack'
    )
    fig.update_yaxes(title='Users', automargin=True, autorange='reversed')
    fig.update_xaxes(title='Number of reviews')
    return fig


def generate_fig6(collection, category: str):
    response = list(collection.find(
        {'category': category},
        {'summary': 1, '_id': 0}
    ))
    total_words = [data.get('summary') for data in response]
    wc_text = ' '.join(total_words)
    shopping_mask = np.array(Image.open('amazon.png'))
    image_colors = ImageColorGenerator(shopping_mask)
    wc = WordCloud(background_color="white", max_words=150, mask=shopping_mask,
                   max_font_size=500, min_word_length=3, random_state=42)
    wc.generate(wc_text)
    fig6 = px.imshow(wc.recolor(color_func=image_colors))
    fig6.update_layout(
        height=400
    )
    fig6.update_xaxes(visible=False)
    fig6.update_yaxes(visible=False)
    return fig6


def generate_fig7(collection, user_ids: Collection[str]):
    G = nx.Graph()
    for user_id in user_ids:
        user_reviews = collection.find({'reviewer_id': user_id})
        for review in user_reviews:
            G.add_node(review['reviewer_id'], type='user')
            G.add_node(review['item_id'], type='item')
            G.add_edge(review['reviewer_id'], review['item_id'], type='review')
    pos = nx.spring_layout(G)
    edge_trace = go.Scatter(
        x=[],
        y=[],
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')
    for edge in G.edges():
        x0, y0 = pos[edge[0]]
        x1, y1 = pos[edge[1]]
        edge_trace['x'] += tuple([x0, x1, None])
        edge_trace['y'] += tuple([y0, y1, None])
    node_trace = go.Scatter(
        x=[],
        y=[],
        text=[],
        mode='markers',
        hoverinfo='text',
        marker=dict(
            showscale=True,
            colorscale='Greys',
            reversescale=True,
            color=[],
            size=10,
            colorbar=dict(
                thickness=15,
                title='Number of reviews',
                xanchor='left',
                titleside='right'
            ),
            line=dict(width=2)))
    for node in G.nodes():
        x, y = pos[node]
        node_trace['x'] += tuple([x])
        node_trace['y'] += tuple([y])
        if G.nodes[node]['type'] == 'item':
            node_trace['marker']['color'] += tuple(['#ff9900'])
        else:
            num_reviews = len(G.adj[node])
            node_trace['marker']['color'] += tuple([num_reviews])
    for node, adjacencies in enumerate(G.adjacency()):
        if G.nodes[adjacencies[0]]['type'] == 'user':
            node_info = 'User Id: ' + str(adjacencies[0]) + '<br>Reviews: ' + str(len(adjacencies[1]))
        else:
            node_info = 'Item Id: ' + str(adjacencies[0]) + '<br>Reviews: ' + str(len(adjacencies[1]))
        node_trace['text'] += tuple([node_info])
    fig = go.Figure(
        data=[edge_trace, node_trace],
        layout=go.Layout(
            height=800,
            titlefont=dict(size=16),
            showlegend=False,
            hovermode='closest',
            margin=dict(b=10, l=5, r=5, t=40),
            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
        )
    )
    return fig
