import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import pymongo
import pandas as pd
import plotly.express as px

# Initialize MongoDB Connection
mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
db = mongo_client["user_profiles"]
collection = db["user_collection"]

# Fetch Data from MongoDB
def get_mongo_data():
    cursor = collection.find({}, {"_id": 0, "email": 1, "nationality": 1, "age": 1, "country": 1})
    data = list(cursor)
    return data

# Initialize Dash App
app = dash.Dash(__name__)

# Define Layout
app.layout = html.Div([
    html.H1("User Profiles Analysis Dashboard", style={'textAlign': 'center'}),
    
    html.Button("Aggregate Data", id="aggregate-button", style={'margin': '20px'}),
    
    dcc.Graph(id='user-count-bar', config={'displayModeBar': False}),
    dcc.Graph(id='average-age-bar', config={'displayModeBar': False}),
    dcc.Graph(id='average-age-by-country-map', config={'displayModeBar': False}),  # Choropleth map
    dcc.Graph(id='email-domains-pie', config={'displayModeBar': False}),
])

# Callbacks to update graphs
@app.callback(
    [Output('user-count-bar', 'figure'),
     Output('average-age-bar', 'figure'),
     Output('average-age-by-country-map', 'figure'),  # Choropleth map
     Output('email-domains-pie', 'figure')],
    [Input('aggregate-button', 'n_clicks')]
)
def update_graphs(n_clicks):
    data = get_mongo_data()
    df = pd.DataFrame(data)
    
    # Aggregation: Number of Users by Nationality
    user_count_agg = df.groupby("nationality").size().reset_index(name='user_count')
    user_count_fig = px.bar(user_count_agg, x='nationality', y='user_count', labels={'user_count': 'User Count'})
    user_count_fig.update_layout(
        title_text='Number of Users by Nationality',
        xaxis_title="Nationality",
        yaxis_title="User Count",
        showlegend=True,
        legend_title_text="Legend Title",
    )
    
    # Aggregation: Average Age of Users by Nationality
    avg_age_agg = df.groupby("nationality")['age'].mean().reset_index(name='avg_age')
    avg_age_bar_fig = px.bar(avg_age_agg, x='nationality', y='avg_age', labels={'avg_age': 'Average Age'})
    avg_age_bar_fig.update_layout(
        title_text='Average Age of Users by Nationality',
        xaxis_title="Nationality",
        yaxis_title="Average Age",
        showlegend=True,
        legend_title_text="Legend Title",
    )

    # Aggregation: Average Age of Users by Country
    avg_age_by_country_agg = df.groupby("country")['age'].mean().reset_index(name='avg_age')
    avg_age_by_country_map = px.choropleth(avg_age_by_country_agg, 
                                           locations="country", 
                                           locationmode="country names", 
                                           color="avg_age", 
                                           hover_name="country",
                                           color_continuous_scale=px.colors.sequential.Plasma,
                                           title="Average Age of Users by Country")
    
    avg_age_by_country_map.update_geos(
        showcoastlines=True, coastlinecolor="Black",
        showland=True, landcolor="white"
    )

    # Aggregation: Most Common Email Domains
    email_domains = df['email'].apply(lambda x: x.split('@')[1] if isinstance(x, str) else '').value_counts().reset_index()
    email_domains.columns = ['email_domain', 'count']
    email_domains_fig = px.pie(email_domains, names='email_domain', values='count', labels={'email_domain': 'Email Domains'})
    email_domains_fig.update_layout(
        title_text='Most Common Email Domains',
        showlegend=True,
        legend_title_text="Legend Title",
    )

    return user_count_fig, avg_age_bar_fig, avg_age_by_country_map, email_domains_fig

if __name__ == '__main__':
    app.run_server(debug=True)
