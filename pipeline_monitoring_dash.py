from dash import Dash, dcc, html, Output, Input, State, ctx
import pandas as pd
import sqlalchemy as sa
import os
import psutil
import signal
import singlestoredb as s2
import plotly.express as px
from dash.dependencies import ALL
import json
import dash_daq as daq
import logging

logging.basicConfig(level = logging.INFO)

# Function to kill processes using the specified port
def find_process_by_port(port):
    for proc in psutil.process_iter(['pid']):
        try:
            connections = proc.connections()
            for conn in connections:
                if conn.laddr.port == port:
                    return proc
        except psutil.AccessDenied:
            pass
    return None

# Kill any process using the port
dash_port = 8050
process = find_process_by_port(dash_port)

if process and process.pid != os.getpid():
    print(f"Killing process {process.pid} which is using port {dash_port}")
    os.kill(process.pid, signal.SIGKILL)

# SQLAlchemy connection setup
def create_db_connection():
    return s2.connect('<ADD CONNECTION STRING HERE>')

# Function to get list of databases
def get_databases():
    query = "SHOW DATABASES;"
    df = pd.read_sql(query, sa_conn)
    return [{'label': db, 'value': db} for db in df['Database']]

# Function to get pipelines for a given database
def get_pipelines(database_name):
    query = f"SELECT pipeline_name FROM information_schema.pipelines WHERE database_name = '{database_name}';"
    df = pd.read_sql(query, sa_conn)
    return [{'label': pipeline, 'value': pipeline} for pipeline in df['pipeline_name']]

# Function to get files for a given pipeline and database
def get_files(database_name, pipeline_name):
    query = f"""
    SELECT file_name, file_state
    FROM information_schema.pipelines_files
    WHERE database_name = '{database_name}' AND pipeline_name = '{pipeline_name}'
    ORDER BY file_state DESC
    LIMIT 50;
    """
    df = pd.read_sql(query, sa_conn)
    return df[['file_name', 'file_state']]

# Function to get pipeline config details
def get_pipeline_config(database_name, pipeline_name):
    query = f"""
    SELECT config_json
    FROM information_schema.pipelines
    WHERE database_name = '{database_name}' AND pipeline_name = '{pipeline_name}';
    """
    df = pd.read_sql(query, sa_conn)
    if df.empty:
        return {}

    config_json = df.iloc[0]['config_json']
    config = json.loads(config_json)
    return {
        'source': config.get('connection_string'),
        'source_type': config.get('source_type'),
        'data_format': config.get('data_format'),
        'stop_on_error': config.get('stop_on_error')
    }
    
def get_latency(database_name, pipeline_name):
    query = f"""
    SELECT database_name, pipeline_name, SUM(cursor_offset - latest_offset) as latency
        FROM information_schema.pipelines_cursors
        WHERE database_name = '{database_name}' 
        AND pipeline_name = '{pipeline_name}'
        GROUP BY 1,2;
    """
    df = pd.read_sql(query, sa_conn)
    if df.empty:
        return {}
    return df.iloc[0]['latency']

# Initialize Dash app
sa_conn = create_db_connection()
app = Dash(__name__)
app.layout = html.Div([
    dcc.Store(id='selected-database', data=None),
    dcc.Store(id='selected-pipeline', data=None),
    dcc.Store(id='selected-error-file', data=None),  # Store for the selected error file

    # Add the ConfirmDialog component
    dcc.ConfirmDialog(
        id='error-alert',
        message='Error message goes here',
        displayed=False  # Initially hidden
    ),

    html.Div(
        children=[
            html.Img(
                src='https://seeklogo.com/images/S/singlestore-logo-CBD32FECEE-seeklogo.com.png',  # Replace with the path to your logo image
                style={
                    'height': '50px',  # Adjust the size as needed
                    'marginRight': '20px',
                    'verticalAlign': 'middle'
                }
            ),
            html.H1(
                "Pipeline Monitoring Dashboard",
                style={
                    'display': 'inline-block',
                    'textAlign': 'center',
                    'padding': '20px',
                    # 'backgroundColor': '#D8BFD8',  # Light purple background
                    'color': 'rgb(255,255,255)',  # Black text color
                    # 'border': '2px solid #800080',  # Dark purple border
                    'borderRadius': '10px',
                    'marginBottom': '20px',
                    'fontSize': '2.5rem',
                    'margin': '0'  # Remove default margin to align with the image
                }
            )
        ],
        style={
            'textAlign': 'center',  # Center the content of the div
            'display': 'flex',
            'alignItems': 'center',
            'justifyContent': 'center',
            'backgroundColor': 'rgb(50,50,50)',  # Light purple background
            # 'border': '2px solid #800080',  # Dark purple border
            'borderRadius': '10px',
            'padding': '10px',
            'height':'60px',
            'marginBottom':'20px'
        }
    ),

    html.Div([
        html.Div([
            html.Div([
                html.Div([
                    html.Label('Select Database', style={'fontSize': '1.2rem', 'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='database-dropdown',
                        options=get_databases(),
                        placeholder='Select a database',
                        style={'border': '2px solid #9A1DD2', 'borderRadius': '5px'}
                    ),
                ], style={
                    'border': '2px solid #9A1DD2',
                    'borderRadius': '10px',
                    'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                    'backgroundColor': '#fff',
                    'padding': '10px',
                    'marginBottom': '20px',
                    'flex': '1',
                    'marginTop':'20px'
                }),
                html.Div([
                    html.Label('Select Pipeline', style={'fontSize': '1.2rem', 'fontWeight': 'bold'}),
                    dcc.Dropdown(
                        id='pipeline-dropdown',
                        placeholder='Select a pipeline',
                        style={'border': '2px solid #9A1DD2', 'borderRadius': '5px'}
                    ),
                ], style={
                    'border': '2px solid #9A1DD2',
                    'borderRadius': '10px',
                    'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                    'backgroundColor': '#fff',
                    'padding': '10px',
                    'marginBottom': '20px',
                    'flex': '1',
                    'marginTop':'20px'
                })
            ], style={
                'display': 'flex',
                'gap': '10px',
                'width': '100%'
            }),
            html.Div([
                html.Div(
                    children=[
                        # html.Label("Data Ingestion Speed", style={'position': 'absolute', 'top': '10px', 'left': '10px'}),
                        daq.Gauge(
                            id='speedometer',
                            label='Data Ingestion Speed',
                            min=0,
                            max=500,
                            size=150,
                            showCurrentValue=True,
                            value=0,
                            style={'width': '100%', 'height': 'auto', 'marginTop': '10px'}
                        ),
                        html.Div(id='speed-units', style={'fontSize': '18px', 'marginBottom':'-30px'}),
                        dcc.Dropdown(
                            id='speed-dropdown',
                            options=[
                                {'label': 'Batches/sec', 'value': 'Batches/sec'},
                                {'label': 'Rows/sec', 'value': 'Rows/sec'},
                                {'label': 'KBs/sec', 'value': 'KBs/sec'}
                            ],
                            value='Rows/sec',
                            style={'width': '80%', 'margin': '2px auto', 'height': 'auto', 'marginTop':'0px'}
                        ),
                    ],
                    style={
                        'height': '28vh',  # Same height as the pie chart div
                        'border': '2px solid #9A1DD2',
                        'borderRadius': '10px',
                        'backgroundColor': '#fff',
                        'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                        'padding': '10px',
                        'width': '40%'
                    }
                ),
                dcc.Graph(id='file-states-pie-chart', style={
                    'height': '28vh',  # 1/4th of the original height
                    'border': '2px solid #9A1DD2',
                    'borderRadius': '10px',
                    'backgroundColor': '#fff',
                    'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                    'padding': '10px',
                    'width': '40%'
                }),
                html.Div(
                    children=[
                        html.Div("Ingestion Lag", style={'fontSize': '24px', 'fontWeight': 'bold', 'marginBottom':'20px'}),
                        html.Div(id='latency-output', style={'fontSize': '36px', 'align':'center'})
                    ],
                    style={
                        'height': '28vh',
                        'border': '2px solid #9A1DD2',
                        'borderRadius': '10px',
                        'backgroundColor': '#fff',
                        'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                        'padding': '10px',
                        'width': '20%',
                        'display': 'flex',
                        'flexDirection': 'column',
                        'alignItems': 'center',  # Center horizontally
                        'justifyContent': 'center'  # Center vertically
                    }
                )
            ], style={
                'display': 'flex',
                'flexDirection': 'row',
                'gap': '10px',
                'height': '32vh'
            }),
            html.Div([
                dcc.Graph(id='ingestion-speed-graph',
                          config={'responsive': True},
                          style = {'width': '100%'})
            ], style={
                # 'height': '28vh',  # Same height as the other divs
                'border': '2px solid #9A1DD2',
                'borderRadius': '10px',
                'backgroundColor': '#fff',
                'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                'padding': '10px',
                # 'width': '100%',
                'marginTop': '10px',
                'display': 'flex',
                'flexDirection': 'row',
                'gap': '10px',
                'height': '32vh'
            })
        ], style={
            'flex': '2',
            'padding': '10px',
            'marginRight': '10px'
        }),

        html.Div([
            html.H3("Pipeline Configuration", style={
                'color': '#9A1DD2',
                'marginBottom': '20px'
            }),
            html.Div(id='pipeline-config-details', style={
                'display': 'grid',
                'gridTemplateColumns': 'repeat(4, 1fr)',  # 4 columns each taking 25% width
                'gap': '10px',
                'padding': '10px',
                'backgroundColor': '#eaf2f8',
                'borderRadius': '10px',
                'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
                'fontSize': '1rem',
                'marginBottom': '20px'
            }, children=[
                # Card for Source
                html.Div([
                    html.H4("Source", style={'margin': '0', 'color': '#333'}),
                    html.P("Source Name Here", style={'margin': '5px 0', 'color': '#555'})
                ], style={
                    'border': '2px solid #9A1DD2',
                    'borderRadius': '5px',
                    'padding': '10px',
                    'backgroundColor': '#fff',
                    'boxShadow': '1px 1px 5px rgba(0,0,0,0.1)',
                    'fontSize': '0.9rem'
                }),

                # Card for Source Type
                html.Div([
                    html.H4("Source Type", style={'margin': '0', 'color': '#333'}),
                    html.P("Source Type Here", style={'margin': '5px 0', 'color': '#555'})
                ], style={
                    'border': '2px solid #9A1DD2',
                    'borderRadius': '5px',
                    'padding': '10px',
                    'backgroundColor': '#fff',
                    'boxShadow': '1px 1px 5px rgba(0,0,0,0.1)',
                    'fontSize': '0.9rem'
                }),

                # Card for Data Format
                html.Div([
                    html.H4("Data Format", style={'margin': '0', 'color': '#333'}),
                    html.P("Data Format Here", style={'margin': '5px 0', 'color': '#555'})
                ], style={
                    'border': '2px solid #9A1DD2',
                    'borderRadius': '5px',
                    'padding': '10px',
                    'backgroundColor': '#fff',
                    'boxShadow': '1px 1px 5px rgba(0,0,0,0.1)',
                    'fontSize': '0.9rem'
                }),

                # Card for Stop on Error
                html.Div([
                    html.H4("Stop on Error", style={'margin': '0', 'color': '#333'}),
                    html.P("Yes/No", style={'margin': '5px 0', 'color': '#555'})
                ], style={
                    'border': '2px solid #9A1DD2',
                    'borderRadius': '5px',
                    'padding': '10px',
                    'backgroundColor': '#fff',
                    'boxShadow': '1px 1px 5px rgba(0,0,0,0.1)',
                    'fontSize': '0.9rem'
                })
            ]),
            html.H3("List of files", style={
                'color': '#9A1DD2',
                'marginBottom': '20px'
            }),
            html.Div(id='file-list', style={  # Moved the file list here
                'overflowY': 'scroll',
                'height': '47vh',
                'border': '2px solid #9A1DD2',
                'padding': '10px',
                'borderRadius': '10px',
                'backgroundColor': '#eaf2f8',
                'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)'
            })
        ], style={
            'flex': '1',
            'padding': '10px',
            'border': '2px solid #9A1DD2',
            'borderRadius': '10px',
            'boxShadow': '2px 2px 10px rgba(0,0,0,0.2)',
            'backgroundColor': '#fff'
        })
    ], style={
        'display': 'flex',
        'flexDirection': 'row',
        'flexWrap': 'wrap'
    }),
    dcc.Interval(
        id='interval-component',
        interval=2*1000,
        n_intervals=0
    )
], style={
    'padding': '20px',
    'fontFamily': 'Arial, sans-serif',
    'backgroundColor': '#f4f4f4'
})



# Callback to update pipeline dropdown based on selected database and store the database name
@app.callback(
    Output('pipeline-dropdown', 'options'),
    Output('selected-database', 'data'),
    Input('database-dropdown', 'value')
)
def update_pipelines(selected_database):
    if selected_database is None:
        return [], None
    pipelines = get_pipelines(selected_database)
    return pipelines, selected_database

@app.callback(
    [Output('file-list', 'children'),
     Output('file-states-pie-chart', 'figure'),
     Output('pipeline-config-details', 'children'),
     Output('latency-output', 'children'),
     Output('speedometer', 'value'),
     Output('ingestion-speed-graph', 'figure')],
    [Input('pipeline-dropdown', 'value'),
     Input('interval-component', 'n_intervals')],
    [State('speed-dropdown', 'value'),
     State('selected-database', 'data')]
)
def update_files(selected_pipeline, n_intervals, speed_type, selected_database):
    if selected_database is None or selected_pipeline is None:
        return [], {}, "", 0, 0, {}

    try:
        files_df = get_files(selected_database, selected_pipeline)
        pipeline_config = get_pipeline_config(selected_database, selected_pipeline)
        file_list = []
        file_state_counts = {'Loaded': 0, 'Skipped': 0, 'Unloaded': 0}
        
        loaded_query = f"""
        select file_state, count(*) as count
        from information_schema.pipelines_files
        where database_name = '{selected_database}' and pipeline_name = '{selected_pipeline}'
        group by file_state;
        """
        print(loaded_query)
        file_state_result = pd.read_sql(loaded_query, sa_conn)
        for _, row in file_state_result.iterrows():
            file_state_counts[row['file_state']] = row['count']
            
        print(file_state_counts)
       
        for _, row in files_df.iterrows():
            file_name = row['file_name']
            file_state = row['file_state']
            show_error_button = html.Span()
           
            if file_state == 'Loaded':
                status_icon = html.Img(src='https://img.icons8.com/?size=100&id=82881&format=png&color=40C057', style={'width': '20px', 'height': '20px'})
            elif file_state == 'Skipped':
                status_icon = html.Img(src='https://img.icons8.com/?size=100&id=23543&format=png&color=FA5252', style={'width': '20px', 'height': '20px'})
                show_error_button = html.Button('Show Error', id={'type': 'error-button', 'index': file_name}, style={'marginLeft': '10px'})
            elif file_state == 'Unloaded':
                status_icon = html.Img(src='https://img.icons8.com/?size=100&id=11334&format=png&color=228BE6', style={'width': '20px', 'height': '20px'})
           
            file_list.append(html.Div([
                html.Span(file_name, style={'flex': '1'}),
                status_icon,
                show_error_button
            ], style={
                'display': 'flex',
                'alignItems': 'center',
                'padding': '10px',
                'borderBottom': '1px solid #ddd',
                'backgroundColor': '#f9f9f9',
                'marginBottom': '5px',
                'borderRadius': '5px'
            }))
       
        # Create the pie chart figure
        labels = list(file_state_counts.keys())
        values = list(file_state_counts.values())
        colors = ['#40C057', '#FA5252', '#228BE6']  # Green, Red, Blue

        fig = px.pie(
            names=labels,
            values=values,
            title='Pipeline Ingestion State',
            color=labels,
            color_discrete_sequence=['green', 'red', 'blue'],
            hole=.35
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(
            margin=dict(l=36, r=36, t=40, b=20),
            height=None,
            width=None,
            autosize=True,
            template='plotly_white'
        )


        # Pipeline config details
        source_type_content = pipeline_config.get('source_type', 'N/A')
        if source_type_content == 'S3':
            source_type_content = html.Div([
                html.Img(src='https://img.icons8.com/?size=100&id=Gk2QpGf92IzK&format=png&color=000000', style={'width': '40px', 'height': '40px'}),
                html.P('S3', style={'marginLeft': '10px', 'fontSize': '1rem', 'color': '#555'})
            ], style={'display': 'flex', 'alignItems': 'center'})
        if source_type_content == 'FS':
            source_type_content = html.Div([
                html.Img(src='https://img.icons8.com/?size=100&id=2939&format=png&color=000000', style={'width': '40px', 'height': '40px'}),
                html.P('FS', style={'marginLeft': '10px', 'fontSize': '1rem', 'color': '#555'})
            ], style={'display': 'flex', 'alignItems': 'center'})
        if source_type_content == 'KAFKA':
            source_type_content = html.Div([
                html.Img(src='https://img.icons8.com/?size=100&id=fOhLNqGJsUbJ&format=png&color=000000', style={'width': '40px', 'height': '40px'}),
                html.P('KAFKA', style={'marginLeft': '10px', 'fontSize': '1rem', 'color': '#555'})
            ], style={'display': 'flex', 'alignItems': 'center'})
        else:
            source_type_content = html.P(source_type_content, style={'margin': '5px 0', 'color': '#555'})

        stop_on_error = pipeline_config.get('stop_on_error', 'N/A')
        if not stop_on_error:
            default_query = "SELECT @@pipelines_stop_on_error;"
            try:
                default_df = pd.read_sql(default_query, sa_conn)
                default_value = default_df.iloc[0, 0]
                stop_on_error = 'On' if default_value == 1 else 'Off'
            except Exception as e:
                print(f"An error occurred while fetching default stop_on_error value: {e}")
                stop_on_error = 'Unknown'
        else:
            stop_on_error = 'On' if stop_on_error == 1 else 'Off'
       
        pipeline_config_details = [
            # Card for Source
            html.Div([
                html.H4("Source", style={'margin': '0', 'color': '#333'}),
                html.P(pipeline_config.get('source', 'N/A'), style={'margin': '5px 0', 'color': '#555'})
            ], style={
                'border': '2px solid #9A1DD2',
                'borderRadius': '5px',
                'padding': '10px',
                'backgroundColor': '#fff',
                'boxShadow': '1px 1px 5px rgba(0,0,0,0.1)',
                'fontSize': '0.9rem'
            }),

            # Card for Source Type
            html.Div([
                html.H4("Source Type", style={'margin': '0', 'color': '#333'}),
                source_type_content
            ], style={
                'border': '2px solid #9A1DD2',
                'borderRadius': '5px',
                'padding': '10px',
                'backgroundColor': '#fff',
                'boxShadow': '1px 1px 5px rgba(0,0,0,0.1)',
                'fontSize': '0.9rem'
            }),

            # Card for Data Format
            html.Div([
                html.H4("Data Format", style={'margin': '0', 'color': '#333'}),
                html.P(pipeline_config.get('data_format', 'N/A'), style={'margin': '5px 0', 'color': '#555'})
            ], style={
                'border': '2px solid #9A1DD2',
                'borderRadius': '5px',
                'padding': '10px',
                'backgroundColor': '#fff',
                'boxShadow': '1px 1px 5px rgba(0,0,0,0.1)',
                'fontSize': '0.9rem'
            }),

            # Card for Stop on Error
            html.Div([
                html.H4("Stop on Error", style={'margin': '0', 'color': '#333'}),
                html.P(stop_on_error, style={'margin': '5px 0', 'color': '#555'})
            ], style={
                'border': '2px solid #9A1DD2',
                'borderRadius': '5px',
                'padding': '10px',
                'backgroundColor': '#fff',
                'boxShadow': '1px 1px 5px rgba(0,0,0,0.1)',
                'fontSize': '0.9rem'
            })
        ]
        
        latency_query = f"""
        SELECT database_name, pipeline_name, SUM(cursor_offset - latest_offset) as latency
        FROM information_schema.pipelines_cursors
        WHERE database_name = '{selected_database}' 
        AND pipeline_name = '{selected_pipeline}'
        GROUP BY 1,2;
        """
        df = pd.read_sql(latency_query, sa_conn)

        
        speed_query = f"""
        select START_TIME, ROWS_PER_SEC, BATCH_TIME, MB_PER_SEC from information_schema.pipelines_batches_summary where batch_state = 'Succeeded' and start_time > (select now() - 600) and pipeline_name = '{selected_pipeline}' and database_name = '{selected_database}' order by start_time desc limit 100;
        """
        print("executing: ", speed_query)
        result = pd.read_sql(speed_query, sa_conn)
        if not result.empty:
            if speed_type == 'Rows/sec':
                speed_value = result.iloc[0]['ROWS_PER_SEC']
                y_values = result['ROWS_PER_SEC']
                y_label = 'Rows/sec'
                y_max = 1500
            elif speed_type == 'KBs/sec':
                speed_value = result.iloc[0]['MB_PER_SEC'] * 1024
                y_values = result['MB_PER_SEC'] * 1024
                y_label = 'KBs/sec'
                y_max = 100
            elif speed_type == 'Batches/sec':
                speed_value = 1 / result.iloc[0]['BATCH_TIME']
                y_values = 1 / result['BATCH_TIME']
                y_label = 'Batches/sec'
                y_max = 5
            else:
                speed_value = 0
                y_values = []
                y_label = speed_type
                y_max = 0
        else:
            speed_value = 0
            y_values = []   
            y_label = speed_type
            y_max = 0
        formatted_speed_value = float(f"{speed_value:.3f}")
        logging.info(f"Updating speedometer to {formatted_speed_value} {speed_type}")
        
        if result.empty:
            graph_fig = px.line()
        else:        
            graph_fig = px.line(
                result,
                x=result['START_TIME'],
                y=y_values,
                title=f'Ingestion Performance',
                labels={'x': 'Time', 'y': y_label}
            )
            graph_fig.update_layout(yaxis_range=[0, y_max])
            graph_fig.update_layout(
                margin=dict(l=0, r=0, t=40, b=40),
                height=None,
                width=None,
                autosize=True,
                template='plotly_white'
            )
        logging.info(f"Updating graph to {formatted_speed_value} {speed_type}")
        
        
        return file_list, fig, pipeline_config_details, df.iloc[0]['latency'], formatted_speed_value, graph_fig
   
    except Exception as e:
        print(f"Error updating files: {e}")
        return [], {}, "", 0, 0, {}
    
@app.callback(
    Output('speedometer', 'max'),
    Input('speed-dropdown', 'value')
)
def update_speedometer_max(speed_type):
    if speed_type == 'Rows/sec':
        return 1500
    elif speed_type == 'KBs/sec':
        return 100
    elif speed_type == 'Batches/sec':
        return 5
    return 0



@app.callback(
    Output('error-alert', 'displayed'),
    Output('error-alert', 'message'),
    Input({'type': 'error-button', 'index': ALL}, 'n_clicks'),
    State('selected-database', 'data'),
    State('pipeline-dropdown', 'value')
)
def show_error_alert(n_clicks, selected_database, selected_pipeline):
    if not any(n_clicks):
        return False, ''
   
    # Get the index of the clicked button
    clicked_button_id = ctx.triggered_id
    selected_file = clicked_button_id['index']
   
    # Query to get error details
    query = f"""
    SELECT DISTINCT ERROR_MESSAGE
    FROM information_schema.pipelines_errors
    WHERE BATCH_SOURCE_PARTITION_ID = '{selected_file}'
    AND PIPELINE_NAME = '{selected_pipeline}';
    """

    try:
        df = pd.read_sql(query, sa_conn)
        error_messages = df['ERROR_MESSAGE'].tolist()
       
        if not error_messages:
            return False, 'No error details available for this file.'

        # Concatenate all error messages
        error_message = '\n'.join(error_messages)
        return True, error_message
   
    except Exception as e:
        print(f"An error occurred while fetching error details: {e}")
        return True, 'Failed to retrieve error details.'    

if __name__ == '__main__':
    app.run_server(host="0.0.0.0", port = dash_port)
