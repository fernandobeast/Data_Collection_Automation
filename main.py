# Data analysis and manipulation
import pandas as pd
import numpy as np

# Visualization
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# Web scraping
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Financial data
import yfinance as yf

# Timezone management
import pytz

# Database management
from sqlalchemy import create_engine
import psycopg2
import sql_config

# Display-related functionality
from IPython.display import display

# ANSI color formatting
from termcolor import colored

def plotly_1min_chart(plot_data,Questioning_tframe):
    
    # Variables for pre-market and post-hour annotation
    date_string = plot_data['Date'].iloc[0].split(' ', 1)[0]
    before_market_open = ' 09:29:00'
    after_market_close = ' 16:01:00'
    
    # Create subplots and mention plot grid size
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03,row_width=[0.2, 0.7],)
    
    # Plot OHLC (Open-High-Low-Close) on 1st row
    fig.add_trace(go.Candlestick(x=plot_data["Date"], open=plot_data["Open"], high=plot_data["High"],
                   low=plot_data["Low"], close=plot_data["Close"],increasing_line_color = 'green',
                   decreasing_line_color='red'),row=1, col=1)
    
    # Add grey area for pre-market and post-hour on the 1st row, if the timeframe is not 1 hour
    if Questioning_tframe.lower() != '1h':
        fig.add_vrect(x0=plot_data['Date'].iloc[0], x1= date_string + before_market_open,
                  annotation_text='', annotation_position="top left",
                  fillcolor="grey", opacity=0.25, line_width=0,row=1, col=1)

        fig.add_vrect(x0= date_string + after_market_close, x1= plot_data['Date'].iloc[-1],
                  annotation_text='', annotation_position="top left",
                  fillcolor="grey", opacity=0.25, line_width=0,row=1, col=1)
    else:
        pass
    
    # Plot bar trace for volumes on 2nd row without legend
    fig.add_trace(go.Bar(x=plot_data['Date'], y=plot_data['Volume'],marker={"color":'#0000FF'},showlegend=False), row=2, col=1)

    # Update plot layout
    fig.update(layout_xaxis_rangeslider_visible=False,layout_showlegend=False)
    fig.update_layout(title=f'{ticker} {Questioning_tframe} chart {ending_date}',title_x=0.5, xaxis_rangeslider_visible =False)
    
    # Show plot
    fig.show()
    
# Download data from different API endpoints 
def get_data_from_api(symbol,end_date,api_key,intraday=False,daily=False,start_date=''):
    
    ''' Start_date will be day -1 , end_date will be day +0.
     If set Intraday to True then it will download 1 min data on the end_date from data provider. 
     If set daily to True then it will download day -1 and day +0 data from provider. '''
    eastern_time = pytz.timezone('US/Eastern')

    if intraday == True:
        # Create API URL for intraday data
        api_url_intraday=f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{end_date}/{end_date}?adjusted=true&sort=asc&apiKey={api_key}'
        # Get data from API and convert to json
        data = requests.get(api_url_intraday).json()
        # Create pandas DataFrame from json data
        df_plot= pd.DataFrame(data['results'])
        # Transform timestamp(ms) to datetime
        df_plot['Date'] = pd.to_datetime(df_plot['t'], unit='ms')
        # Convert time to Eastern Standard Time (EST)
        df_plot['Date'] = df_plot['Date'].dt.tz_localize(pytz.utc).dt.tz_convert(eastern_time)
        # Remove timezone info(-04:00) and format the datetime column
        df_plot['Date'] = df_plot['Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S'))
        # Rename columns
        df_plot.rename(columns = {'v':'Volume', 'vw':'Vwap', 'o':'Open', 'c':'Close', 'h':'High', 'l':'Low','n':'N_of_trades'}, inplace = True)
        # Drop the 't' column(timestamp)
        df_plot = df_plot.drop(columns=['t'])
        return df_plot
    
    elif daily == True:
        # Create API URL for daily data
        api_url_day = f'https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/{start_date}/{end_date}?adjusted=true&sort=asc&limit=120&apiKey={api_key}'
        # Get data from API and convert to json
        data = requests.get(api_url_day).json()
        # Create pandas DataFrame from json data
        df_input= pd.DataFrame(data['results'])
        # Transform timestamp(ms) to datetime
        df_input['Date'] = pd.to_datetime(df_input['t'], unit='ms')
         # Convert time to Eastern Standard Time (EST)
        df_input['Date'] = df_input['Date'].dt.tz_localize(pytz.utc).dt.tz_convert(eastern_time)
        # Rename columns
        df_input.rename(columns = {'v':'Volume', 'vw':'Vwap', 'o':'Open', 'c':'Close', 'h':'High', 'l':'Low','n':'N_of_trades'},inplace=True)
        # Drop the 't' column(timestamp)
        df_input = df_input.drop(columns=['t'])
        return df_input
    else:
        print('Error: intraday or daily boolean arg has not been set')
    
# Make an timeframe plot function
def timeframe_resample_plot(plot_data,Questioning_tframe):

    # Create a copy of the original data
    timeframe_data = plot_data.copy()
    # Convert the 'Date' column to a datetime type
    timeframe_data['Date'] = pd.to_datetime(timeframe_data['Date'])
    # Set the 'Date' column as the index
    timeframe_data.set_index('Date',inplace=True)
    # Resample the data based on the specified timeframe
    # The 'closed' argument is set to 'left' meaning that the interval will be closed on the left side and open on the right side
    # The 'label' argument is set to 'left' meaning that the label for the new interval will be the left edge of the interval
    timeframe_data = timeframe_data.resample(Questioning_tframe, closed='left',label='left').agg(resample_payload).reset_index()
    # Convert the 'Date' column back to a string type
    timeframe_data['Date'] = timeframe_data['Date'].astype('str')
    # Set the index to be a datetime index based on the 'Date' column
    timeframe_data = timeframe_data.set_index(pd.DatetimeIndex(timeframe_data['Date'].values))
    # Call the 'plotly_1min_chart' function and pass in the resampled data and the questioning timeframe
    plotly_1min_chart(timeframe_data,Questioning_tframe)
    
# Pre-process 5mins high/ volume before breakout/ high of the day    
def hod_volbo_5mins_high(voldata):
    # Create a dataframe containing data from 9:30 to 9:35
    # first candle will be at 9:30, so last candle should be 9:34
    nine30_to_nine35 = voldata.loc[(voldata['Date']>= f'{ending_date} 09:30:00')&(voldata['Date'] < f'{ending_date} 09:35:00')]
    # Get the index of the 5-minute high
    idx_of_fivemins_high = nine30_to_nine35['High'].idxmax()
    # Get the 5-minute high
    fivemins_high = nine30_to_nine35.loc[idx_of_fivemins_high]
    five_mins_high.append(fivemins_high['High'])
    # Get the HOD (High of the Day)
    HOD_mkt_time = voldata.loc[(voldata['Date']>= f'{ending_date} 09:30:00')&(voldata['Date']<= f'{ending_date} 16:00:00')]
    HOD_mkt_time_idx = HOD_mkt_time['High'].idxmax()
    HOD = HOD_mkt_time.loc[HOD_mkt_time_idx,'Date']
    HOD_editing.append(HOD)
    # Get the volume before breakout time
    data_after_nine30 = voldata.loc[(voldata['Date']> f'{ending_date} 09:35:00')&(voldata['Date']<= f'{ending_date} 16:00:00')]
    for row in range(len(data_after_nine30)):
        sub_row = data_after_nine30.iloc[row]
        if sub_row['High'] > fivemins_high['High']:
            vol_b4_bo_time.append(sub_row['Date'])
    else:
        pass

def validated_input_plot(prompt, input_value):
    """ A function to receive user input and validate that it matches the acceptable inputs
    """
    # flag to track if input is valid or not
    valid_input = False
    # loop until a valid input is received
    while not valid_input:
        value = input(prompt + ' (' + '/'.join(input_value) + '): ')
        # check if input is in the list of accepted inputs
        valid_input = value.lower() in input_value
        # if input is 'q', break the loop and return
        if value.lower() in ['q','n']:
            break
    # return the received input
    return value

# Setup python connection with SQL
username = sql_config.USERNAME 
password = sql_config.PASSWORD 
ipaddress = sql_config.IPADDRESS 
port = sql_config.PORT 
dbname = sql_config.DB_NAME

# A long string that contains the necessary Postgres login information
postgres_str = f'postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'

# Create the connection
cnx = create_engine(postgres_str)

# Define variables
api_key = sql_config.API_KEY
eastern_time = pytz.timezone('US/Eastern')

# For plotting different timeframe chart
resample_payload = {'Open':'first','High':'max','Low':'min','Close':'last','Volume':'sum'}

# Variables to keep track of user choices
option_1_question = -1
option_2_question = -1
option = -1

# Start the bot
# Option 1 will download stock data and update it to SQL: daily +0 and -1, exchange traded on and sector data
# Option 2 will display intraday(1min) with volume interactive chart
# Option 3 will exit the bot

while option != 0:
    print('-----------------------------------------')
    print('Welcome to Beast\'s Data Entering Algo')
    print(colored('1. Entering new ticker data to SQL','cyan'))
    print(colored('2. Only plot the chart','blue'))
    print(colored('3. Exit',attrs=['bold']))
    print()      

    option = int(input('Choose an option\n'))
    print()

    # Option 1: Enter new ticker data to SQL
    if option == 1:
        # Get ticker name from user input
        ticker = input('Ticker name : ').upper()

        # Starting date will be day -1
        starting_date = input('Starting date is  : ')
        # Ending date will be day +0
        ending_date = input('Ending date is : ')
        
        # Get intraday data
        df_plot = get_data_from_api(ticker,ending_date,api_key,intraday=True)
        
        # Getting day-1 and day+0 daily chart price data
        df_input = get_data_from_api(ticker,ending_date,api_key,daily=True,start_date = starting_date)
        
        # Define 2 dataframes day +0 and day -1
        df_today_daily = df_input.loc[df_input['Date'].astype(str).str.contains(f'{ending_date}')]
        df_yesterday_daily = df_input.loc[df_input['Date'].astype(str).str.contains(f'{starting_date}')]

        # Data required from day -1
        Prev_Close = float(df_yesterday_daily['Close'])
        Prior_day_vol = int(df_yesterday_daily['Volume'])
        Prior_day_VWAP = float(df_yesterday_daily['Vwap'])

        # Get pandas series first element(Get weekday for day 0)
        weekday = df_today_daily['Date'].dt.day_name().item()

        # Data required from day 0
        Open_today = float(df_today_daily['Open'])
        High_today = float(df_today_daily['High'])
        Low_today = float(df_today_daily['Low'])
        Close_today = float(df_today_daily['Close'])
        volume_today = int(df_today_daily['Volume'])
        
        # Get exchange and weighted shares outstanding
        try:
            # API request for stock details data
            details_api = f'https://api.polygon.io/v3/reference/tickers/{ticker}?date={ending_date}&apiKey={api_key}'
            details_data = requests.get(details_api).json()
            results = details_data.get('results')
            weighted_shares_oustanding = results.get('weighted_shares_outstanding')

            # Get the primary exchange and replace it with a more descriptive name
            pre_exchange = results.get('primary_exchange')
            # If both data types is correct then rename Exchange name, else it might be an error
            if isinstance(results.get('weighted_shares_outstanding'),int) and isinstance(results.get('primary_exchange'),str)==True:
                if pre_exchange == 'XNYS':
                    exchange = pre_exchange.replace('XNYS','Nyse')
                elif pre_exchange == 'XNAS':
                    exchange = pre_exchange.replace('XNAS','Nasdaq')
                elif pre_exchange == 'XASE':
                    exchange = pre_exchange.replace('XASE','Amex')
            else:
                print('-----------------------------------------------\nExchange may belongs to OTC, trying next method\n-----------------------------------------------')
                try:
                    weighted_shares_oustanding = int(input('Polygon.io API can\'t find shares_outstanding, please fill out manually '))
                    exchange = results.get('market')
                except Exception as e:
                    print('error occurred at getting exchange and shares outstanding ', e)
                    pass
        except Exception as e:
            print('error occurred at getting exchange and shares outstanding ', e)
            pass
       
        # Intraday volume and high price lists
        # Get intraday volume before breakout time
        vol_b4_bo_time = []
        # Get intraday high after 5mins market open
        five_mins_high = []
        # Get high of the day
        HOD_editing = []

        # Get intraday volume before breakout time, 5 minute high and high of the day
        hod_volbo_5mins_high(df_plot)

        # filter out the B/O candle time 
        min_vol_b4_bo_time = min(vol_b4_bo_time)

        # Get the sum of the volume before breakout 5mins high
        sum_loc = df_plot.loc[(df_plot['Date'] < min_vol_b4_bo_time )]
        result_sum_loc = int(sum_loc['Volume'].sum())

        # Get the 5 minute high
        five_mins_high = float(five_mins_high[0])

        # Locate the time in format %H%M%S
        HOD_str = ''.join(HOD_editing)
        HOD_final = HOD_str[11:19]

        # Set up BS website scrap 
        try:
            url_profile = f'https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}'
            headers = {'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'}
            response = requests.get(url_profile,headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')

            # Grab sector data from yahoo
            sector = []
            for span in soup.find_all('span',{'class':'Fw(600)'}):
                sector.append(span.contents[0])
            final_sector = str(sector[1])

        except Exception as e:
            print('-----------------------------------------------\nwebscraping sector info failed, try next method\n-----------------------------------------------')
            pass

            # The path for chromedriver
            web_driver_path = r'C:\Users\admin\Python\Web Scrape Related\chromedriver.exe'
            selenium_driver = webdriver.Chrome(web_driver_path)

            # The website we need to access
            selenium_driver.get(f'https://www.nasdaq.com/market-activity/stocks/{ticker}')

            # wait for the "Accept Cookies" button to appear
            selenium_wait = WebDriverWait(selenium_driver, 10)
            accept_cookies_button = selenium_wait.until(EC.element_to_be_clickable((By.ID, "onetrust-accept-btn-handler")))

            # click on the "Accept Cookies" button
            accept_cookies_button.click()

            # Find the element using XPATH
            sector_element = selenium_driver.find_element(By.XPATH, '//td[text()="Sector"]/following-sibling::td')
            final_sector = sector_element.text
            selenium_driver.quit()    

        # Store all infos to a dictionary
        dic_1 = {
            'weekday':f'{weekday}',
            'date': f'{ending_date}',
            'ticker': f'{ticker}',
            'exchange': f'{exchange}',
            'industry': f'{final_sector}',
            'so': int(f'{weighted_shares_oustanding}'),
            'dv': int(f'{volume_today}'),
            'prior_day_vol': int(f'{Prior_day_vol}'),
            'prior_day_vwap': float(f'{Prior_day_VWAP}'),
            'b_run_up_low': float(input('B runup low is : ')),
            'run_up_high': float(input('Run up High is ')),
            'prev_close': float(f'{Prev_Close}'),
            'open': float(f'{Open_today}'),
            'low': float(f'{Low_today}'),
            'five_mins_high': float(f'{five_mins_high}'),
            'high': float(f'{High_today}'),
            'close': float(f'{Close_today}'),
            'vol_b4_bo': int(f'{result_sum_loc}'),
            'hod': f'{HOD_final}'
        }

        # Check if dictionary has data
        if len(dic_1) != 0:
            
            # Convert dictionary to dataframe
            df = pd.DataFrame.from_dict([dic_1])
            display(df)
            print(colored('-------------------------','red',attrs=['bold']))
            
            # Get input on whether to plot chart or not
            Questioning_input_plot = validated_input_plot('Do you want to plot the chart? ',['y','n '])

            while Questioning_input_plot.lower() == 'y':
                # Get time frame for chart
                Questioning_tframe = validated_input_plot('Select a time frame, for exit press q',['1min','5min','15min','30min','1h '])
                # Set time frame for 1 minute
                if Questioning_tframe.lower() == '1min':
                    df_plot_mkt_time = df_plot.set_index(pd.DatetimeIndex(df_plot['Date'].values))
                    plotly_1min_chart(df_plot_mkt_time,Questioning_tframe)
                    
                elif Questioning_tframe.lower() in ["5min", "15min", "30min", "1h"]:
                    # Set time frame for other values
                    timeframe_resample_plot(df_plot, Questioning_tframe)
                elif Questioning_tframe.lower() == 'q':
                    break

            else:
                pass

            # Ask the user if they want to update the data
            Questioning_input = str(input('Do you want to update the data? y/n '))

            if Questioning_input.lower() == 'y':
                # Get the id from the most recent entry in the "primary_sheet" table
                result = cnx.execute('''SELECT id FROM primary_sheet ORDER BY id DESC LIMIT 1''')
                primary_key = result.fetchall()
                primary_key = [i[0] for i in primary_key]
                primary_key = primary_key[0] + 1

                # Insert the id column to the first postion
                df.insert(0,column='id',value=primary_key)

                # Store data to SQL table "primary_sheet"
                df.to_sql('primary_sheet',con=cnx,index=False,if_exists='append')
                
                # Store 1min data to SQL table "one_min_data" 
                df_foreign_table = df_plot.copy()
                df_foreign_table.columns = df_foreign_table.columns.str.lower()
                df_foreign_table['ticker'], df_foreign_table['stock_one_min_id'] = f'{ticker}', primary_key
                df_foreign_table=df_foreign_table[['stock_one_min_id','ticker','date','open',
                                            'high','low','close','volume','vwap','n_of_trades']]
                df_foreign_table.to_sql('one_min_data',con=cnx,index=False,if_exists='append')
                print(colored('Data has been stored', 'red', attrs=['underline']))
                print()

            else:
                print('\n')
                print(colored('Data has not been stored', attrs=['underline']))
                
        else:
            print('no data in the dictionary')
            pass
        
    # Option 2 for data visualization 
    elif option == 2:
        ticker = input('Ticker name : ').upper()

        # Ending date will be day +0
        ending_date = input('Ending date is : ')
        # Grab the intraday +0 data
        df_plot = get_data_from_api(ticker,ending_date,api_key,intraday=True)
        
        while option_2_question != 0:
            # Visual separator
            print(colored('-------------------------','red',attrs=['bold']))

            # Input for time frame
            Questioning_tframe = validated_input_plot('Select a frame or press q to quit',['1min','5min','15min','30min','1h',' '])
            
            # Plotting based on time frame
            if Questioning_tframe.lower() == '1min':
                # Convert index to datetime for 1min chart
                df_plot_mkt_time = df_plot.set_index(pd.DatetimeIndex(df_plot['Date'].values))
                plotly_1min_chart(df_plot_mkt_time,Questioning_tframe)

            elif Questioning_tframe.lower() in ['5min','15min','30min','1h']:
                # Resample and plot for other time frames
                timeframe_resample_plot(df_plot,Questioning_tframe)
                

            else:
                break
    # Exit option        
    elif option == 3:
        print('Thanks for using Beast\'s Data Entering Algo')
        break    
