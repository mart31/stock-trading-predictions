# -*- coding: utf-8 -*-
"""
Created on Tue Mar  8 11:11:47 2022

@author: marti
"""

# iqfeed.py

import argparse
import datetime
import pyiqfeed as iq
from localconfig.passwords import dtn_product_id, dtn_login, dtn_password
import pandas as pd
import math
from datetime import timedelta

import db_manager
import time as t
import date_manager
import test_service


'''
This module used https://github.com/akapur/pyiqfeed to download the historical candle data from iqfeed. 
'''


def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = test_service.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch(headless=False)

    # If you are running headless comment out the line above and uncomment
    # the line below instead. This runs IQFeed.exe using the xvfb X Framebuffer
    # server since IQFeed.exe runs under wine and always wants to create a GUI
    # window.
    # svc.launch(headless=True)



def get_bar_col():
    col_list = ['date','time','open','high','low','close','total_vol','prd_vol','num_trds']
    
    return col_list



def extract_ticker_price(ticker_array,ticker):
    ticker_dict = {}
    for row in ticker_array:
        #print(row)
        #print(type(row))
    
        d = row[0]
        tick_time = row[1]
        
        open_price = row[2]
        high = row[3]
        low = row[4]
        close = row[5]
        #total_volume = row[6]
        volume = row[7]
        
        date_numpy = d+tick_time
        key_date = pd.to_datetime(date_numpy)
        key_date =key_date.replace(microsecond=0)
        candle_date =key_date.replace(second=0)
        candle_index = candle_date - timedelta(minutes=1) # the bar candle have 1 extra minute
        
        #print(candle_index)
        #new_row = [candle_index,open_price,high,low,close,total_volume,volume]
        
        ticker_dict[candle_index]={}
        ticker_dict[candle_index]['date']=candle_index
        ticker_dict[candle_index]['open']=open_price
        ticker_dict[candle_index]['high']=high
        ticker_dict[candle_index]['low']=low
        ticker_dict[candle_index]['close']=close
        #ticker_dict[candle_index]['total_volume']=total_volume
        ticker_dict[candle_index]['volume']=volume
        ticker_dict[candle_index]['barCount']=0
        ticker_dict[candle_index]['average']=(high+low+close)/3
        ticker_dict[candle_index]['ticker']=ticker
        
    return ticker_dict



def get_multiple_histo_bar(ticker_list, bar_len: int, bar_unit: str,
                            num_bars: int):
    """Shows how to get interval bars."""
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-historical-bars")
    hist_listener = iq.VerboseIQFeedListener("History Bar Listener")
    hist_conn.add_listener(hist_listener)

    stock_dict = {}
    
    launch_service()
    with iq.ConnConnector([hist_conn]) as connector:
        # look at conn.py for request_bars, request_bars_for_days and
        # request_bars_in_period for other ways to specify time periods etc
        
        for ticker in ticker_list:
            try:
                
                
                temps = t.time()
                bars = hist_conn.request_bars(ticker=ticker,
                                              interval_len=bar_len,
                                              interval_type=bar_unit,
                                              max_bars=num_bars)
                
                stock_dict[ticker]=bars
                
                temps = t.time()-temps
                print('pulling the bars for {} tooks {} seconds'.format(ticker,temps))
                
            
             
            except (iq.NoDataError, iq.UnauthorizedError) as err:
                print("No data returned because {0}".format(err))
            
    return stock_dict



def get_multiple_ticker(ticker_list, bar_len: int, bar_unit: str,
                            date_dict):
    """Shows how to get interval bars."""
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-historical-bars")
    hist_listener = iq.VerboseIQFeedListener("History Bar Listener")
    hist_conn.add_listener(hist_listener)

    stock_dict = {}
    launch_service()
    with iq.ConnConnector([hist_conn]) as connector:
        # look at conn.py for request_bars, request_bars_for_days and
        # request_bars_in_period for other ways to specify time periods etc
        
        for ticker in ticker_list:
            try:
                
                
                temps = t.time()
                
                start_date = date_dict[ticker]['start_date']
                end_date = date_dict[ticker]['end_date']
                 
                bars = hist_conn.request_bars_in_period(ticker=ticker,
                                                        interval_len=bar_len,
                                                        interval_type=bar_unit,
                                                        bgn_prd=start_date,
                                                        end_prd=end_date)
                
            
                stock_dict[ticker]=bars
                
                temps = t.time()-temps
                print('pulling the bars for {} tooks {} seconds'.format(ticker,temps))
                
            
             
            except (iq.NoDataError, iq.UnauthorizedError) as err:
                print("No data returned because {0}".format(err))
            
    return stock_dict




def fill_missing_price(ticker_dict,date_list,ticker):
    '''
    Note this function assumes date list contains every date
    needed. 
    '''
  
    first=True
    remove_list = []
    for d in date_list:
        if first:
            if d in ticker_dict:
                first=False
                current_price = ticker_dict[d]['close']
                previous_price=current_price
            
        else:
    
            if d not in ticker_dict:
                ticker_dict[d] = {}
                ticker_dict[d]['date']=d
                ticker_dict[d]['open'] = previous_price
                ticker_dict[d]['high'] = previous_price
                ticker_dict[d]['low'] = previous_price
                ticker_dict[d]['close'] = previous_price
                ticker_dict[d]['volume']=0
                ticker_dict[d]['barCount']=0
                ticker_dict[d]['average']=previous_price
                ticker_dict[d]['ticker']=ticker
                
                
            else:
                current_price = ticker_dict[d]['close']
                previous_price = current_price


def remove_outside_market_candle(ticker_dict,date_list):

    date_dict = {}
    
    for d in date_list:
        date_dict[d]=True
        

    remove_candle = []
    for d in ticker_dict:
        if d not in date_dict:
            remove_candle.append(d)
            
    for d in remove_candle:
        del ticker_dict[d]  
        
        
def fill_overnight_candle(ticker_dict,date_list):

    first = True
    found_oc = False
    for d in date_list:
        
        if first:
            if d in ticker_dict:
                first=False
                previous_candle = d
                previous_close = ticker_dict[d]['close']
                overnight_open = previous_close
                previous_barcount = ticker_dict[d]['barCount']
                previous_volume = ticker_dict[d]['volume']
                ticker = ticker_dict[d]['ticker']
        else:
            
            if found_oc:
                found_oc=False
                overnight_close = ticker_dict[d]['open']
                ticker_dict[previous_candle]['close'] = overnight_close
                if overnight_close >overnight_open:
                    ticker_dict[previous_candle]['high'] = overnight_close
                    ticker_dict[previous_candle]['low'] = overnight_open
                else:
                    ticker_dict[previous_candle]['high'] = overnight_open
                    ticker_dict[previous_candle]['low'] = overnight_close
        
                ticker_dict[previous_candle]['average'] = (overnight_close+overnight_open)/2
    
    
            if isinstance(d,str):
                temp = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
                hour = temp.hour
            else:
                hour = d.hour
            
            if hour == 8:
                found_oc = True
                overnight_open = previous_close
                ticker_dict[d]['open']=overnight_open
                ticker_dict[d]['barCount']=previous_barcount  
                ticker_dict[d]['volume']=previous_volume
                ticker_dict[d]['ticker']=ticker
            
        if d in ticker_dict:
            previous_candle = d       
            previous_close = ticker_dict[d]['close']
            previous_barcount = ticker_dict[d]['barCount']
            previous_volume = ticker_dict[d]['volume']


def sort_ticker_dict(ticker_dict,date_list):
    '''
    To avoid problems related to the date order we create a new sorted
    dictionary. This also remove the candles outside the regular market
    hour. 
    '''
            
    new_dict = {}
    
    for d in date_list:
        if d in ticker_dict:
            new_dict[d] = ticker_dict[d]
        
    return new_dict


def create_ticker_price_dict(ticker_array,date_list,ticker):
    '''
    Note, the date list ensure the data is processed in the right order. 
    '''
    
    
    ticker_dict = extract_ticker_price(ticker_array,ticker)
    fill_missing_price(ticker_dict, date_list,ticker)
    #remove_outside_market_candle(ticker_dict, date_list) # included in sort ticker dict
    fill_overnight_candle(ticker_dict, date_list)
    ticker_dict = sort_ticker_dict(ticker_dict,date_list)
    
    return ticker_dict


def download_price_data(ticker_list,date_dict):
    '''
    At most 500 ticker can be provided at once. The date dict contains
    the start and end time of each ticker. It also contain the update 
    date list. The datelist is required to ensure only the required
    data is returned.
    '''
    
    bar_len = 60
    bar_unit = 's'
    price_data = {}
    
    stock_dict = get_multiple_ticker(ticker_list,bar_len,bar_unit,date_dict)
    
    temps = t.time()
    
    print('')
    print('Computing the price data')
    print('')
    for ticker in stock_dict:
        ticker_array = stock_dict[ticker]
        date_list = date_dict[ticker]['date_list']
        ticker_dict = create_ticker_price_dict(ticker_array,date_list,ticker)
        
        price_data[ticker]=ticker_dict
        
    temps = t.time()-temps
    print('Creating the price data for {} stocks tooks {} seconds'.format(len(ticker_list),temps))
        
    return price_data



# end


