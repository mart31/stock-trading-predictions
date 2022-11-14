# -*- coding: utf-8 -*-
"""
Created on Tue Dec 14 11:13:01 2021

@author: marti
"""



import pyiqfeed as iq
from pyiqfeed import VerboseIQFeedListener
from pyiqfeed import QuoteConn
from pyiqfeed import FeedConn
from typing import Sequence

from localconfig.passwords import dtn_product_id, dtn_login, dtn_password

import time as t
import pandas as pd
import datetime
import numpy as np
import math

import compute_module as cm
import features_creator as fc
#import db_manager
import db_histo_manager as dhm
import date_manager
import test_service

import otc_object
import live_object

import live_models


def get_db_manager(db_name):
    
    if db_name=='otc_database':
        data_shape = otc_object.otc_object()
    if db_name=='live_database':
        data_shape = live_object.live_object()
        
    dbm = dhm.db_manager(db_name,data_shape)
    
    return dbm 
    

def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = test_service.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch(headless=False)
    
    

def get_tickdata(watchlist,num_days=1):
    
    '''
    This function can be called at any time. However, it will pull extra data
    that isnt needed if pull during the market hour. 
    
    ticker: Ticker symbol
    num_days: Number of calendar days. 1 means today only.
    '''
    
    max_ticks = 50000000 # which is an average of 1000 trade per min. 
    
    launch_service()
    
    hist_conn = iq.HistoryConn(name="pyiqfeed-Example-tickdata")
    hist_listener = iq.VerboseIQFeedListener("History Tick Listener")
    hist_conn.add_listener(hist_listener)
    
    # Look at conn.py for request_ticks, request_ticks_for_days and
    # request_ticks_in_period to see various ways to specify time periods
    # etc.
    
    tick_dict = {}
    with iq.ConnConnector([hist_conn]) as connector:
        temps2 = t.time()
        for ticker in watchlist:
            try:
                
                #temps = t.time()
                
                tick_data = hist_conn.request_ticks(ticker=ticker,
                                                    max_ticks=max_ticks)
                #print(tick_data)
        
                # Get the last num_days days trades between 10AM and 12AM
                # Limit to max_ticks ticks else too much will be printed on screen
                bgn_flt = datetime.time(hour=9, minute=30, second=0)
                end_flt = datetime.time(hour=16, minute=0, second=0)
                tick_dict[ticker] = hist_conn.request_ticks_for_days(ticker=ticker,
                                                             num_days=num_days,
                                                             bgn_flt=bgn_flt,
                                                             end_flt=end_flt,
                                                             max_ticks=max_ticks,
                                                             ascend=True)
                
                #temps = t.time()-temps
                #print('it tooks {} seconds to pull {}'.format(temps,ticker))
               
            except (iq.NoDataError, iq.UnauthorizedError) as err:
                #print("No data returned because {0}".format(err))
                tick_dict[ticker] = []
                
        temps2 = t.time()-temps2
        print('pulling todays tick data of {} stocks tooks {} seconds'.format(len(watchlist),temps2))
            
        return tick_dict




def compute_price(tick_data,stock_dict,ticker):
    
    #temps = t.time()
    for row in tick_data:
    
        d = row[1]
        tick_time = row[2]
        
        date_numpy = d+tick_time
        key_date = pd.to_datetime(date_numpy)
        key_date =key_date.replace(microsecond=0)
        candle_date =key_date.replace(second=0)
        
        last_price = row[3]
        volume = row[4]
       
        if candle_date in stock_dict:
            candle_open = stock_dict[candle_date][ticker]['open']
        
            if math.isnan(candle_open):
                stock_dict[candle_date][ticker]['open'] = last_price
                stock_dict[candle_date][ticker]['average'] = last_price
                stock_dict[candle_date][ticker]['high'] = last_price
                stock_dict[candle_date][ticker]['low'] = last_price
                stock_dict[candle_date][ticker]['volume'] = volume
                stock_dict[candle_date][ticker]['barCount'] = 1
                stock_dict[candle_date][ticker]['close'] = last_price
          
            else:
                high = stock_dict[candle_date][ticker]['high']
                low = stock_dict[candle_date][ticker]['low']
                barCount = stock_dict[candle_date][ticker]['barCount']
                average = stock_dict[candle_date][ticker]['average']
                
                stock_dict[candle_date][ticker]['average'] = ((average*barCount)+last_price)/(barCount+1)
            
                if last_price>high:
                    stock_dict[candle_date][ticker]['high'] = last_price
                elif last_price<low:
                    stock_dict[candle_date][ticker]['low'] = last_price
            
                stock_dict[candle_date][ticker]['close'] = last_price
                stock_dict[candle_date][ticker]['volume'] = stock_dict[candle_date][ticker]['volume'] + volume
                stock_dict[candle_date][ticker]['barCount'] = barCount + 1



def fill_missing_price(stock_dict,date_list,ticker,last_date):

    first = True
    remove_list = []
    
    for d in date_list:
        if d<last_date:
            current_price = stock_dict[d][ticker]['close']
            
            if first:
                first = False
                previous_price = current_price
        
            if math.isnan(current_price):
                if math.isnan(previous_price):
                    del stock_dict[d]
                    remove_list.append(d)
                else:
                    stock_dict[d][ticker]['close'] = previous_price
                    stock_dict[d][ticker]['open'] = previous_price
                    stock_dict[d][ticker]['high'] = previous_price
                    stock_dict[d][ticker]['low'] = previous_price
                    stock_dict[d][ticker]['average'] = previous_price
                
                
            else:
                previous_price = current_price
        else:
            break


    for r in remove_list:
        date_list.remove(r)



def fill_overnight_candle(stock_dict,date_list,ticker):

    first = True
    found_oc = False
    for d in date_list:
        
        if first:
            first=False
            previous_candle = d
            previous_close = stock_dict[d][ticker]['close']
            overnight_open = previous_close
            previous_barcount = stock_dict[d][ticker]['barCount']
            previous_volume = stock_dict[d][ticker]['volume']
            ticker = stock_dict[d][ticker]['ticker']
        else:
            
            if found_oc:
                found_oc=False
                overnight_close = stock_dict[d][ticker]['open']
                stock_dict[previous_candle][ticker]['close'] = overnight_close
                if overnight_close >overnight_open:
                    stock_dict[previous_candle][ticker]['high'] = overnight_close
                    stock_dict[previous_candle][ticker]['low'] = overnight_open
                else:
                    stock_dict[previous_candle][ticker]['high'] = overnight_open
                    stock_dict[previous_candle][ticker]['low'] = overnight_close
        
                stock_dict[previous_candle][ticker]['average'] = (overnight_close+overnight_open)/2
    
    
            if isinstance(d,str):
                temp = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
                hour = temp.hour
            else:
                hour = d.hour
            
            if hour == 8:
                found_oc = True
                overnight_open = previous_close
                stock_dict[d][ticker]['open']=overnight_open
                stock_dict[d][ticker]['barCount']=previous_barcount  
                stock_dict[d][ticker]['volume']=previous_volume
                stock_dict[d][ticker]['ticker']=ticker
                
        previous_candle = d       
        previous_close = stock_dict[d][ticker]['close']
        previous_barcount = stock_dict[d][ticker]['barCount']
        previous_volume = stock_dict[d][ticker]['volume']


def fill_price_dict(price_dict,watchlist):
    dh  = date_manager.date_handler()
    date_list = dh.create_date_list_from_dict(price_dict)
    
    tick_dict = get_tickdata(watchlist)
    today = datetime.datetime.today() #used to fill the price dict up to this moment
    today =today.replace(microsecond=0)
    last_date =today.replace(second=0)
    
    for ticker in tick_dict:
        tick_data = tick_dict[ticker]
        
        compute_price(tick_data, price_dict,ticker)
        
        fill_overnight_candle(price_dict, date_list,ticker)
        
        fill_missing_price(price_dict,date_list,ticker,last_date)
        
    return last_date



def fill_features(price_dict,watchlist,bb_dict,vwap_dict,daily_dict):
    for stock in watchlist:
        ticker = stock
        break
        
    approx=False
    approx_ratio=1.0
    
    dh  = date_manager.date_handler()
    overnight_candle = dh.get_today_overnight()
    date_list = dh.create_date_list_from_dict(price_dict)
    first=True
    
    for live_date in date_list:
        test_close = price_dict[live_date][ticker]['close']
        if math.isnan(test_close):
            break
        else:
            #print(live_date)
            if first:
                first=False
                previous_date = live_date
            else:
                cm.compute_all_ticker(price_dict,watchlist,previous_date,live_date,bb_dict,vwap_dict,approx,approx_ratio)
                cm.compute_remaining_features(price_dict,watchlist,overnight_candle,live_date)
                
                #cm.compute_all_intraday_daily(price_dict,daily_dict,watchlist,live_date)
        
                
            previous_date=live_date


def get_live_df(live_date,price_dict):
        
        df_dict = price_dict[live_date]
        
        df = pd.DataFrame.from_dict(df_dict,orient='index')
        df['date'] = live_date
        #df.reset_index(drop=True,inplace=True)
        df.set_index('date',inplace=True)
        
        return df
    
    
def refill_database(price_dict,last_date):

    #live_dbm = db_manager.db_manager(db_name='live_database')
    live_dbm = get_db_manager(db_name='live_database')
    table='FEATURES_1M'
    
    today = datetime.datetime.today()
    today_day = today.day
    
    lm = live_models.live_models() 
    
    for d in price_dict:
        d = d.to_pydatetime()
        if d.day == today_day:
            if d<last_date:
                #test_list = live_dbm.download_minute_candle(d,data_struct='list') 
                test_list = live_dbm.download_minute_candle(d,table,cond_list=[],data_struct='df')
                #print(len(test_list))
                if len(test_list)==0:
                    #print('a')
                    live_df = get_live_df(d,price_dict)
                    
                    # add the predictions
                    lm.predict_1m(live_df)
                   
                    live_dbm.save_multiple_ticker(live_df,table)
                    print('filled: {}'.format(d))
            else:
                break


def recreate_live_data(price_dict,watchlist,bb_dict,vwap_dict,daily_dict):
    temps = t.time()
    
    last_date = fill_price_dict(price_dict,watchlist)
    fill_features(price_dict, watchlist, bb_dict, vwap_dict,daily_dict)
    refill_database(price_dict,last_date)
    
    temps = t.time()-temps
    print('tooks {} seconds to recreate the current price dict'.format(temps))

















"""
dbm = db_manager.db_manager(db_name='iq_database')
ticker='AABB'
#ztest = dbm.download_ticker_using_date(ticker)
dh  = date_manager.date_handler() 
num_stocks=500
price_dict,watchlist = dbm.create_watchlist_price_dict(num_stocks)
bb_dict = dbm.get_bb_dict(watchlist)
vwap_dict = dbm.get_vwap_queue_dict(watchlist)
recreate_live_data(price_dict, watchlist, bb_dict, vwap_dict)
"""


"""
live_dbm = db_manager.db_manager(db_name='live_database')

today = datetime.datetime.today() #used to fill the price dict up to this moment
today =today.replace(microsecond=0)
last_date =today.replace(minute=10,second=0)
print(today.day)

for d in price_dict:
    d = d.to_pydatetime()
    if d<last_date:
        test_list = live_dbm.download_minute_candle(d,data_struct='list') 
        #print(len(test_list))
        if len(test_list)==0:
            #print('a')
            live_df = get_live_df(d,price_dict)
            live_dbm.save_live_data(live_df, candle_size=1)
            live_dbm.update_last_save(d)
            print('filled: {}'.format(d))
    else:
        break



test_df = get_live_df(d,price_dict)
"""



"""
live_dbm = db_manager.db_manager(db_name='live_database')
last_save = live_dbm.load_last_save()

last_date = last_save.replace(hour=14,minute=0)
live_df = live_dbm.download_minute_candle(live_date)

watchlist = live_df.index.to_list()
date_morning = dh.find_previous_morning()
date_list = dh.get_specific_date_list(date_morning)
first_date = date_list[1]

price_dict = dbm.create_specific_live_price_dict(date_morning, watchlist)
"""


    
  
#live_df = get_live_df(live_date,price_dict)




'''
At this point we need to look if the data is there and other fill the empty row. 
stop once we get to last date. Let the freedom app fill the rest. Probably
a good idea to get mini function that add save the data given the date. 
'''

























#end