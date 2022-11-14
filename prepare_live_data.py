# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 13:27:18 2022

@author: marti
"""

'''
These function have not been tested or modified

'''

import time as t
import queue
import numpy as np
import pandas as pd

import db_histo_manager as dhm
import date_manager


import otc_object
import live_object  
import american_object


def print_unique_fifth_letter(watchlist):

     fifth_list = []    
     for ticker in watchlist:
         if len(ticker)>4:
            fifth = ticker[4]
            fifth_list.append(fifth)
            
     unique_letter = set(fifth_list)
     print('The unique fifth letter are:')
     print(unique_letter) 
     
     

def get_db_manager(db_name):
    
    if db_name=='otc_database':
        data_shape = otc_object.otc_object() 
    elif db_name=='live_database':
        data_shape = live_object.live_object
    elif db_name=='american_database':
        data_shape = american_object.american_object()
        
    dbm = dhm.db_manager(db_name,data_shape)
    
    return dbm    


def create_watchlist(db_name='',num_stocks=490,data_struct='dict'):
    '''
    Create a watchlist of the live ticker being watched. Note,
    the data structure is a dictionary as we sometime need to 
    quickly know if a ticker is in the watchlist.
    '''
    
    if db_name =='':
        db_name= get_active_database()
        
    
    """
    if db_name=='otc_database':
        sort_by='average_cash'
    elif db_name=='american_database':
        sort_by='candle_range'
    """
    
    sort_by='average_cash'
    
    dh  = date_manager.date_handler()      
    last_date = dh.get_last_market_date().to_pydatetime()
   
    dbm = get_db_manager(db_name)
   
    table='FEATURES_390M'
    cond_list=[]
    df390 = dbm.download_specific_day(last_date,table,cond_list,data_struct='df')
   
    col = ['ticker','average_cash','candle_range']
    df390 = df390[col]
    df390.sort_values(sort_by,ascending=False,inplace=True)
   
    df390 = df390[:num_stocks]
   
    if data_struct=='dict':
        watchlist = {}
        
        for index,row in df390.iterrows():
            ticker = row['ticker']
            avg_cash = row['average_cash']
            candle_range = row['candle_range']
            
            watchlist[ticker]={}
            watchlist[ticker]['average_cash']=avg_cash
            watchlist[ticker]['candle_range']=candle_range
            watchlist[ticker]['active'] = 0
            
    elif data_struct=='list':
        watchlist = []
        
        for index,row in df390.iterrows():
            ticker = row['ticker']
            watchlist.append(ticker)
        
    print_unique_fifth_letter(watchlist)
        
    return watchlist


def get_watchlist_df(db_name=''):
    
    if db_name =='':
        db_name= get_active_database()
    
    watchlist = create_watchlist(db_name,data_struct='dict')
    watchlist_df = pd.DataFrame.from_dict(watchlist,orient='index')
    watchlist_df.index.name='ticker'
    
    watchlist_df['db_name']=db_name
    
    return watchlist_df


def init_overnight_candle(overnight_candle,last_update_candle,price_dict):
        
        price_dict[overnight_candle]['open'] = price_dict[last_update_candle]['close']
        price_dict[overnight_candle]['barCount'] = price_dict[last_update_candle]['barCount']
        price_dict[overnight_candle]['volume'] = price_dict[last_update_candle]['volume']
        price_dict[overnight_candle]['overnight_candle']=int(1)


def create_last_row(last_update_candle,last_data_row,col_list):
    last_row = {}
    last_row['date']=last_update_candle
    
    for col in col_list:
        if col !='date':
            last_row[col]=float('Nan')
    
    for col in col_list:
        if col in last_data_row:
            last_row[col]=last_data_row[col]
            
    
    return last_row



def create_ticker_price_dict(ticker,db_name):
    
    dbm = get_db_manager(db_name)
    dh  = date_manager.date_handler() 
    
    live_col = live_object.live_object()
    col_list = live_col.features
    
    date_list = dh.live_price_date_list(candle_size=1,data_struct='data')
    
    price_dict = {}
    
    last_update_candle = dh.get_last_market_candle()
    #last_row = dbm.download_specific_candle(ticker, candle_size, last_update_candle)
    last_data_row = dbm.download_specific_candle(ticker,last_update_candle,table='FEATURES_1M',data_struct='dict')
    
    last_row = create_last_row(last_update_candle,last_data_row,col_list)
    
    if len(last_row)>0:
        price_dict[last_update_candle] = last_row
        
        
        for index in date_list:
            d = index.to_pydatetime()
            
            if d != last_update_candle:
                price_dict[d]={}
                for col in col_list:
                    price_dict[d][col] = float('NaN')
                
                price_dict[d]['barCount'] = 0
                price_dict[d]['volume'] = 0
                price_dict[d]['ticker']=ticker
                #price_dict[d]['oc_gains']=1.0
                
                
        overnight_candle = dh.get_today_overnight()
        init_overnight_candle(overnight_candle,last_update_candle,price_dict)
    
       
        return price_dict
    
    else:
        print('{} was not updated yestedays'.format(ticker))
    
        return {} 
    
    
def create_price_dict(watchlist,db_name):
    dh  = date_manager.date_handler()  
    date_list = dh.live_price_date_list(candle_size=1,data_struct='data')   
     
    all_ticker = {}
    temps = t.time()
    for ticker in watchlist:  
        
        ticker_price_dict = create_ticker_price_dict(ticker,db_name) 
        
        all_ticker[ticker] = ticker_price_dict  
        
    price_dict={}
    
    for d in date_list:
        price_dict[d]={} 
        
        for ticker in watchlist:
            price_dict[d][ticker]={}
            
    
    #temps = t.time()
    for ticker in all_ticker:
        for d in all_ticker[ticker]:
            for col in all_ticker[ticker][d]:
                price_dict[d][ticker][col] = all_ticker[ticker][d][col]
        
    
        
    temps = t.time()-temps
        
    print('creating the live price dict tooks {} seconds'.format(temps))   
    
    return price_dict 



def get_bb_dict(watchlist,db_name):
        dh  = date_manager.date_handler()      
        date_morning = dh.get_last_market_date()
        
        dbm = get_db_manager(db_name)
        
        temps = t.time()
        bb_dict = {}    
        for ticker in watchlist:
            bb_dict[ticker] = get_last_bb_queue(date_morning,ticker,dbm)
            
            
        temps = t.time()-temps
        print('creating the bb dict tooks {} seconds'.format(temps))
        
        return bb_dict    

    
 
def get_last_bb_queue(date_morning,ticker,dbm):
    '''
    This function create a bb queue using the last update from the stock table.
    '''
    table='FEATURES_1M'
    
    q = queue.Queue(maxsize=20)  
     
    date_list = [date_morning]
    
    df = dbm.download_ticker(ticker,table,date_list,cond_list=[],data_struct='df')
    
    df = df[-20:]
    for index,row in df.iterrows():
        if q.full()==False:
            q.put(row['close'])
        else:
            q.get()
            q.put(row['close'])
            
    return q 



def get_vwap_queue_dict(watchlist,db_name):
        
    dh  = date_manager.date_handler()      
    date_morning = dh.get_last_market_date()
    
    dbm = get_db_manager(db_name)
    
    temps = t.time()
    vwap_dict = {}    
    for ticker in watchlist:
        price_q,volume_q = get_last_vwap_queue(date_morning,ticker,dbm)
        #vwap_dict[ticker] = self.get_last_vwap_queue(ticker, candle_size=1)
        vwap_dict[ticker]={}
        vwap_dict[ticker]['price_q']=price_q
        vwap_dict[ticker]['volume_q']=volume_q
        
    temps = t.time()-temps
    print('creating the vwap queue tooks {} seconds'.format(temps))
    
    return vwap_dict


def get_last_vwap_queue(date_morning,ticker,dbm):
    '''
    This function create a vwap queue using the last update from the stock table.
    '''
    
    price_q = queue.Queue(maxsize=20)
    volume_q = queue.Queue(maxsize=20)
     
    date_list = [date_morning]
    table='FEATURES_1M'
    df = dbm.download_ticker(ticker,table,date_list,cond_list=[],data_struct='df')   
    
    df = df[-20:]
    for index,row in df.iterrows():
        if price_q.full()==False:
            
            price_q.put(row['average'])
            volume_q.put(row['volume'])
        else:
            price_q.get()
            volume_q.get()
            
            price_q.put(row['average'])
            volume_q.put(row['volume'])
            
    return price_q,volume_q


def get_daily_data(ticker,date_list,date_morning,dbm):

    table='FEATURES_390M'
    data_row = dbm.download_ticker(ticker,table,[date_morning],cond_list=[],data_struct='dict')

    last_row={}
    for d in data_row:
        last_row = data_row[d]
    
    
    bb_queue = dbm.get_daily_filled_queue(ticker,date_list,col='close')
    price_q = dbm.get_daily_filled_queue(ticker,date_list,col='average')
    volume_q = dbm.get_daily_filled_queue(ticker,date_list,col='volume')
    
    daily_dict = {}
    daily_dict['last_row']=last_row
    daily_dict['bb_queue']=bb_queue
    daily_dict['price_q']=price_q
    daily_dict['volume_q']=volume_q
    
    
    live_col = live_object.live_object()
    col_list = live_col.features_390m
    
    live_row = {}
    for col in col_list:
        live_row[col]=0
        
    daily_dict['live_row'] = live_row
    
    return daily_dict


def prepare_live_daily_data(watchlist,db_name):
    
    temps = t.time()
    
    dbm = get_db_manager(db_name)
    
    dh  = date_manager.date_handler()  
    calendar = dh.get_market_calendar()
    calendar = calendar[-20:]
    date_list = calendar.index.to_list()   
    date_morning = dh.get_last_market_date()
    
    daily_dict={}
    for ticker in watchlist:
        #print(ticker)
        daily_dict[ticker]=get_daily_data(ticker,date_list,date_morning,dbm)
        
    temps = t.time()-temps
    print('preparing the live daily data tooks {} seconds'.format(temps))
        
    return daily_dict



def create_specific_live_price_dict(date_morning,watchlist,db_name):
  
    temps = t.time()
    
    dh  = date_manager.date_handler()  
    date_list = dh.get_specific_date_list(date_morning)
    
    dbm = get_db_manager(db_name)
    table='FEATURES_1M'
    
    ticker_list = []
    for ticker in watchlist:
        ticker_list.append(ticker)
    
    #import live_object
    live_col = live_object.live_object()
    col_list = live_col.features
    
    col_list.remove('pred_high20')
    col_list.remove('pred_low20')
    col_list.remove('pred_hl_ratio20')
    col_list.remove('pred_ng20')
    col_list.remove('hl_ratio')
    
    price_dict = {}
    for candle_date in date_list:
        #ticker_dict = dbm.download_candle_ticker_list(ticker_list,table,candle_date,cond_list=[],data_struct='dict')
        
        ticker_dict = dbm.download_minute_candle(candle_date,table='FEATURES_1M',cond_list=[],data_struct='dict')
        #ticker_dict = dbm.download_inner_join_candle(candle_date,data_struct='dict')
   
        price_dict[candle_date] = {}
        
        for ticker in ticker_dict:
            if ticker in watchlist:
                #print(ticker)
                price_dict[candle_date][ticker]={}
                for col in col_list:
                    #if col !='date':
                    price_dict[candle_date][ticker][col]=ticker_dict[ticker][col]
                    
                price_dict[candle_date][ticker]['pred_high20']=np.nan
                price_dict[candle_date][ticker]['pred_low20']=np.nan
                price_dict[candle_date][ticker]['pred_hl_ratio20']=np.nan
                price_dict[candle_date][ticker]['pred_ng20']=np.nan
                price_dict[candle_date][ticker]['hl_ratio']=np.nan
     
    temps = t.time()-temps
    print('creating the live price dict tooks {} seconds'.format(temps))
    return price_dict


def get_active_database():
    db_name='american_database'
    return db_name




























    
    
    
    
# end