# -*- coding: utf-8 -*-
"""
Created on Sun Mar 13 12:26:21 2022

@author: marti
"""

import db_histo_manager as dhm
import otc_object
import live_object
import american_object

import live_models

import histo_bar_feed 
import market_summary_feed as msf
import date_manager
import features_creator as fc
import compute_module as cm
import news_feed 
import run_test as rt

import queue
import pandas as pd
import time as t
import numpy as np
import math
import datetime





def restart_daily_database():

    otc_dbm = get_db_manager('otc_database')
    stock_info = otc_dbm.download_stock_list()
    all_ticker = stock_info.index.to_list()
    splited_ticker = split_ticker_list(all_ticker)
    
    db_name = 'otc_database'
    
    count = 0
    for ticker_list in splited_ticker:
        temps = t.time()
        
        init_daily_ticker_list(ticker_list,db_name)
        
        count = count + len(ticker_list)
        temps = t.time()-temps
        print('')
        print('{}/{} daily stock initialized last round tooks {} seconds'.format(count,len(all_ticker),temps))
        print('')
        
        t.sleep(2)




def create_stock_info_from_old_db():
    import db_manager    
    dbm = db_manager.db_manager(db_name='iq_database')
    stock_list = dbm.download_stock_list(table='STOCK')
    
    date = create_summary_date()
    df_eds,df_fds = msf.request_otc_summary(date)    
      
    
    all_stock = {}
    
    for index,row in df_eds.iterrows():
        ticker = row['Symbol']
        all_stock[ticker]=True
     
        
    
    stock_dict = {}
    for ticker,row in stock_list.iterrows():
        if ticker in all_stock:
            stock_dict[ticker]={}
            stock_dict[ticker]['name']=''
            stock_dict[ticker]['exchange']=0
            stock_dict[ticker]['sec_type']=1
            stock_dict[ticker]['common_share']=0
            stock_dict[ticker]['institutionalPercent']=0.0
            stock_dict[ticker]['shortInterest']=0.0
        
    
    #def create_stock_info(df_eds,df_fds,stock_dict):
    
    for index,row in df_eds.iterrows():
        ticker = row['Symbol']
        
        if ticker in stock_dict:
            exchange = row['Exchange']
            stock_dict[ticker]['exchange']=exchange
                
            
    for index,row in df_fds.iterrows():
        ticker = row['Symbol']
        
        if ticker in stock_dict:
            name = row['Description']
            stock_dict[ticker]['name']=name
            
            ins_percent = row['InstitutionalPercent']
            if ins_percent!='':
                stock_dict[ticker]['institutionalPercent']=float(ins_percent)
                
            common_share = row['CommonSharesOutstanding']
            if common_share !='':
                stock_dict[ticker]['common_share']= int(common_share)
                
            shortInterest = row['ShortInterest']
            if shortInterest !='':
                stock_dict[ticker]['shortInterest']=float(shortInterest)
                
    
     
    stock_df = pd.DataFrame.from_dict(stock_dict,orient='index')     
    stock_df.index.name='ticker'
    
    return stock_df


def init_features_data(ticker_dict,date_list):

    bb_queue = queue.Queue(maxsize=20)   
    price_q = queue.Queue(maxsize=20)     
    volume_q = queue.Queue(maxsize=20)  
    
    
    first=True
    
    for d in date_list:
        live_row = ticker_dict[d]
        
        if first:
            first=False
            last_row = live_row
            fc.init_first_row(live_row)
        else:
            cm.compute_test_row(last_row,live_row,d,bb_queue,price_q,volume_q)
        
        last_row = live_row
            
    fc.dict_green_line(ticker_dict,init=True)
    fc.dict_add_next_gain_20(ticker_dict)
    
    fc.nan_init(ticker_dict,i = 390,ng=20)
    
    fc.init_highest_ng(ticker_dict,ng=20)
    fc.init_lowest_ng(ticker_dict,ng=20)
    fc.init_hl_ratio(ticker_dict,ng=20)
    
    




def get_init_date_dict(ticker_list,db_name):
  
    dh  = date_manager.date_handler()
            
    date_list = dh.get_intraday_date_list(keep_2021=True,db_name=db_name)
    
    
    start_date = date_list[0]
    end_date = date_list[-1]
    end_date = end_date.replace(hour=16,minute=1)
    
    date_dict = {}
    
    for ticker in ticker_list:
        date_dict[ticker]={}
        date_dict[ticker]['start_date']=start_date
        date_dict[ticker]['end_date']=end_date
        date_dict[ticker]['date_list']=date_list
        
    return date_dict



def get_db_manager(db_name):
    
    if db_name=='otc_database':
        data_shape = otc_object.otc_object()
    if db_name=='live_database':
        data_shape = live_object.live_object()
    if db_name=='american_database':
        data_shape = american_object.american_object()
        
    dbm = dhm.db_manager(db_name,data_shape)
    
    return dbm



def find_last_update(ticker,table,db_name):
 
    dbm = get_db_manager(db_name)       
    dh  = date_manager.date_handler()    
    calendar = dh.get_market_calendar(keep_2021=True,db_name=db_name)
    calendar.sort_index(ascending=False,inplace=True)
      
    found = False
    
    for index,row in calendar.iterrows():
        #print(index)
        if found == False:
            #daily_date = index.to_pydatetime()
            date_list = [index]
            #date_df = self.download_ticker(ticker,date=daily_date,candle_size=candle_size)
            date_df = dbm.download_ticker(ticker, table, date_list)
            if len(date_df)>0:
                found = True
                #last_update = daily_date
                last_update=index
                last_candle = date_df.index[-1].to_pydatetime()
                #print('the last activity date for {} was on {}'.format(ticker,last_update))
                
        else:
            break   
        
    if found:   
        return last_update,last_candle
    else:
        last_update = calendar.index[-1]
        last_candle = last_update
        return last_update,last_candle
    
    
def find_last_update_morning(db_name):
 
    table='FEATURES_390M'
    dbm = get_db_manager(db_name)       
    dh  = date_manager.date_handler()    
    calendar = dh.get_market_calendar(keep_2021=True,db_name=db_name)
    calendar.sort_index(ascending=False,inplace=True)
      
    found = False
    
    cond_list=[]
    for index,row in calendar.iterrows():
        
        if found == False:
            
            date_morning = index.to_pydatetime()
            date_df = dbm.download_specific_day(date_morning,table,cond_list,data_struct='df')
            # = dbm.download_ticker(ticker, table, date_list)
            
            if len(date_df)>0:
                
                found = True
                #last_update = daily_date
                last_update=index
                #last_candle = date_df.index[-1].to_pydatetime()
                #print('the last activity date for {} was on {}'.format(ticker,last_update))
                
        else:
            break
       
        
    if found:
        
        return last_update
    else:
        last_update = calendar.index[-1]
       
        return last_update


def find_update_dates(ticker,table,db_name):
    '''
    Note last update will have us download the last updatted day. This is on
    purpose to avoid download error while updating the database. 
    '''
    
    last_update,last_candle = find_last_update(ticker,table,db_name)
    start_date = last_update
    #start_date = last_candle.replace(hour=16,minute=1)
    
    dh  = date_manager.date_handler() 
    last_date = dh.get_last_market_date()
    end_date = last_date.replace(hour=16,minute=1)
    
    #print('last_update: {} ticker: {}'.format(last_update,ticker))
    
    return start_date,end_date


def get_update_date_dict(ticker_list,table,db_name,date_list=[]):
    temps = t.time()
    
    dh  = date_manager.date_handler()
    
    if len(date_list)==0:
        date_list = dh.get_intraday_date_list(keep_2021=True,db_name=db_name)
    
    date_dict = {}
    
    for ticker in ticker_list:
        
        #temps2 = t.time()
        start_date,end_date= find_update_dates(ticker,table,db_name)
        sliced_list = dh.slice_date_list(date_list, start_date, end_date)
        
        date_dict[ticker]={}
        date_dict[ticker]['start_date']=start_date
        date_dict[ticker]['end_date']=end_date
        date_dict[ticker]['date_list']=sliced_list
        
        #temps2 = t.time()-temps2
        #print('creating the date dict of {} tooks {} seconds'.format(ticker,temps2))
    
    temps = t.time()-temps
    print('creating the date dict tooks {} seconds'.format(temps))
    
    return date_dict


def update_ticker_list(ticker_list,table,db_name,date_list=[]):

    date_dict = get_update_date_dict(ticker_list,table,db_name,date_list)
    price_data = histo_bar_feed.download_price_data(ticker_list,date_dict)
    
    dh  = date_manager.date_handler()
    dbm = get_db_manager(db_name)
    
    anomaly_list = []
    
    for ticker in ticker_list:
        #temps = t.time()
        
        if ticker in price_data:
            ticker_dict = price_data[ticker]
            date_list = dh.create_date_list_from_dict(ticker_dict)
            
            #if db_name =='otc_database':
            init_features_data(ticker_dict,date_list)
                    
            df = pd.DataFrame.from_dict(ticker_dict,orient='index')
            
            if len(df)>0:
                #dbm.add_stock(df_row)
                df.index.name='date'
                df.drop(['date'],axis=1,inplace=True)
                
                dbm.save_ticker_data(df,table,ticker,check_duplicate=True)
            
            #temps = t.time()-temps
            #print('computing the features for {} tooks {}'.format(ticker,temps))
        else:
            print('No data was available for {}'.format(ticker))
            anomaly_list.append(ticker)
            
    return anomaly_list


def create_summary_date():
    dh  = date_manager.date_handler()
    last_market_date = dh.get_last_market_date()
    
    year = str(last_market_date.year)
    month = last_market_date.month
    day = last_market_date.day
    
    if month<10:
        month = '0'+str(month)
    else:
        month = str(month)
        
    if day<10:
        day = '0'+str(day)
    else:
        day = str(day)
        
    date = year+month+day
    return date


def prepare_stock_info_dict(ticker_list):
    stock_dict = {}
    for ticker in ticker_list:
        stock_dict[ticker]={}
        stock_dict[ticker]['name']=''
        stock_dict[ticker]['exchange']=0
        stock_dict[ticker]['sec_type']=1
        stock_dict[ticker]['common_share']=0
        stock_dict[ticker]['institutionalPercent']=0.0
        stock_dict[ticker]['shortInterest']=0.0
        
    return stock_dict



def init_multiple_stock(stock_list,db_name):

    '''
    stock_list contain the stock info of the new stocks. 
    ''' 
    
    ticker_list = stock_list.index.to_list()   
    
    date_dict = get_init_date_dict(ticker_list,db_name=db_name)
    price_data = histo_bar_feed.download_price_data(ticker_list,date_dict)
    
    
    dh  = date_manager.date_handler()
    dbm = get_db_manager(db_name)
    table='FEATURES_1M'
    
    for ticker,row in stock_list.iterrows():
        temps = t.time()
        
        if ticker in price_data:
            ticker_dict = price_data[ticker]
            date_list = dh.create_date_list_from_dict(ticker_dict)
            
            #if db_name =='otc_database':
            init_features_data(ticker_dict,date_list)
                    
            df = pd.DataFrame.from_dict(ticker_dict,orient='index')
        
            if len(df)>0:
                
                df.index.name='date'
                df.drop(['date'],axis=1,inplace=True)
                
                dbm.add_stock_info(row, ticker)
                dbm.save_ticker_data(df,table,ticker,check_duplicate=False)
                
                #create the 390m data after we confirm 1m data is available
                init_daily_data(ticker,db_name,price_data)
            
                temps = t.time()-temps
                print('computing the features for {} tooks {}'.format(ticker,temps))
            else:
                print('There was no data for {}'.format(ticker))
        else:
            print('There was no data for {}'.format(ticker))


def split_ticker_list(all_ticker,n_split=50):
    stock_split = np.array_split(all_ticker,n_split)
    
    ticker_split = []
    
    for t_list in stock_split:
        ticker_list = []
        for ticker in t_list:
            ticker_list.append(str(ticker))
            
        ticker_split.append(ticker_list)
    
    return ticker_split


def update_1m_database(db_name):
    table='FEATURES_1M'
    dbm = get_db_manager(db_name)    
    stock_info = dbm.download_stock_list()
    all_ticker = stock_info.index.to_list()
    
    ticker_split = split_ticker_list(all_ticker)
    dh  = date_manager.date_handler()
    date_list = dh.get_intraday_date_list(keep_2021=True,db_name=db_name) # providing the date list speed up the update process
     
    all_anomaly =[]
    count = 0
    total_temps = t.time()
    for ticker_list in ticker_split:
        temps = t.time()
        
        anomaly_list = update_ticker_list(ticker_list,table,db_name,date_list)
        all_anomaly.append(anomaly_list)
        
        count = count + len(ticker_list)
        temps = t.time()-temps
        print('')
        print('updated {}/{} stocks'.format(count,len(all_ticker)))
        print('updating {} stocks tooks {} seconds'.format(len(ticker_list),temps))
        print('')
        
    
    total_temps = t.time()-total_temps
    print('updating {} stocks tooks {} seconds'.format(len(all_ticker),total_temps))
    
    return all_anomaly



def candle_from_dict(ticker_dict,intraday):
    first_index = str(intraday[0])
    last_index = str(intraday[-1])
      
    volume = 0
    barcount = 0
    average = 0.0
    high = 0.0
    low = 9999999
    
    ticker = ticker_dict[first_index]['ticker']  
    op = ticker_dict[first_index]['open']
    close = ticker_dict[last_index]['close']
    
    for d in intraday:
        vol = ticker_dict[d]['volume']
        avg = ticker_dict[d]['average']
        bar = ticker_dict[d]['barCount']
        h =  ticker_dict[d]['high']
        l =  ticker_dict[d]['low']
        
        if h > high:
            high = h
        if l < low:
            low = l
            
        volume = volume + vol
        barcount = barcount + bar
        average = average+ (vol*avg)
        
    if volume ==0:
        average = op
    else:
        average = average/volume
      
    candle = {}
    
    candle['open']=op
    candle['high']=high
    candle['low']=low
    candle['close']=close
    candle['volume']=volume
    candle['barCount']=barcount
    candle['average']=average
    candle['ticker']=ticker
    
    
    return candle


def create_390m_price_data(ticker_dict,intraday_list):

    price_dict = {}
    for intraday in intraday_list:
        candle = candle_from_dict(ticker_dict,intraday)
        date_candle = intraday[0]  
        price_dict[date_candle]=candle
        
    return price_dict


def create_from(data,create='df',data_struct='dict'):

    if create=='df':
        if data_struct=='dict':
            df = pd.DataFrame.from_dict(data,orient='index')
            return df
        
        
def init_390m_features(ticker_dict,candle_size=390):

    #temps = t.time()
    dh  = date_manager.date_handler()
    date_list = dh.create_date_list_from_dict(ticker_dict,remove_oc=True)
    
    intraday_list = dh.split_intraday_data(date_list)
    
    candle_dict = create_390m_price_data(ticker_dict,intraday_list)
    
    date_list_390m = dh.create_date_list_from_dict(candle_dict)
    
    cm.init_features(candle_dict, date_list_390m,candle_size)
    
    fc.dict_add_390m_oc_gains(candle_dict)
    
    fc.dict_add_ema13_oc_gains(candle_dict) 
    fc.dict_add_d1_ema13_oc_gains(candle_dict)
    
    fc.dict_add_390m_next(candle_dict) 
    
    
    fc.nan_init_390m(candle_dict,date_list_390m)
    
    
    df = create_from(candle_dict)
    df.index.name='date'
    df.index = pd.to_datetime(df.index)
    
    #temps = t.time()-temps
    #print('initializing the daily candle of {} tooks {} seconds'.format(ticker,temps))
    
    return df 


     
def init_daily_data(ticker,db_name,price_data):

    temps = t.time()
  
    dbm = get_db_manager(db_name)
    
    if ticker in price_data:
        price_dict = price_data[ticker]
        
        ticker_dict = {}
        for key in price_dict:
            str_key = str(key)
            ticker_dict[str_key]=price_dict[key]
        
        df = init_390m_features(ticker_dict)
        
        table_390m = 'FEATURES_390M'
        dbm.save_ticker_data(df, table_390m, ticker,check_duplicate=True)
        
        temps = t.time()-temps
        print('initializing {} tooks {} seconds'.format(ticker,temps)) 
    else:
        print('NO DATA FOR {}'.format(ticker))
    
    
def init_daily_ticker_list(ticker_list,db_name):
    
    date_dict = get_init_date_dict(ticker_list,db_name=db_name)
    
    price_data = histo_bar_feed.download_price_data(ticker_list,date_dict)
    
    for ticker in ticker_list:
        init_daily_data(ticker,db_name,price_data)
        
        
def compute_average_cash(volume,close):
    vol = float(volume)
    
    if vol==0:
        return 0
    else:
        if close=='':
            return 0
        else:
            price = float(close)
            avg_cash = price*vol
            return avg_cash
        
        
def get_stock_dict(db_name):
    dbm = get_db_manager(db_name)
    stock_info = dbm.download_stock_list()
    
    stock_dict = {}
    for ticker,row in stock_info.iterrows():
        stock_dict[ticker]=True

    return stock_dict


def find_new_otc_stocks(min_cash=1000000):
    
    stock_dict = get_stock_dict('otc_database')
    
    date = create_summary_date()
    df_eds,df_fds = msf.request_otc_summary(date)            
    df_eds['cash'] = df_eds.apply(lambda x: compute_average_cash(x['Volume'],x['Close']),axis=1)
    potential_stock = df_eds.loc[df_eds['cash']>=min_cash]
    
    
    new_stock = {}
    for index,row in potential_stock.iterrows():
        ticker=row['Symbol']
        
        if ticker not in stock_dict:
            new_stock[ticker]=True
            
    return new_stock


def create_stock_info(df_eds,df_fds,ticker_list):
     
    stock_dict = {}
    for ticker in ticker_list:
        stock_dict[ticker]={}
        stock_dict[ticker]['name']=''
        stock_dict[ticker]['exchange']=0
        stock_dict[ticker]['sec_type']=1
        stock_dict[ticker]['common_share']=0
        stock_dict[ticker]['institutionalPercent']=0.0
        stock_dict[ticker]['shortInterest']=0.0    
    
    
    for index,row in df_eds.iterrows():
        ticker = row['Symbol']
        
        if ticker in stock_dict:
            exchange = row['Exchange']
            sec_type = row['Type']
            stock_dict[ticker]['exchange']=exchange
            stock_dict[ticker]['sec_type']=sec_type
                
            
    for index,row in df_fds.iterrows():
        ticker = row['Symbol']
        
        if ticker in stock_dict:
            name = row['Description']
            stock_dict[ticker]['name']=name
            
            ins_percent = row['InstitutionalPercent']
            if ins_percent!='':
                stock_dict[ticker]['institutionalPercent']=float(ins_percent)
                
            common_share = row['CommonSharesOutstanding']
            if common_share !='':
                stock_dict[ticker]['common_share']= int(common_share)
                
            shortInterest = row['ShortInterest']
            if shortInterest !='':
                stock_dict[ticker]['shortInterest']=float(shortInterest)
                
    
    stock_df = pd.DataFrame.from_dict(stock_dict,orient='index')     
    stock_df.index.name='ticker'
    
    return stock_df



def add_new_otc_stocks():
    #ticker_list = find_new_otc_stocks()   
        
    db_name='otc_database'
    
    ticker_list = find_new_otc_stocks() 
    date = create_summary_date()
    df_eds,df_fds = msf.request_otc_summary(date) 
    
    stock_info = create_stock_info(df_eds, df_fds, ticker_list)
    
    init_multiple_stock(stock_info,db_name)
    
    return stock_info



def find_new_stocks(db_name,min_cash=1000000,mc_limit=700000000):
    '''
    Look for new stock with a market cap below 700 million and trading at
    least 1 million a day. 
    '''
    date = create_summary_date()
    stock_list = msf.request_summary(date,mc_limit=mc_limit,min_cash = min_cash)
    stock_dict = get_stock_dict(db_name)
    
    new_stock = []
    
    for ticker,row in stock_list.iterrows():
        if ticker not in stock_dict:
            new_stock.append(ticker)
    
    new_stock_info = stock_list.loc[new_stock]
            
    return new_stock_info


def add_new_stocks(db_name):
    new_stock_info = find_new_stocks(db_name)
    init_multiple_stock(new_stock_info,db_name)
    return new_stock_info

    
    
def get_daily_date_list(keep_2021=True,db_name='otc_database'):
    dh = date_manager.date_handler()
    calendar = dh.get_market_calendar(keep_2021=keep_2021,db_name=db_name)
    d_list = calendar.index.to_list()
    
    date_list = []
    
    for d in d_list:
        daily_date = d.replace(hour=9,minute=30)
        date_list.append(daily_date)
        
    return date_list


def find_missing_daily_candle(daily_date_list,ticker_dict390):

    missing = []
    for d in daily_date_list:
        test_date = str(d)
        if test_date not in ticker_dict390:
            missing.append(d)
            
    return missing


def create_stocks_group(stock_list,length=100):

    groups = math.ceil(len(stock_list)/length)
    groups_list = []
    for i in range(groups):
        x = i*length  
        grp = stock_list[x:x+length]
        groups_list.append(grp)
        
    return groups_list



def update_headlines(db_name,length=300,max_stock=10):
    '''
    According to the pyiq documentation, we can only batch download news headlines for a given
    stock. Trying to download a specific date wont return anything. 
    '''
    
    dbm = get_db_manager(db_name)
    stock_info = dbm.download_stock_list()
    stock_list = stock_info.index.to_list()
    
    stocks_groups = create_stocks_group(stock_list,length)
    
    for ticker_list in stocks_groups:
     
        temps = t.time()
      
        headlines = news_feed.get_news_headlines(ticker_list)
        
        df = news_feed.prepare_headline_df(headlines)
        
        dh  = date_manager.date_handler()
        calendar = dh.get_market_calendar(keep_2021=True,db_name=db_name)
        
        test_date = calendar.index[-3] # we are only interested in the last 3 days

        if len(df)>0:
            df = df.loc[df['date']>=test_date]
        
        if len(df)>0:
            df = df.loc[df['stock_number']<=max_stock]
        
        
        if len(df)>0:
            print('{} news were pulled'.format(len(df)))
            dbm.save_headlines(df)
        else:
            print('There was no news!!!!!!!!!!!!!!!!!!!!!!!!! wierd.........')
        
        temps = t.time()-temps
        print('Updating the news headlines for {} stocks tooks {} seconds'.format(len(ticker_list),temps))
        
        print('waiting 10 seconds')
        t.sleep(10) 
    
        
def get_latest_headlines(db_name,num_days=10):

    dh  = date_manager.date_handler()
    calendar = dh.get_market_calendar(num_days=num_days,db_name=db_name)
    
    date_dict={}
    date_dict['begin_date'] = calendar.index[0].to_pydatetime()
    date_dict['end_date'] = datetime.datetime.today()
    
    dbm = get_db_manager(db_name)
    headlines_df = dbm.download_all_headlines(date_dict,data_struct='df')
    
    return headlines_df



def update_story(db_name,num_days=20):

    print('let update the stories!!!')
    dbm = get_db_manager(db_name)
    headlines_df = get_latest_headlines(db_name,num_days=num_days)
    
    exception_dict={}
    new_story = {}
    for index,row in headlines_df.iterrows():
        
        test_date = row['date']
        last_candle = datetime.datetime.strptime(test_date, '%Y-%m-%d %H:%M:%S')
        today = datetime.datetime.today()
        diff = today-last_candle
        #print(diff.days)
        if diff.days <=175:
        
            temps = t.time()
            story_id = row['story_id']
            story_exist = dbm.download_a_story(story_id)
        
        
            if len(story_exist)<=0:
                new_story[story_id]={}
                new_story[story_id]['story']=''
    
    if len(new_story)>0:
        exception_dict = news_feed.get_multiple_news_story(new_story)
        
        df = pd.DataFrame.from_dict(new_story,orient='index')
        df.index.name='story_id'
        dbm.save_a_story(df)
        temps = t.time()-temps
        print('downloading and saving {} story tooks {} seconds'.format(len(new_story),temps))
    else:
        print('all the stories were already downloaded')       
       
      
    
    return exception_dict


def create_ticker_dict(ticker,price_data):
    price_dict = price_data[ticker]
    
    ticker_dict = {}
    for key in price_dict:
        str_key = str(key)
        ticker_dict[str_key]=price_dict[key]
        
    return ticker_dict



def update_390m_features(candle_dict,candle_size=390):

    dh = date_manager.date_handler()
    date_list_390m = dh.create_date_list_from_dict(candle_dict)
    
    cm.init_features(candle_dict, date_list_390m,candle_size)
    
    
    fc.dict_add_390m_oc_gains(candle_dict)
    
    fc.dict_add_ema13_oc_gains(candle_dict) 
    fc.dict_add_d1_ema13_oc_gains(candle_dict)
    
    fc.dict_add_390m_next(candle_dict) 
    
    
    fc.nan_init_390m(candle_dict,date_list_390m)
    
    
    df = create_from(candle_dict)
    df.index.name='date'
    df.index = pd.to_datetime(df.index)
    
    return df



def update_390m_predictions(db_name):
 
    live_dbm = get_db_manager('live_database')
    
    iq_dbm = get_db_manager(db_name)
    
    date_morning = find_last_update_morning(db_name).to_pydatetime()
    
    df390 = iq_dbm.download_specific_day(date_morning,table='FEATURES_390M',cond_list=[],data_struct='df')
    
    lm = live_models.live_models()
    lm.predict_390m(df390)
    
    features = live_dbm.data_shape.features_390m
    daily_pred = df390[features]
    daily_pred.set_index('date',inplace=True)
    
    live_dbm.save_multiple_ticker(daily_pred,table='FEATURES_390M')
    
    
def get_init_bb_queue(init_df):

    bb_queue = queue.Queue(maxsize=20)   
    
    for index,row in init_df.iterrows():
        close = row['close']
        add_queue_value(bb_queue,close)
            
    return bb_queue


def add_queue_value(q,value):
    if q.full()==False:
        q.put(value)
    else:
        q.get()
        q.put(value)
        
        
    
        
'''
BEGIN INTRADAY_DAILY_UPDATE
'''   
        
def create_dict_row(col_list,row,date):

    '''
    Create a dictionary,from a dataframe, containing the value of every 
    column except the date.
    '''
    
    
    row_dict = {}
    for col in col_list:
        if col=='date':
            row_dict['date']=date
        else:
            row_dict[col]=row[col]
            
            
    return row_dict



def create_intraday_df(df):
    
    col=['date','ticker','previous_gains','RSI','d1_ema3_RSI','d2_ema3_RSI','normalize_bb',
         'd1_ema3_bb','d2_ema3_bb','PPO_line','d1_ema3_PPO_line','d2_ema3_PPO_line',
         'PPO_histo','d1_ema3_PPO_histo','d2_ema3_PPO_histo','vwap_ratio','time_index']
    
    intraday_df = df[col]
    
    data_shape = otc_object.otc_object()
    intraday_daily_col = data_shape.intraday_daily
    
    intraday_df.set_axis(intraday_daily_col,axis=1,inplace=True)
    intraday_df.set_index('date',inplace=True)
    
    return intraday_df



def init_intraday_daily(ticker,price_data,date_list,db_name):
    '''
    Since its an init function the date list contains all the date from 
    the beginning of 2021.
    '''
    temps = t.time()
        
    
    dbm = get_db_manager(db_name)
    
    table390 = 'FEATURES_390M'
    df390 = dbm.download_ticker(ticker,table390,date_list)
    
    if len(df390)>10 and ticker in price_data:
        init_df = df390[:10]
        
        bb_queue = get_init_bb_queue(init_df)
        daily_df = df390.dropna()
        
        ticker_dict = price_data[ticker]
        
        data_shape = otc_object.otc_object()
        col_list=data_shape.features_390m
        
        intraday_daily={}
        
        available = False # if daily data not available we cant calculate these features.
        first = True
        for d in ticker_dict:
            
            if first:
                first=False
                current_day = d.replace(hour=9,minute=30)
                previous_day = d.replace(hour=9,minute=30)
                row = df390.iloc[0]
                last_row = create_dict_row(col_list,row,previous_day)
            
            if d.hour==8:
               index = d.replace(hour=9,minute=30)
               previous_day = current_day
               current_day = index
               
               if previous_day in daily_df.index:
                   available=True
                   
                   row = daily_df.loc[previous_day]
                   
                   last_row = create_dict_row(col_list,row,previous_day)
                   
                   add_queue_value(bb_queue,last_row['close'])
                   
               else:
                   available=False
                   
            
            if available:
                #print(d)
                live_row = ticker_dict[d]
                
                cm.compute_intraday(last_row, live_row, bb_queue, d)
             
                intraday_daily[d]=live_row
        
        
        df = pd.DataFrame.from_dict(intraday_daily,orient='index')
        if len(df)>0:
            intraday_df = create_intraday_df(df)
        else:
            return []
        
        temps = t.time()-temps
        print('Computing the intraday daily of {} tooks {} seconds'.format(ticker,temps))
    
        return intraday_df
    
    else:
        print('Could not create the INTRADAY_DAILY for {}'.format(ticker))
        return []




def init_intraday_daily_list(ticker_list,db_name):

    table='INTRADAY_DAILY'
    dbm = get_db_manager(db_name)
    dh = date_manager.date_handler()
    calendar = dh.get_market_calendar(keep_2021=True,db_name=db_name)
    date_list = calendar.index.to_list()
    
    date_dict = get_init_date_dict(ticker_list,db_name=db_name)
    price_data = histo_bar_feed.download_price_data(ticker_list,date_dict)
    
    for ticker in ticker_list:
        intraday_df = init_intraday_daily(ticker,price_data,date_list,db_name)
        
        if len(intraday_df)>0:
            dbm.save_ticker_data(intraday_df, table, ticker)

        

def init_all_intraday_daily(db_name,split = 25):
    
    '''
    This function is used to init all the intraday_daily.
    '''

    dbm = get_db_manager(db_name)
    stock_info = dbm.download_stock_list()
    all_ticker = stock_info.index.to_list()
    splited_ticker = split_ticker_list(all_ticker,n_split=split)
    
    count = 0
    for ticker_list in splited_ticker:
        temps = t.time()
        count = count + 1
        init_intraday_daily_list(ticker_list,db_name)
        
        temps = t.time()-temps
        print('Split: {}/{} tooks {} seconds'.format(count,split,temps))



def create_intraday_update_bb_queue(data390,last_update,date_list):

    index = date_list.index(last_update)+1
    d_list = date_list[index-20:index]
    
    bb_queue = queue.Queue(maxsize=20) 
    
    '''
    In case the stock is really illiquid we upload the only data we know is valid. 
    '''
    d_index = str(last_update.replace(hour=9,minute=30))
    close = data390[d_index]['close']
    for d in d_list:
        d_index = str(d.replace(hour=9,minute=30))
        if d_index in data390:
            close = data390[d_index]['close']
            
        add_queue_value(bb_queue,close)
    
    
    #for x in range(20):
    #    value = bb_queue.get()
    #    print(value)
    
    return bb_queue



def remove_extra_row(price_dict,overnight_index):
    
    found = False #found the first overnight candle
    ticker_dict={}
    for d in price_dict:
        if found:
            ticker_dict[d]=price_dict[d]
        else:
            if d.hour==8:
                if d>=overnight_index:
                    found = True  
                    ticker_dict[d]=price_dict[d]
                    
    return ticker_dict




def create_updated_intraday_df(ticker,daily_date_list,price_data,dbm,last_update):
    
    table390 = 'FEATURES_390M'
    data390 = dbm.download_ticker(ticker,table390,daily_date_list,data_struct='dict')
    bb_queue = create_intraday_update_bb_queue(data390,last_update,daily_date_list)
    
    index = daily_date_list.index(last_update)+1
    if index < len(daily_date_list):
 
        date_index = daily_date_list[index]
        overnight_index = date_index.replace(hour=8)
        
        if ticker in price_data:
            price_dict = price_data[ticker]
            ticker_dict = remove_extra_row(price_dict,overnight_index)
        else:
            print('no price data for {}'.format(ticker))
            return []
        
        if len(ticker_dict)==0:
            print('no price data for {}'.format(ticker))
            return []
        
        previous_day = str(last_update.replace(hour=9,minute=30))
        current_day = str(date_index.replace(hour=9,minute=30))
        
        first=True
        intraday_daily={}
        for d in ticker_dict:
            if first:
                first = False
                last_row = data390[previous_day]
            else:
                if d.hour==8:
                    index = d.replace(hour=9,minute=30)
                    previous_day = str(current_day)
                    current_day = str(index)
                    last_row = data390[previous_day]
                    last_close = last_row['close']
                    add_queue_value(bb_queue,last_close)
                    
                    
            live_row = ticker_dict[d]  
            cm.compute_intraday(last_row, live_row, bb_queue, d)
            intraday_daily[d]=live_row
        
        df = pd.DataFrame.from_dict(intraday_daily,orient='index')
        
        if len(df)>0:
            intraday_df = create_intraday_df(df)
            return intraday_df
        else:
            return []
    else:
        print('{} was already updated'.format(ticker))
        return []



def get_intraday_daily_df(ticker,db_name,daily_date_list,price_data,dbm):

     
    
    intraday_table ='INTRADAY_DAILY'
    last_update,last_candle = find_last_update(ticker,intraday_table,db_name) 
    test_date = datetime.datetime.strptime('2021-01-14 00:00:00', '%Y-%m-%d %H:%M:%S')
    
    if last_update<=test_date:
        print('initializing the intraday_daily of {}'.format(ticker))
        intraday_df = init_intraday_daily(ticker,price_data,daily_date_list,db_name)
    else:
        intraday_df = create_updated_intraday_df(ticker,daily_date_list,price_data,dbm,last_update)    
    
    
    
    return intraday_df


def update_intraday_daily_list(ticker_list,date_list,daily_date_list,db_name):
    
    dbm = get_db_manager(db_name)
    
    intraday_table ='INTRADAY_DAILY'
    date_dict = get_update_date_dict(ticker_list,intraday_table,db_name,date_list)
    price_data = histo_bar_feed.download_price_data(ticker_list,date_dict)
    
    for ticker in ticker_list:
        temps = t.time()
        
        intraday_df = get_intraday_daily_df(ticker,db_name,daily_date_list,price_data,dbm)
          
        if len(intraday_df)>0:
            dbm.save_ticker_data(intraday_df, intraday_table, ticker)
            
        temps = t.time()-temps
        print('updating the intraday daily of {} tooks {} seconds'.format(ticker,temps))
        
        
def update_intraday_daily_database(db_name):
    dbm = get_db_manager(db_name)  
    
    stock_info = dbm.download_stock_list()
    all_ticker = stock_info.index.to_list()
    
    ticker_split = split_ticker_list(all_ticker)
    
    dh = date_manager.date_handler()
    date_list = dh.get_intraday_date_list(keep_2021=True,db_name=db_name) # providing the date list speed up the update process    
    calendar = dh.get_market_calendar(keep_2021=True,db_name=db_name)
    daily_date_list = calendar.index.to_list()
    
    count = 0
    for ticker_list in ticker_split:
        temps = t.time()
        
        update_intraday_daily_list(ticker_list,date_list,daily_date_list,db_name)
        
        temps = t.time()-temps
        count = count + len(ticker_list)
        print('Progress: {}/{}'.format(count,len(all_ticker)))
        print('updating {} stocks tooks {} seconds'.format(len(ticker_list),temps))
        
'''
END UPDATE INTRADAY_DAILY
''' 




'''
BEGIN DAILY UPDATE
'''

def create_390m_empty_dict(db_name):
    
    dh = date_manager.date_handler()
    calendar = dh.get_market_calendar(keep_2021=True,db_name=db_name)
    date_list = calendar.index.to_list()  
    
    ticker_dict = {}
    for d in date_list:
        index = d.replace(hour=9,minute=30)
        
        ticker_dict[index]={}
        
    return ticker_dict


def fill_old_daily_data(old_ticker_dict,ticker_dict):

    for d in old_ticker_dict:
        index = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        ticker_dict[index] =old_ticker_dict[d]
        
        
def find_daily_missing_date(ticker_dict):
    
    missing_date = []
    
    found_first = False
    for d in ticker_dict:
        if len(ticker_dict[d])==0:
            if found_first:
                missing_date.append(d)
        else:
            found_first=True
            
    return missing_date


def create_missing_date_list(missing_date):
    combined_list =[]
    dh = date_manager.date_handler()
    
    for date_morning in missing_date:
        ticker_date_list = dh.get_specific_date_list(date_morning,add_overnight=False)
        combined_list.append(ticker_date_list)
    
    return combined_list


def get_update_daily_data(ticker,daily_date_list,dbm):

    table390='FEATURES_390M' 
    old_ticker_dict = dbm.download_ticker(ticker, table390, daily_date_list,data_struct='dict')
    
    ticker_dict = create_390m_empty_dict(dbm.db_name)
    fill_old_daily_data(old_ticker_dict,ticker_dict)  
    
    missing_date = find_daily_missing_date(ticker_dict)
    combined_list = create_missing_date_list(missing_date)
    
    return ticker_dict,missing_date,combined_list



def create_update_daily_date_list(ticker,daily_date_list,missing_date,date_dict):
    missing_date = date_dict[ticker]['missing_date']
    
    if len(missing_date)>0:
        first_date_missing = missing_date[0]
    else:
        return [],[]
    
    first_index = daily_date_list.index(first_date_missing.replace(hour=0,minute=0))
    d_list = daily_date_list[first_index:]
    
    dh = date_manager.date_handler()
    date_list = dh.create_custom_date_list(d_list,add_overnight=False)
    missing_date_list = dh.create_custom_date_list(missing_date,add_overnight=False)

    return date_list,missing_date_list



def create_missing_daily_candles(ticker,price_data,date_dict):
    price_dict = price_data[ticker]
    
    combined_list = date_dict[ticker]['combined_list']
    
    new_candles ={}
    volume = 0 # if volume stay 0 we know there was no price data. 
    for date_list in combined_list:
        
        candle_index = date_list[0].replace(hour=9,minute=30)
        
        first=True
        for d in date_list:
            if d in price_dict:
                
                if first:
                    first=False
                    
                    op = price_dict[d]['open']
                    volume = price_dict[d]['volume']
                    low = price_dict[d]['low']
                    high = price_dict[d]['high']
                    barcount = 0
                    ticker = price_dict[d]['ticker']
                    average = price_dict[d]['average']*volume
                    close = price_dict[d]['close']
                else:
                    vol = price_dict[d]['volume'] # candle volume
                    volume = volume + vol # bigger candle volume
                    if price_dict[d]['low']<low:
                        low = price_dict[d]['low']
                    if price_dict[d]['high']>high:
                        high = price_dict[d]['high']
                    
                    avg = price_dict[d]['average']
                    average = average+ (vol*avg)
                    
                    close = price_dict[d]['close']
         
        if volume==0:
            new_candles[candle_index]={}
        else:
            average=average/volume
            
            new_candles[candle_index]={}
            new_candles[candle_index]['open']=op
            new_candles[candle_index]['high']=high
            new_candles[candle_index]['low']=low
            new_candles[candle_index]['close']=close
            new_candles[candle_index]['volume']=volume
            new_candles[candle_index]['barCount']=barcount
            new_candles[candle_index]['average']=average
            new_candles[candle_index]['ticker']=ticker
            
    return new_candles



def fill_daily_ticker_dict(ticker,stock_dict,price_data,date_dict):
    ticker_dict = stock_dict[ticker] 
    
    if ticker in price_data:
        new_candles = create_missing_daily_candles(ticker,price_data,date_dict)          
               
        for d in new_candles:
            ticker_dict[d]=new_candles[d]     
     
    
     
    '''
    Fill missing candle. 
    '''    
    first=True
    for d in ticker_dict:
        if first:
            first=False
            previous_row = ticker_dict[d]
        else:
            current_row =ticker_dict[d]
            
            if len(current_row)==0:
                #print(d)
                price = previous_row['close']
                current_row['open']=price
                current_row['high']=price
                current_row['low']=price
                current_row['close']=price
                current_row['volume']=0
                current_row['barCount']=0
                current_row['average']=price
                current_row['ticker']=previous_row['ticker']
                
                
        previous_row = ticker_dict[d]


def remove_first_empty_row(stock_dict,ticker):
  
    ticker_dict = stock_dict[ticker]  
    new_dict = {}
    found=False
    for d in ticker_dict:
        if len(ticker_dict[d])>0:
            found=True
        
        if found:
            new_dict[d]=ticker_dict[d]
        
    stock_dict[ticker] = new_dict
    
    
def update_daily_ticker_list(ticker_list,db_name):
    
    dbm = get_db_manager(db_name) 
    
    dh = date_manager.date_handler()
    calendar = dh.get_market_calendar(keep_2021=True,db_name=db_name)
    daily_date_list = calendar.index.to_list()    
    
    print('initializing the daily date dict')
    date_dict = get_init_date_dict(ticker_list,db_name=db_name)
    
    
    '''
    Create the needed time data and pull the data present in the database
    for every ticker in the list.
    '''
    
    stock_dict ={}
    for ticker in ticker_list:
        
        print('Preparing the daily data for {}'.format(ticker))
        ticker_dict,missing_date,combined_list =get_update_daily_data(ticker,daily_date_list,dbm)
        stock_dict[ticker]=ticker_dict
        date_dict[ticker]['missing_date']=missing_date
        date_dict[ticker]['combined_list']=combined_list
        date_list,missing_date_list = create_update_daily_date_list(ticker,daily_date_list,missing_date,date_dict)
        
        date_dict[ticker]['date_list']=date_list
        date_dict[ticker]['missing_date_list']=missing_date_list
        
        if len(missing_date)>0:
            date_dict[ticker]['start_date'] = missing_date[0]
        
    
    
    price_data = histo_bar_feed.download_price_data(ticker_list,date_dict)
    
    
    '''
    Combine the old data with the new and fill the empty candle
    '''
    
    for ticker in ticker_list:
        remove_first_empty_row(stock_dict,ticker)
        fill_daily_ticker_dict(ticker,stock_dict,price_data,date_dict)       
               
     
    '''
    Delete the old data and add the new.
    '''
    table390='FEATURES_390M'
    failed_daily_update =[]    
    for ticker in ticker_list:
        ticker_dict = stock_dict[ticker]
        
        df390 = update_390m_features(ticker_dict)
        
        if len(df390)>0:
            dbm.delete_390m_ticker(ticker)
            dbm.save_ticker_data(df390, table390, ticker,check_duplicate=False)
        else:
            failed_daily_update.append(ticker)
            
    return failed_daily_update




def update_390m_database(db_name):

    dbm = get_db_manager(db_name)  
    stock_info = dbm.download_stock_list() 
    ticker_list = stock_info.index.to_list() 
    #ticker_list = ticker_list[0:20]
    
    failed_daily_update = update_daily_ticker_list(ticker_list,db_name)
    
    return failed_daily_update


def create_ng_list(r=20):
    next_list = []
    for x in range(r):
        i = x+1
        ng_text = 'next_gain'+str(i)
        next_list.append(ng_text)
    
    next_list.append('highest_ng20')
    next_list.append('lowest_ng20')
    next_list.append('hl_ratio20')
    
    return next_list


def refresh_ticker_ng(ticker_dict):
    
    fc.dict_add_next_gain_20(ticker_dict)
    fc.init_highest_ng(ticker_dict,ng=20)
    fc.init_lowest_ng(ticker_dict,ng=20)
    fc.init_hl_ratio(ticker_dict,ng=20)


def recreate_ticker_ng(ticker,date_list,dbm,table,next_list):

    temps = t.time()
    #dbm = get_db_manager(db_name)
    
    ticker_dict = dbm.download_ticker(ticker,table,date_list,data_struct='dict')
    
    dh = date_manager.date_handler()
    intraday_date_list = dh.create_date_list_from_dict(ticker_dict)
    
    refresh_ticker_ng(ticker_dict)
    
    dbm.update_all_ng(ticker_dict,next_list,intraday_date_list,ticker,table=table)
    
    temps = t.time()-temps
    print('Refresing the next gain of {} tooks {} seconds'.format(ticker,temps))

'''
END DAILY UPDATE
'''  



def split_stock_list(stock_list,split_n=20):

    length = len(stock_list)
    split = int(length/split_n)+1
    
    df_list = []
    
    for x in range(split):
        start = x*split_n
        end = (x+1)*split_n
        
        if start<length:
            temp_df = stock_list[start:end]
            df_list.append(temp_df)

    return df_list 


def initialize_american_database():
    '''
    Since initialy the features is kept the same, we can use the 
    same function to initiale the new database. Note the intraday_daily
    is not initialized with this function. 
    '''
    date='20220720'
    stock_list = msf.request_summary(date,mc_limit=200000000,min_cash = 200000)
    #stock_list = stock_list[0:3]
    db_name='american_database'
    
    df_list = split_stock_list(stock_list)
    
    count = 0
    for stock_df in df_list:
        count = count+1
        temps = t.time()
        init_multiple_stock(stock_df, db_name)
        temps = t.time()-temps
        print('')
        print('*****')
        print('round: {}, initializing {} stocks tooks {} seconds'.format(count,len(stock_df),temps))
        print('*****')
        print('')
    
    update_intraday_daily_database(db_name='american_database')
    
        

def update_all_database():
    temps1 = t.time()
    #update_1m_database('otc_database')
    update_1m_database('american_database')
    temps1 = t.time()-temps1
    
    temps2 = t.time()
    #failed_daily_update = update_390m_database('otc_database')
    failed_daily_update_american = update_390m_database('american_database')
    temps2 = t.time()-temps2
    
    temps3 = t.time()
    #new_stock_info = add_new_otc_stocks()
    new_stock_info_american = add_new_stocks('american_database')
    temps3 = t.time()-temps3
    
    temps4 = t.time()
    #update_headlines('otc_database')
    update_headlines('american_database')
    
    #exception_story = update_story('otc_database',num_days=20)
    exception_story_american = update_story('american_database',num_days=20)
    temps4 = t.time()-temps4
    
    temps6 = t.time()
    #update_intraday_daily_database(db_name='otc_database')
    #update_intraday_daily_database(db_name='american_database')
    temps6 = t.time()-temps6
    
    temps5 = t.time()
    #update_390m_predictions('otc_database')
    update_390m_predictions('american_database')
    temps5 = t.time()-temps5
    
    #temps7 = t.time()
    #test_dict = rt.run_test([390],db_name='american_database')
    #temps7 = t.time()-temps7
    
    print('')
    print('Updating the 1m database tooks {} seconds'.format(temps1))
    print('Updating the 390m database tooks {} seconds'.format(temps2))
    print('Adding the new stocks tooks {} seconds'.format(temps3))
    print('Updating the headlines and story tooks {} seconds'.format(temps4))
    print('Updating the daily predictions tooks {} seconds'.format(temps5))
    print('Updating the INTRADAY_DAILY tooks {} seconds'.format(temps6))
    #print('Performing the tests tooks {} seconds'.format(temps7))
    print('')
    total_temps = temps1+temps2+temps3+temps4+temps5+temps6
    print('updating all database tooks: {} seconds'.format(total_temps))
    
    print('')
    print('{} stocks were added'.format(len(new_stock_info_american)))
    
    return new_stock_info_american,exception_story_american,failed_daily_update_american



#update_intraday_daily_database(db_name='otc_database')
#update_all_database()



















#end