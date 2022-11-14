# -*- coding: utf-8 -*-
"""
Created on Fri Mar 18 09:34:59 2022

@author: marti
"""

import pyiqfeed as iq
from localconfig.passwords import dtn_product_id, dtn_login, dtn_password
import test_service

import datetime
import pandas as pd
import time as t

import db_histo_manager as dhm
import otc_object
import american_object
import date_manager


def get_db_manager(db_name):
    
    if db_name=='otc_database':
        data_shape = otc_object.otc_object()
    if db_name=='american_database':
        data_shape = american_object.american_object()
        
    dbm = dhm.db_manager(db_name,data_shape)
    
    return dbm



def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = test_service.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch(headless=False)


def get_news_headlines(symbols=[],limit=100000):
    """Download the news headlines from the list of symbols"""
    
    news_conn = iq.NewsConn("pyiqfeed-example-News-Conn")
    news_listener = iq.VerboseIQFeedListener("NewsListener")
    news_conn.add_listener(news_listener)
    
    headlines_dict = {}

 
    launch_service()
    with iq.ConnConnector([news_conn]) as connector:
       
        headlines = news_conn.request_news_headlines(
            sources=[], symbols=symbols, date=None, limit=limit)
          
       
        for row in headlines: 
            d = row[3]
            date_string = str(d)
            
            tick_time = row[4]
            time_in_microseconds = (tick_time/60000000)
            time_in_minutes = datetime.timedelta(minutes= time_in_microseconds) 
            tod = str(time_in_minutes) 
            
            candle_date = datetime.datetime.strptime((date_string+' '+tod), '%Y-%m-%d %H:%M:%S') 
            candle_date = candle_date.replace(second=0)
            
            story_id = row[0]
            distributor = row[1]
            title = row[5]
            stock_number = len(row[2])
            
            headlines_dict[story_id]={}
            for ticker in row[2]:
                headlines_dict[story_id][ticker]={}
                headlines_dict[story_id][ticker]['stock_number']=stock_number
                headlines_dict[story_id][ticker]['distributor']=distributor
                headlines_dict[story_id][ticker]['date']=candle_date
                headlines_dict[story_id][ticker]['headline']= title

        return headlines_dict


def prepare_headline_df(headlines):
    
    col_headline = ['story_id','ticker','stock_number','distributor','date','headline']
    
    df = pd.DataFrame(columns = col_headline)
    data_list = []
 
    for story_id in headlines:
        for ticker in headlines[story_id]:
            data = []
            data.append(story_id)
            data.append(ticker)
            data.append(headlines[story_id][ticker]['stock_number'])
            data.append(headlines[story_id][ticker]['distributor'])
            data.append(headlines[story_id][ticker]['date'])
            data.append(headlines[story_id][ticker]['headline'])
            data_list.append(data)
            
    df = pd.DataFrame(data_list,columns=col_headline)
            
    return df



def get_headlines(ticker_list):  
    
    temps = t.time()
    headlines = get_news_headlines(ticker_list)
    temps = t.time()-temps
    print('get_news_headlines tooks {} seconds'.format(temps))
    
    temps = t.time()
    df = prepare_headline_df(headlines)
    temps = t.time()-temps
    print('prepare headlines df tooks {} seconds'.format(temps))
    
    return df



def get_news_story(story_id):
    """Exercise NewsConn functionality"""
    news_conn = iq.NewsConn("pyiqfeed-example-News-Conn")
    news_listener = iq.VerboseIQFeedListener("NewsListener")
    news_conn.add_listener(news_listener)

    launch_service()
    with iq.ConnConnector([news_conn]) as connector:
        
        story = news_conn.request_news_story(story_id)
        
        return story.story
    
    
def get_multiple_news_story(new_story):
    news_conn = iq.NewsConn("pyiqfeed-example-News-Conn")
    news_listener = iq.VerboseIQFeedListener("NewsListener")
    news_conn.add_listener(news_listener)

    launch_service()
    
    temps = t.time()
    exception_dict={}
    with iq.ConnConnector([news_conn]) as connector:
        
        for story_id in new_story:
            
            try:
                story = news_conn.request_news_story(story_id)
                story_text = story.story
                
                new_story[story_id]['story']=story_text
                #new_story[story_id]=story_text
            except Exception as e:
                print('caught an exception:')
                print(e)
                exception_dict[story_id]=e  
            
            
        temps = t.time()-temps
        print('downloading {} stories tooks {} seconds'.format(len(new_story),temps))
        
        return exception_dict
    
    
def keep_latest_headlines(headlines):

    dh  = date_manager.date_handler()
    last_candle = dh.get_last_market_candle()
    
    latest_headlines = {}
    
    for story_id in headlines:
        for ticker in headlines[story_id]:
            date_index = headlines[story_id][ticker]['date']
            if date_index>=last_candle:
                latest_headlines[story_id]=headlines[story_id]
            break
        
    return latest_headlines


def download_latest_headlines(ticker_list):
    
    headlines = get_news_headlines(ticker_list)
    latest_headlines = keep_latest_headlines(headlines)
    
    return latest_headlines

def download_latest_stories(latest_headlines):

    story_dict = {}
    for story_id in latest_headlines:
        story_dict[story_id]={}
    
    exception_dict = get_multiple_news_story(story_dict)
    
    return story_dict
    
    
def pull_latest_news(ticker_list):
    temps = t.time()
    
    latest_headlines = download_latest_headlines(ticker_list)
    df_headlines = prepare_headline_df(latest_headlines)
    story_dict = download_latest_stories(latest_headlines)
    df_stories = pd.DataFrame.from_dict(story_dict,orient='index')
    df_stories.index.name='story_id'
    
    temps = t.time()-temps
    print('Pulling the latest news tooks {} seconds'.format(temps))
    
    return df_headlines,df_stories
    

def save_latest_news(df_headlines,df_stories,db_name):
    
    dbm = get_db_manager(db_name)
    dbm.save_headlines(df_headlines)
    dbm.save_a_story(df_stories)

  
def update_latest_news(ticker_list,db_name):
    df_headlines,df_stories = pull_latest_news(ticker_list)
    save_latest_news(df_headlines,df_stories,db_name)
        
    



# end