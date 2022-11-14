# -*- coding: utf-8 -*-
"""
Created on Sun Aug 15 14:37:18 2021

@author: marti
"""

import warnings
warnings.filterwarnings(action='ignore', category=UserWarning)
warnings.filterwarnings(action='ignore', category=FutureWarning)


import time as t
import datetime
import pandas as pd

import pyiqfeed as iq
from localconfig.passwords import dtn_product_id, dtn_login, dtn_password

import live_feed
import news_feed

#import db_updater
import db_histo_updater

import live_test as lt
#import db_manager
import db_histo_manager as dhm
import date_manager
import live_models
import refill_live_price
import test_service
import prepare_live_data as pld

import otc_object
import live_object
import american_object
  

"""
This module is the core of the live system. It coordinates the different modules in order 
to process the live data and make predictions as fast as possible. 
"""

  

def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = test_service.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch(headless=False)


def stop_feed():
    launch_service()
    print('stopping the feed')
    quote_conn = iq.QuoteConn(name="pyiqfeed-Example-trades-only")
    with iq.ConnConnector([quote_conn]) as connector:
        quote_conn.unwatch_all()
        quote_conn.news_off()
        
        
def get_db_manager(db_name):
    
    if db_name=='otc_database':
        data_shape = otc_object.otc_object()
    if db_name=='live_database':
        data_shape = live_object.live_object()
    if db_name=='american_database':
        data_shape = american_object.american_object()
        
    dbm = dhm.db_manager(db_name,data_shape)
    
    return dbm


def update_candle(live_date,new_candle,first_candle,feed,lm):
    #print('starting a new candle: {}'.format(new_candle))
               
    if live_date == first_candle:
        dbm = get_db_manager(db_name='live_database')
      
        feed.fill_overnight_candle()  
        
        overnight_df = feed.get_new_overnight_df() # the four line above allow us to revert back to the original algo
        lm.predict_1m(overnight_df)
        
        table='FEATURES_1M'
        dbm.save_multiple_ticker(overnight_df,table)
        
    feed.fill_inactive(live_date)
    
    previous_date = feed.get_previous_candle()
    feed.compute_feats(previous_date, live_date, approx=False)
    
    live_df = feed.get_live_df()
    
    feed.set_live_candle(new_candle)
    feed.set_previous_candle(live_date)
    
    return live_df


def daily_approximation(feed):
    feed.approximate_daily_features()
    daily_df = feed.get_daily_df()
    
    return daily_df


def add_stories(news_conn,story_list,dbm):
    
    new_story = {}
    for story_id in story_list:
        new_story[story_id]={}
        
        try:
            story = news_conn.request_news_story(story_id)
            story_text = story.story
            new_story[story_id]['story']=story_text
            
        except Exception as e:
            print('caught an exception:')
            print(e)
            
    df = pd.DataFrame.from_dict(new_story,orient='index')
    df.index.name='story_id'
    dbm.save_a_story(df)
    
            


def approximate_features(live_date,first_candle,feed,approx_ratio):
    if live_date==first_candle:
        '''
        Let add the code here to approximate the first minute
        '''
        live_df = feed.get_first_approximation_df(approx_ratio)
        
    else:
        previous_date = feed.get_previous_candle()
        approx=True
        feed.compute_feats(previous_date, live_date, approx,approx_ratio)
        live_df = feed.get_live_df()
        
        live_df = live_df.loc[live_df['barCount']>0]
        
        
    return live_df
        
        

def re_init(new_candle,feed):
    '''
    !!! this function is incomplete.
    '''
    init = 40
    previous_date = feed.find_previous_date(new_candle)
    feed.set_previous_candle(previous_date)
    feed.set_live_candle(new_candle)
    
    feed.re_init_features(previous_date)
    
    return init

    
def init_listener(db_name):
    
    dbm = get_db_manager(db_name=db_name)
    dh  = date_manager.date_handler()
    
    print('Pulling the required data')
    temps = t.time()
    
    watchlist = pld.create_watchlist(db_name)

    price_dict = pld.create_price_dict(watchlist,db_name)
    
    bb_dict = pld.get_bb_dict(watchlist,db_name)
    vwap_dict = pld.get_vwap_queue_dict(watchlist,db_name)
    
    daily_dict = pld.prepare_live_daily_data(watchlist,db_name)
    
    date_list = dh.live_price_date_list(candle_size=1,data_struct='data')
    
    first_candle = dh.get_today_first_candle()
    overnight_candle = dh.get_today_overnight()
  
    
    '''
    If we start the system pass 9:30am we have to pull the necessary data from iqfeed.
    '''
    today = datetime.datetime.today()
    if today > first_candle:
        print('Recreating the price dict with up to date data')
        refill_live_price.recreate_live_data(price_dict,watchlist,bb_dict,vwap_dict,daily_dict)
        
    '''
    Initialize the feed with the relevant data.
    '''
    last_candle = dh.get_last_market_candle()
    feed = live_feed.FreedomListener(name="live feed",
                           overnight_candle=overnight_candle,
                           first_candle=first_candle,
                           price_dict=price_dict,
                           watchlist=watchlist,
                           date_list=date_list,
                           bb_dict=bb_dict,
                           vwap_dict=vwap_dict,
                           daily_dict=daily_dict,
                           last_candle=last_candle)
    
    temps = t.time()-temps
    print('pulling the required data tooks {} seconds'.format(temps))
    
    return feed,watchlist


def update_news(watchlist,db_name):
    ticker_list=[]
    for ticker in watchlist:
        ticker_list.append(ticker)
        
    news_feed.update_latest_news(ticker_list,db_name)
      
   
     
def main():
    
        import warnings
        warnings.filterwarnings(action='ignore', category=UserWarning)
        warnings.filterwarnings(action='ignore', category=FutureWarning)
        
        '''
        Init the model and data feed
        '''
        
        db_name='american_database'
        
        dbm_histo = get_db_manager(db_name)
        dbm = get_db_manager(db_name='live_database')
        dh  = date_manager.date_handler()
        lm = live_models.live_models() 
        
        first_candle = dh.get_today_first_candle()
        
        feed,watchlist = init_listener(db_name)
        quote_conn = iq.QuoteConn(name="freedom_conn")
        quote_conn.add_listener(feed)
        update_fields = feed.get_update_fields()
        
        '''
        Before we start pulling the tick data we update the latest news
        '''
        update_news(watchlist,db_name)
        
        '''
        Create the news conn
        '''
        news_conn = iq.NewsConn("pyiqfeed-example-News-Conn")
        news_listener = iq.VerboseIQFeedListener("NewsListener")
        news_conn.add_listener(news_listener)
    
        launch_service()
        with iq.ConnConnector([quote_conn,news_conn]) as connector:
            
            print('starting the feed')
            '''
            Request the data from the watchlist.
            '''
            
            quote_conn.select_update_fieldnames(update_fields) #highly likely that we only need to call this once.
            
            for ticker in watchlist:       
                #quote_conn.select_update_fieldnames(update_fields)
                quote_conn.trades_watch(ticker)
        
            '''
            Add the real time news.
            '''
            quote_conn.news_on()
            
            '''
            time monitoring
            '''
        
            today = datetime.datetime.today()
            
            waiting_count = 20
            while today < first_candle:
                
                # launch service maybe ?
                waiting_count = waiting_count + 1
                if waiting_count >=20:
                    waiting_count = 0
                    print('market are not open yet.')
                
                today = datetime.datetime.today()
            
                t.sleep(3)
                
            print('the market are open !!!!!!!!!!!')
            
            today = datetime.datetime.today()
            hour = today.hour
            init=0 # in case we lost some candles
            #approx_index = 999 # no approximation for the first minute
            approx_index = 0 # testing approximation for the first minute
            
            while hour<16: # this is the main loop.
                '''
                The datetime value needed are computed first.
                '''
                today = datetime.datetime.today()
                new_candle = today.replace(second=0,microsecond=0)
                hour = new_candle.hour
                live_date = feed.get_live_candle()
                td = new_candle-live_date
                
                '''
                Checking if the candle is completed
                '''
                
                if td.seconds==60:
                    t.sleep(0.2) # testing if a delay will prevent the anomalies between the histo and live data. 
                    temps = t.time() 
                    
                    live_df = update_candle(live_date, new_candle, first_candle,feed,lm)
                    lm.predict_1m(live_df)
                    
                    if init > 0:
                        print('{} candle before the features are re initialize'.format(init))
                        init = init - 1
                        
                    
                    #dbm.save_live_data(live_df, candle_size=1)
                    table='FEATURES_1M'
                    dbm.save_multiple_ticker(live_df,table)
                    
                    approx_index=0
                    temps = t.time()-temps
                    print('Computing and saving {} took {} seconds'.format(live_date,temps))
                
                    if hour >=15:
                        
                        temps = t.time()
                        
                        daily_df = daily_approximation(feed)
                        lm.predict_390m(daily_df)
                        
                        #print('!!!!!!1')
                        #print(len(daily_df.columns))
                        #print('!!!!!!1')
                        
                        table='APPROX_390'
                        dbm.save_multiple_ticker(daily_df,table)
                
                        temps = t.time()-temps
                        print('approximating the daily features tooks {} seconds'.format(temps))
                        
                    temps = t.time()   
                    headlines_df,story_list = feed.get_latest_headlines()
                    if len(headlines_df)>0:
                        print('{} news were pulled'.format(len(headlines_df)))
                        dbm_histo.save_headlines(headlines_df)
                        add_stories(news_conn,story_list,dbm_histo)
                        temps = t.time()-temps
                        print('pulling and saving the news tooks {} seconds'.format(temps))
                    else:
                        print('There was no headlines')
                    
                elif td.seconds>60:
                    
                    print('The system is missing many candle we need to re initialize.')
                    init = re_init(new_candle,feed)
                    
                    '''
                    Need to add the missing data row to the database.  
                    '''
                    
                elif new_candle == first_candle:
                    '''
                    Pull and save the overnight data as fast as possible. 
                    '''
                    overnight_df = feed.get_new_overnight_df()
                    
                    if len(overnight_df)>=1:
                        lm.predict_1m(overnight_df)
                        
                        #dbm.save_live_data(overnight_df, candle_size=1)
                        table='FEATURES_1M'
                        dbm.save_multiple_ticker(overnight_df,table)
                        
                    
                    second = int(today.second)
                    if second>=2:
                        if second>approx_index:
                            approx_index=second
                            approx_ratio = 60/second
                            
                            live_df = approximate_features(live_date, first_candle, feed, approx_ratio)
                            
                            if len(live_df)>0:
                                lm.predict_1m(live_df)
                                table='APPROX_1M'
                                
                                dbm.update_approx_table(live_df, table)
                        
              
                else:
                    '''
                    We approximate the predictions every 5 seconds
                    '''
                    
                    if init <39:
                        
                        second = int(today.second)
                        if second>=2: #make sure the data was updated properly.
                            if second>approx_index:
                                approx_index=second
                                approx_ratio = 60/second
                                
                                live_df = approximate_features(live_date, first_candle, feed, approx_ratio)
                                
                                if len(live_df)>0:
                                    lm.predict_1m(live_df)
                                    table='APPROX_1M'
                                    
                                    dbm.update_approx_table(live_df, table)
                     
                    
            
            stop_feed()
            print('Another completed day toward freedom!!!!!')
            
        
        feed_price_dict = feed.get_price_dict()
        news_list = feed.get_news_list()
        
        histo_feed = True
        while histo_feed:
            '''
            Waiting for 16:05 to start the update 
            '''
            today = datetime.datetime.today()
            if today.minute >=5:
                histo_feed =False
        
        #new_stock_info,exception_story,test_dict,failed_daily_update = db_histo_updater.update_all_database()
        new_stock_info,exception_story,failed_daily_update =db_histo_updater.update_all_database()
        print('start the test')
        
        
        '''
        Note, the live test has to be completed before we retrain the model. Otherwise,
        we will test the model on data it has already seen. 
        '''
        
        lt.save_stock_list(db_name) # save the stock list to be able to restart the database if we lose it.
        summary = lt.evaluate_predictions(db_name)
        
        today = datetime.datetime.today()
        week_day = today.weekday() 
        
        if week_day>=4:
            
            temps = t.time()
            import train_live_model
            train_live_model.train_all_models()
            
            temps = t.time()-temps
            
            print('')
            print('training the live models tooks {} seconds'.format(temps))
        
        today = datetime.datetime.today()
        print('completed the update at: {}'.format(today))
        
        return feed_price_dict,new_stock_info,exception_story,summary,failed_daily_update,news_list


if __name__=='__main__':
    feed_price_dict,new_stock_info,exception_story,summary,failed_daily_update,news_list = main()
    
    












# end


