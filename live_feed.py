# -*- coding: utf-8 -*-
"""
Created on Fri Jul 16 13:54:13 2021

@author: marti
"""



"""
This module creates a listener to process the live tick data coming from iqfeed. 
"""


import pyiqfeed as iq
from pyiqfeed import VerboseIQFeedListener
from pyiqfeed import QuoteConn
from pyiqfeed import FeedConn
from typing import Sequence

from localconfig.passwords import dtn_product_id, dtn_login, dtn_password

#import time as t
import pandas as pd
import datetime
import numpy as np
import math

import compute_module as cm
import features_creator as fc

import test_service


'''

'''
    
def launch_service():
    """Check if IQFeed.exe is running and start if not"""

    svc = test_service.FeedService(product=dtn_product_id,
                         version="Debugging",
                         login=dtn_login,
                         password=dtn_password)
    svc.launch(headless=False)



class FreedomListener(VerboseIQFeedListener):
    """
    Verbose version of SilentQuoteListener.

    See documentation for SilentQuoteListener member functions.

    """
    
    update_fields = ['Most Recent Trade','Most Recent Trade Size','Most Recent Trade Time']
    

    def __init__(self, name,overnight_candle,first_candle,price_dict,watchlist,date_list,bb_dict,vwap_dict,daily_dict,last_candle):
        super().__init__(name)
        
        today = datetime.datetime.today()
        today =today.replace(microsecond=0)
        today =today.replace(second=0)
        today = today.replace(minute=0)
        today = today.replace(hour=0)
        self.today_date = today
        
        self.first_candle = first_candle
        self.overnight_candle = overnight_candle
        self.price_dict = price_dict
        self.completed_candle = False
        self.watchlist = watchlist
        self.previous_candle = overnight_candle
        self.live_candle = first_candle
        self.last_candle=last_candle
        
        self.market_open = True
        self.overnight_fill = False
        self.date_list = date_list
        self.daily_date_list = date_list[-390:]
        self.bb_dict = bb_dict
        self.vwap_dict = vwap_dict
        self.count=10 # used to print the first few update
        self.update_count=0 # used to print the first few update
    
        self.daily_dict = daily_dict
        
        self.already_saved_overnight = []
        self.news_list=[]
        self.headlines_dict={}
     
    
    
    def get_daily_dict(self):
        return self.daily_dict
    
    def approximate_daily_features(self):
        self.compute_daily_price()
        self.compute_all_390_features()
    
    def compute_all_390_features(self):
        for ticker in self.watchlist: 
            self.compute_390_features(ticker)
    
    def compute_390_features(self,ticker):
        
        live_row = self.daily_dict[ticker]['live_row']
        last_row = self.daily_dict[ticker]['last_row']
        bb_queue = self.daily_dict[ticker]['bb_queue']
        price_q = self.daily_dict[ticker]['price_q']
        volume_q = self.daily_dict[ticker]['volume_q']

        cm.approximate_390m_features(last_row, live_row, self.live_candle, 
                                     bb_queue, price_q, volume_q)
        
        fc.add_live_390m_oc_gains(last_row,live_row)
        fc.add_live_ema13_oc_gains(last_row,live_row)
        fc.add_live_d1_ema13_oc_gains(last_row,live_row)
    
    
    def compute_daily_price(self):
        for ticker in self.watchlist:
            self.daily_dict[ticker]['live_row']=self.create_bigger_candle(ticker)
    
    
    def create_bigger_candle(self,ticker):
      
        volume = 0
        barcount = 0
        average = 0.0
        high = 0.0
        low = 9999999
        
         
        op = self.price_dict[self.first_candle][ticker]['open']
        close = 999
        
        for d in self.daily_date_list:
            
            vol = self.price_dict[d][ticker]['volume']
            avg = self.price_dict[d][ticker]['average']
            bar = self.price_dict[d][ticker]['barCount']
            h =  self.price_dict[d][ticker]['high']
            l =  self.price_dict[d][ticker]['low']
            cl = self.price_dict[d][ticker]['close']
            
            if np.isnan(cl)==False:
                close = cl
                
                if h > high:
                    high = h
                if l < low:
                    low = l
                    
                volume = volume + vol
                barcount = barcount + bar
                average = average+ (vol*avg)
            if d == self.live_candle:
                break
            
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

    
    
    def get_overnight_fill(self):
        return self.overnight_fill
    
    def set_overnight_fill(self,value):
        self.overnight_fill = value
        
    def get_price_dict(self):
        return self.price_dict
      
    def get_market_open(self):
        return self.market_open()
    
    def set_market_open(self,value):
        self.market_open = value
    
    def get_previous_candle(self):
        return self.previous_candle
    
    def set_previous_candle(self,new_candle):
        self.previous_candle = new_candle
        
    def get_live_candle(self):
        return self.live_candle
    
    def set_live_candle(self,new_candle):
        self.live_candle = new_candle
    
    def get_completed_candle(self):
        return self.completed_candle
        
    def get_update_fields(self):
        return self.update_fields
    
    
    def get_news_list(self):
        return self.news_list
    
    
    def get_latest_headlines(self):
        story_list = []
        if len(self.headlines_dict)>0:
            headlines_df = self.prepare_headline_df(self.headlines_dict)
            
            for story_id in self.headlines_dict:
                story_list.append(story_id)
            
            self.headlines_dict={} # reseting the headlines dict 
        else:
            headlines_df = []
            
        return headlines_df,story_list
    

    def process_invalid_symbol(self, bad_symbol: str) -> None:
        print("%s: Invalid Symbol: %s" % (self._name, bad_symbol))

    def process_news(self, news_item: QuoteConn.NewsMsg) -> None:
        self.news_list.append(news_item)
        self.update_headlines(news_item)
    
           
    def update_headlines(self,headlines):
        #print(headlines)
        for row in headlines: 
            """
            d = row[3]
            date_string = str(d)
            story_id = row[0]
            distributor = row[1]
            title = row[5]
            stock_number = len(row[2])
            tick_time = row[4]
            """
            d = headlines.story_date
            date_string = str(d)
            story_id = headlines.story_id
            distributor = headlines.distributor
            title = headlines.headline
            tick_time = headlines.story_time
            symbol_list = headlines.symbol_list
            
            ticker_list = []
            for ticker in symbol_list:
                if (len(ticker)>=2) and (len(ticker)<=5):
                    ticker_list.append(ticker)
                    
            stock_number = len(ticker_list)
            
            #print('date:{},tick_time:{},story_id:{},distributor:{},title:{},stock_number:{}'.format(d,tick_time,story_id,distributor,title,stock_number))
            
            time_in_microseconds = (tick_time/60000000)
            time_in_minutes = datetime.timedelta(minutes= time_in_microseconds) 
            tod = str(time_in_minutes) 
            
            candle_date = datetime.datetime.strptime((date_string+' '+tod), '%Y-%m-%d %H:%M:%S') 
            candle_date = candle_date.replace(second=0)
            
            
            
            if story_id not in self.headlines_dict:
                self.headlines_dict[story_id]={}
            
            for ticker in ticker_list:
                self.headlines_dict[story_id][ticker]={}
                self.headlines_dict[story_id][ticker]['stock_number']=stock_number
                self.headlines_dict[story_id][ticker]['distributor']=distributor
                self.headlines_dict[story_id][ticker]['date']=candle_date
                self.headlines_dict[story_id][ticker]['headline']= title


    def prepare_headline_df(self,headlines):
        
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
    
    
        

    def process_regional_quote(self, quote: np.array) -> None:
        print("%s: Regional Quote:" % self._name)
        print(quote)

    def process_summary(self, summary: np.array) -> None:
        '''
        This is the second message sent by iqfeed after requesting to watch
        a ticker
        
        IQFeed.exe will send a summary message for the symbol followed
        by an update message every time there is a trade.
        '''
        #print("%s: Data Summary" % self._name)
        #print(summary)
        pass


    def process_update(self, update: np.array) -> None:
        '''
        IQFeed.exe will send a summary message for the symbol followed
        by an update message every time there is a trade.
        
        This function receive the tick in the form of a numpy array
        '''
        
        #print("%s: Data Update" % self._name)
        #print(update)
        
        
        time_in_microseconds = math.floor(update[0][3]/60000000)
        time_in_minutes = datetime.timedelta(minutes= time_in_microseconds)  
        candle_date = self.today_date + time_in_minutes
        
        if candle_date > self.first_candle and candle_date.hour<16:
            
            self.compute_tick(update, candle_date)
            
        elif candle_date==self.first_candle:
            
            self.compute_tick(update, candle_date)
            
            ticker = str(update[0][0])
            ticker = ticker[2:-1]
            self.fill_overnight_tick(ticker)
            
        else:
            if candle_date.hour>=16:
                if self.market_open == True:
                    print('market are closed')
                    self.market_open = False
               

    def process_fundamentals(self, fund: np.array) -> None:
        
        '''
        This is the first message sent by iqfeed after requesting to watch
        a ticker
        '''
        
        #print("%s: Fundamentals Received:" % self._name)
        #print(fund)
        pass

    def process_auth_key(self, key: str) -> None:
        print("%s: Authorization Key Received: %s" % (self._name, key))

    def process_keyok(self) -> None:
        print("%s: Authorization Key OK" % self._name)

    def process_customer_info(self,
                              cust_info: QuoteConn.CustomerInfoMsg) -> None:
        print("%s: Customer Information:" % self._name)
        print(cust_info)

    def process_watched_symbols(self, symbols: Sequence[str]):
        print("%s: List of subscribed symbols:" % self._name)
        print(symbols)

    def process_log_levels(self, levels: Sequence[str]) -> None:
        print("%s: Active Log levels:" % self._name)
        print(levels)

    def process_symbol_limit_reached(self, sym: str) -> None:
        print("%s: Symbol Limit Reached with subscription to %s" %
              (self._name, sym))

    def process_ip_addresses_used(self, ip: str) -> None:
        print("%s: IP Addresses Used: %s" % (self._name, ip))
        
    def process_timestamp(self, time_val: FeedConn.TimeStampMsg):
        #print("%s: Timestamp:" % self._name)
        #print(time_val)
        pass
        
    

    def compute_tick(self,update,candle_date):
        
        ticker = str(update[0][0])
        length = len(ticker)-3
        ticker = ticker[2:(2+length)]
        price = update[0][1]
        volume = update[0][2]
        
        if ticker in self.price_dict[candle_date]:
            candle_open = self.price_dict[candle_date][ticker]['open']
        else:
            print('ticker: {} not in price_dict'.format(ticker))
            if len(ticker)==4:
                print(ticker)
                print(ticker[3])
            elif len(ticker==5):
                fifth_letter =['F','Q','A','K']
                for letter in fifth_letter:
                    temp_ticker = ticker+letter
                    if temp_ticker in self.price_dict[candle_date]:
                        ticker = temp_ticker
                        candle_open = self.price_dict[candle_date][ticker]['open']
                        break
            
        
        if math.isnan(candle_open):
            self.price_dict[candle_date][ticker]['open']=price
            self.price_dict[candle_date][ticker]['close']=price
            self.price_dict[candle_date][ticker]['high'] = price
            self.price_dict[candle_date][ticker]['low'] = price
            self.price_dict[candle_date][ticker]['average'] = price
            self.price_dict[candle_date][ticker]['barCount'] = 1
            self.price_dict[candle_date][ticker]['volume'] = volume
            
        else:
            high = self.price_dict[candle_date][ticker]['high']
            low = self.price_dict[candle_date][ticker]['low']
            barcount = self.price_dict[candle_date][ticker]['barCount']
            average = self.price_dict[candle_date][ticker]['average']
            
            if price > high:
                self.price_dict[candle_date][ticker]['high'] = price
            
            if price < low:
                self.price_dict[candle_date][ticker]['low'] = price
                
            self.price_dict[candle_date][ticker]['average'] = ((average*barcount)+price)/(barcount+1)
            self.price_dict[candle_date][ticker]['close']=price
            self.price_dict[candle_date][ticker]['volume'] = volume + self.price_dict[candle_date][ticker]['volume']
            
            self.price_dict[candle_date][ticker]['barCount'] = barcount+1
            
            
    def fill_overnight_candle(self):
        
        overnight_candle = self.overnight_candle
        first_candle = self.first_candle
    
        for ticker in self.watchlist:
            first_price= self.price_dict[first_candle][ticker]['open']
            
            if math.isnan(first_price):
                overnight_open = self.price_dict[overnight_candle][ticker]['open']
                self.price_dict[overnight_candle][ticker]['close']= overnight_open
                self.price_dict[overnight_candle][ticker]['high']= overnight_open
                self.price_dict[overnight_candle][ticker]['low']= overnight_open
                self.price_dict[overnight_candle][ticker]['average']= overnight_open
            else:
                overnight_open = self.price_dict[overnight_candle][ticker]['open']
                
                if overnight_open > first_price:
                    self.price_dict[overnight_candle][ticker]['high']= overnight_open
                    self.price_dict[overnight_candle][ticker]['low']= first_price
                else:
                    self.price_dict[overnight_candle][ticker]['high']= first_price
                    self.price_dict[overnight_candle][ticker]['low']= overnight_open
                
                self.price_dict[overnight_candle][ticker]['close']= first_price
                self.price_dict[overnight_candle][ticker]['average']= overnight_open 
                
                
    def fill_overnight_tick(self,ticker):
        overnight_candle = self.overnight_candle
        first_candle = self.first_candle
                
        if ticker not in self.price_dict[overnight_candle]:
            fifth_letter =['F','Q','A']
            for letter in fifth_letter:
                temp_ticker = ticker+letter
                if temp_ticker in self.price_dict[overnight_candle]:
                    ticker = temp_ticker
                    break
                
            
        first_price= self.price_dict[first_candle][ticker]['open']
        if math.isnan(first_price):
            pass

        else:
            overnight_open = self.price_dict[overnight_candle][ticker]['open']
            
            if overnight_open > first_price:
                self.price_dict[overnight_candle][ticker]['high']= overnight_open
                self.price_dict[overnight_candle][ticker]['low']= first_price
            else:
                self.price_dict[overnight_candle][ticker]['high']= first_price
                self.price_dict[overnight_candle][ticker]['low']= overnight_open
            
            self.price_dict[overnight_candle][ticker]['close']= first_price
            self.price_dict[overnight_candle][ticker]['average']= (overnight_open + first_price)/2
              
    
    
    def fill_inactive(self,candle_date):  
    
        for ticker in self.watchlist:
            barcount = self.price_dict[candle_date][ticker]['barCount']
            
            if barcount ==0:
                price = self.price_dict[self.previous_candle][ticker]['close']
                
                if math.isnan(price):
                    price = self.find_last_price(ticker)
                
                self.price_dict[candle_date][ticker]['open']=price
                self.price_dict[candle_date][ticker]['close']=price
                self.price_dict[candle_date][ticker]['high'] = price
                self.price_dict[candle_date][ticker]['low'] = price
                self.price_dict[candle_date][ticker]['average'] = price
                
    
    
    def re_init_features(self,candle_date):  
    
        for ticker in self.watchlist:
            last_updated_row = self.find_last_updated_row(ticker)
            self.price_dict[candle_date][ticker]= last_updated_row    
               
                
                
    def find_last_price(self,ticker):
        
        first = True
        for d in self.date_list:
            if d<self.previous_candle:
                if first:
                    first = False
                    last_price = self.price_dict[d][ticker]['close']
                else:
                    temp_price = self.price_dict[d][ticker]['close']
                    if math.isnan(temp_price) == False:
                        last_price = temp_price
                    
        return last_price
    
    
    def find_last_updated_row(self,ticker):
        
        first = True
        for d in self.date_list:
            if d<self.previous_candle:
                if first:
                    first = False
                    last_row = self.price_dict[d][ticker]
                else:
                    temp_rsi = self.price_dict[d][ticker]['RSI']
                    if math.isnan(temp_rsi) == False:
                        last_row = self.price_dict[d][ticker]
                    
        return last_row
    
    
    def find_previous_date(self,new_candle):
        for d in self.date_list:
            if d<new_candle:
                previous_date = d
            else:
                break
            
        return previous_date 
    
    
    def compute_feats(self,previous_date,live_date,approx,approx_ratio=1.0):
        '''
        Compute the features for all the tickers. The values are automatically saved in price_dict
        '''
        
        cm.compute_all_ticker(self.price_dict,self.watchlist,previous_date,live_date,self.bb_dict,self.vwap_dict,approx,approx_ratio)
        cm.compute_remaining_features(self.price_dict,self.watchlist,self.overnight_candle,live_date)
        
        #cm.compute_all_intraday_daily(self.price_dict,self.daily_dict,self.watchlist,live_date)
        
    def compute_ticker_features(self,ticker,previous_date,live_date,approx,approx_ratio=1.0):
        
        '''
        This function computes the features of only one ticker.
        '''
        bb_queue = self.bb_dict[ticker]
        price_q = self.vwap_dict[ticker]['price_q']
        volume_q = self.vwap_dict[ticker]['volume_q']
        last_row = self.price_dict[previous_date][ticker]
        live_row = self.price_dict[live_date][ticker]
        candle_size=1
        
        
        cm.compute_live_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size,approx,approx_ratio) 

        self.compute_green_line(ticker,live_date)
        
        
    def compute_green_line(self,ticker,live_date):
        
        '''
        This function compute the green_line of a single stock. 
        '''
        
        overnight_open = self.price_dict[self.overnight_candle][ticker]['open']
        current_close = self.price_dict[live_date][ticker]['close']
        green_line = current_close/overnight_open
        self.price_dict[live_date][ticker]['green_line']=green_line
        
        
        
    
    def get_daily_df(self):

        df_list = []    
        for ticker in self.daily_dict:
            df_list.append(self.daily_dict[ticker]['live_row'])
            
        df = pd.DataFrame(df_list)
        df['date']=self.live_candle
        df.set_index('date',inplace=True)  
        
        return df
    
    
    def get_live_df(self):
        live_date = self.live_candle
        df_dict = self.price_dict[live_date]
        
        df = pd.DataFrame.from_dict(df_dict,orient='index')
        df['date'] = live_date
        #df.reset_index(drop=True,inplace=True)
        df.set_index('date',inplace=True)
        
        return df
    
    
    def get_first_approximation_df(self,approx_ratio):
        
        approx=True
        for ticker in self.already_saved_overnight:
            self.compute_ticker_features(ticker,self.overnight_candle,self.first_candle,approx,approx_ratio)
            
        '''
        Need to recreate the dictionaries with the new ticker.
        '''
        df_dict = self.price_dict[self.first_candle]
        
        new_dict = {}
        for ticker in self.already_saved_overnight:
            new_dict[ticker]= df_dict[ticker]

        df = pd.DataFrame.from_dict(new_dict,orient='index')
        df['date'] = self.first_candle
        df.set_index('date',inplace=True)
        
        return df
    
    
    def get_specific_live_df(self,live_date):
        df_dict = self.price_dict[live_date]
        
        df = pd.DataFrame.from_dict(df_dict,orient='index')
        df['date'] = live_date
        df.set_index('date',inplace=True)
        
        return df
    
    
    def get_overnight_df(self):
        overnight_date = self.overnight_candle
        df_dict = self.price_dict[overnight_date]
        
        df = pd.DataFrame.from_dict(df_dict,orient='index')
        df['date'] = overnight_date
        df.set_index('date',inplace=True)
        
        return df
    
    
    def get_new_overnight_df(self):
        overnight_date = self.overnight_candle
        df_dict = self.price_dict[overnight_date]
        
        new_overnight = self.find_unsaved_overnight(df_dict)
        
        '''
        Need to compute the features of the new list right here. 
        '''
        
        approx=False
        for ticker in new_overnight:
            self.compute_ticker_features(ticker,self.last_candle,self.overnight_candle,approx,approx_ratio=1.0)
            
        '''
        Need to recreate the dictionaries with the new ticker.
        '''
        new_dict = {}
        for ticker in new_overnight:
            new_dict[ticker]= df_dict[ticker]

        df = pd.DataFrame.from_dict(new_dict,orient='index')
        df['date'] = overnight_date
        df.set_index('date',inplace=True)
        
        return df
        
    def find_unsaved_overnight(self,df_dict):
        
        new_overnight = []
        
        for ticker in df_dict:
            close_price = df_dict[ticker]['close']
            if math.isnan(close_price):
                 pass
            else:
                 if ticker in self.already_saved_overnight:
                     pass
                 else:
                     new_overnight.append(ticker)
                     self.already_saved_overnight.append(ticker)
                     
        return new_overnight
                
                




















# end


