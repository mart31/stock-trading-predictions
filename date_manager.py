
"""
Created on Tue Aug 31 08:58:11 2021

@author: marti
"""


import datetime
import pandas_market_calendars as mcal
import math
import time as t
import pandas as pd
import features_data
from datetime import timedelta  

import numpy as np


"""
This module provides functions to handle the dates. 
"""

class date_handler():
    
    def __init__(self):
        self.temp = 0
    
    
    def get_market_calendar(self,keep_2021=False,num_days=9999,db_name='american_database'):
        
        '''
        This function provided the market calendar for the last 179 days. 
        '''
        
        if db_name=='otc_database':
            today = datetime.datetime.today()
            hour = today.hour
            today = datetime.datetime.combine(datetime.date.today(),datetime.datetime.min.time()) 
           
            
            nyse = mcal.get_calendar('NYSE')
            
            if keep_2021:
                calendar = nyse.schedule(start_date='2021-01-01',end_date=today.date())
            else:
                date_179_days = today - datetime.timedelta(days = 179)
                calendar = nyse.schedule(start_date=date_179_days,end_date=today.date())
            
            calendar['open'] = 0
            calendar['close']=390
            calendar.drop(['market_open','market_close'],axis=1,inplace=True)
            
            if hour<16:
                dow = today.weekday()
                # dont want to drop today if
                if dow<5:
                    if today in calendar.index:
                        calendar.drop(today,inplace=True)
                    
            
            '''
            These holidays close earlier
            '''
            h1 = datetime.datetime.strptime('2021-11-26 00:00:00', '%Y-%m-%d %H:%M:%S')
            #h2 = datetime.datetime.strptime('2021-12-24 00:00:00', '%Y-%m-%d %H:%M:%S')
            h3 = datetime.datetime.strptime('2022-11-25 00:00:00', '%Y-%m-%d %H:%M:%S')
            holidays = [h1,h3]
            
            
            for h in holidays:
                if h in calendar.index:
                    calendar.loc[h,'close']=210
                    
            if num_days != 9999:
                calendar = calendar[-num_days:]
                
                    
            return calendar
        elif db_name=='american_database':
            calendar = self.get_exchange_calendar('AMERICAN',keep_2021,num_days)
            return calendar
            
    
    
    def get_last_market_date(self):
        calendar = self.get_market_calendar()
        return calendar[-1:].index[0]
    
    
    def get_intraday_date_list(self,keep_2021=False,candle_size=1,db_name='american_database',num_days=400): 
    
        if candle_size ==1:
            freq='1min'
        elif candle_size==30:
            freq = '30min'
        elif candle_size==390:
            freq=1
        else:
            raise Exception('wrong candle size')
            
        
        calendar = self.get_market_calendar(keep_2021,num_days=num_days,db_name=db_name)
        
        if candle_size !=390:
            d = {'date':0,'open':0,'high':0,'low':0,'close':0,'volume':0,'barCount':0
                 ,'average':0,'ticker':0}
            ser = pd.Series(data=d) 
               
            df_list = []
            for index,row in calendar.iterrows():
                period = row['close']/candle_size
                date_string = str(index.date())
                date_overnight = date_string +' 08:00:00'
                date_string = date_string+ ' 09:30'
                
                df = pd.DataFrame(columns=['date','open','high','low','close','volume','barCount','average','ticker'])
                df['date'] = pd.date_range(date_string, periods=period, freq=freq)
                df.set_index('date', inplace = True)
                
                overnight_time = datetime.datetime.strptime(date_overnight, '%Y-%m-%d %H:%M:%S')
                df.loc[overnight_time] = ser
            
                df_list.append(df)
            
            df = pd.concat(df_list)
            df.sort_index(inplace=True)
            
            df.drop(df.index[0],inplace=True)
            
            calendar_list = []
            
            for index,row in df.iterrows():
                calendar_list.append(index)
        
        else:
            calendar_list = []
            for index,row in calendar.iterrows():
                market_date = index.replace(hour=9,minute=30)
                calendar_list.append(market_date)
        
        return calendar_list
    
    
    def get_calendar_dict(self,keep_2021 = False):
        
        '''
        Return a dictionary containing the last 179 days of minute candle as key.
        '''
        calendar_list = self.get_intraday_date_list(keep_2021)
        
        features_object = features_data.features_data(candle_size=1)
        col_list = features_object.features
        col_list.remove('date')
        calendar_dict = {}
        
        for d in calendar_list:
            calendar_dict[d] = {}
            
            for col in col_list:
                calendar_dict[d][col] = float('NaN')
                
            calendar_dict[d]['barCount'] = 0
            calendar_dict[d]['volume'] = 0
            
        return calendar_dict
    
    
    
    
    def get_date_list(self,days,candle_size=1): 
    
        if days ==0:
            raise Exception('There is less than 1 market day between the last update and now')
    
        if days <= 180:
            if candle_size ==1:
                freq='1min'
            else:
                raise Exception('wrong candle size')
                
                
            calendar = self.get_market_calendar(keep_2021=False)
            calendar = calendar[-days:]
            
            d = {'date':0,'open':0,'high':0,'low':0,'close':0,'volume':0,'barCount':0
                 ,'average':0,'ticker':0}
            ser = pd.Series(data=d) 
               
            df_list = []
            for index,row in calendar.iterrows():
                period = row['close']/candle_size
                date_string = str(index.date())
                date_overnight = date_string +' 08:00:00'
                date_string = date_string+ ' 09:30'
                
                df = pd.DataFrame(columns=['date','open','high','low','close','volume','barCount','average','ticker'])
                df['date'] = pd.date_range(date_string, periods=period, freq=freq)
                df.set_index('date', inplace = True)
                
                overnight_time = datetime.datetime.strptime(date_overnight, '%Y-%m-%d %H:%M:%S')
                df.loc[overnight_time] = ser
            
                df_list.append(df)
            
            
            df = pd.concat(df_list)
            df.sort_index(inplace=True)
            
            calendar_list = []
            
            for index,row in df.iterrows():
                calendar_list.append(index)
            
            
            return calendar_list
        
        else:
            raise Exception('too many days was requested')
            
            
    def get_otc_date_list(self,last_update,last_candle,candle_size=1): 
    
        freq='1min'
        
        calendar = self.get_market_calendar(keep_2021=True)
        
        if isinstance(last_update,int):
            print('initializing the otc data')
            calendar_list = []
        else:
            calendar_list = []
            calendar_list.append(last_candle.to_pydatetime())
            calendar = calendar[last_update:]
            calendar = calendar[1:]
        
        d = {'date':0,'ema13_cash':0,'d1_ema13_cash':0,'ema13_barcount':0,'d1_ema13_barcount':0,'vwap_avg':0,'green_line_avg':0}
        ser = pd.Series(data=d) 
           
        df_list = []
        for index,row in calendar.iterrows():
            period = row['close']/candle_size
            date_string = str(index.date())
            date_overnight = date_string +' 08:00:00'
            date_string = date_string+ ' 09:30'
            
            df = pd.DataFrame(columns=['date','ema13_cash','d1_ema13_cash','ema13_barcount','d1_ema13_barcount',
                                       'vwap_avg','green_line_avg'])
            df['date'] = pd.date_range(date_string, periods=period, freq=freq)
            df.set_index('date', inplace = True)
            
            overnight_time = datetime.datetime.strptime(date_overnight, '%Y-%m-%d %H:%M:%S')
            df.loc[overnight_time] = ser
        
            df_list.append(df)
        
        calendar_list = []
        if len(df_list)>0:
            df = pd.concat(df_list)
            df.sort_index(inplace=True)
            
            for index,row in df.iterrows():
                candle_date = index.to_pydatetime()
                calendar_list.append(candle_date)
        
        return calendar_list
    
    
    def get_specific_date_list(self,date_morning,add_overnight=True): 
    
        freq='1min'
          
        period = 390
        date_string = str(date_morning.date())
        date_overnight = date_string +' 08:00:00'
        date_string = date_string+ ' 09:30'
        
        df = pd.DataFrame(columns=['date','ph'])
        df['date'] = pd.date_range(date_string, periods=period, freq=freq)
        df.set_index('date', inplace = True)
        
        if add_overnight:
            overnight_time = datetime.datetime.strptime(date_overnight, '%Y-%m-%d %H:%M:%S')
            df.loc[overnight_time] = 0
    
        calendar_list = []
        if len(df)>0:
            df.sort_index(inplace=True)
            
            for index,row in df.iterrows():
                candle_date = index.to_pydatetime()
                calendar_list.append(candle_date)
        
        return calendar_list
    
    

    
    def get_candle_dict(self,days,remove_last_5=False):
        
        '''
        Return a dictionary containing the X days of minute candle as key.
        '''
        
        calendar_list = self.get_date_list(days)
        
        if remove_last_5:
            calendar_list = calendar_list[:-5]
        
        features_object = features_data.features_data(candle_size=1)
        col_list = features_object.features
        col_list.remove('date')
        calendar_dict = {}
        
        for d in calendar_list:
            calendar_dict[d] = {}
            
            for col in col_list:
                calendar_dict[d][col] = float('NaN')
                
            calendar_dict[d]['barCount'] = 0
            calendar_dict[d]['volume'] = 0
            
        return calendar_dict



    def get_last_market_candle(self):
            market_date = self.get_last_market_date()
            market_date = market_date.replace(hour=15)
            market_date = market_date.replace(minute=59)
            last_market_candle = market_date.to_pydatetime()
            
            return last_market_candle
        
        
        
    def live_price_date_list(self,candle_size=1,data_struct='df'): 
    
        if candle_size ==1:
            freq='1min'
           
        d = {'date':0,'open':float('NaN'),'high':float('NaN'),'low':float('NaN'),
             'close':float('NaN'),'volume':float('NaN'),'barCount':float('NaN')
             ,'average':float('NaN'),'ticker':float('NaN')}
        
        ser = pd.Series(data=d) 
           
        period = 390/candle_size
        date_string = str(datetime.date.today())
        #date_string = str(index.date())
        date_overnight = date_string +' 08:00:00'
        date_string = date_string+ ' 09:30'
        
        df = pd.DataFrame(columns=['date','open','high','low','close','volume','barCount','average','ticker'])
        df['date'] = pd.date_range(date_string, periods=period, freq=freq)
        df.set_index('date', inplace = True)
        
        overnight_time = datetime.datetime.strptime(date_overnight, '%Y-%m-%d %H:%M:%S')
        df.loc[overnight_time] = ser
        
        last_update_candle = self.get_last_market_candle()
        df.loc[last_update_candle] = ser
        
        df.sort_index(inplace=True) 
        
        if data_struct=='df':
        
            return df
        
        else:
            data_list = []
            
            for d,row in df.iterrows():
                data_list.append(d)
                
            return data_list       
        
        
        
    def get_today_overnight(self):
            
            today = datetime.datetime.today()
            overnight_candle = today.replace(hour=8,minute=0,second=0,microsecond=0)
            
            return overnight_candle   
    
            
    def get_today_first_candle(self):
            today = datetime.datetime.today()
            first_candle = today.replace(hour=9,minute=30,second=0,microsecond=0)
            
            return first_candle
        
        
    def get_today_morning(self):
        today = datetime.datetime.today()
        date_morning = today.replace(hour=0,minute=0,second=0,microsecond=0)
        
        return date_morning
    
        
    def create_date_list_from_dict(self,data,remove_oc=False):
    
        key_list = list(data.keys())
        date_list = sorted(key_list)
        
        if remove_oc:
            index = 0
            for d in date_list:
                if type(d)==str:
                    date = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
                else:
                    date = d
                    
                if date.hour ==8:
                    del date_list[index]
                    
                index = index+1
        
        return date_list   
        
        
   
    def split_cv_date(self,date_list,n = 4):

        '''
        This functions split the date index day by day. Such that no two date from
        the same day are at the same time in the testing and training set. 
        '''   
        
        
        cv_index = 1
        
        cv_dict = {}
        first=True
        for d in date_list:
            if first:
                first=False
                previous_day = d.day
                
            if d.day != previous_day:
                #print(d)
                
                cv_index = cv_index +1
                if cv_index > n:
                    cv_index=1
                
                previous_day = d.day
                
                
            cv_dict[d]=cv_index
                
        return cv_dict      
   
    
    def split_last_x_day(self,db_name,date_list,num_days=16):
        
        market_calendar = self.get_market_calendar(db_name)
        cal_list = market_calendar[-num_days:].index.to_list()
        
        cal_dict = {}
        for d in cal_list:
            cal_dict[d]=True
            
        split_dict = {}
        count = 0
        for d in date_list:
            
            test_d = d.replace(hour=0,minute=0)
            
            if test_d in cal_dict:
                split_dict[d]=1
                count = count+1
            else:
                split_dict[d]=0
        
        print('{} days of minutes candle have been put aside for testing'.format(count)) 
        return split_dict
         
    def create_datetime(self,str_date):
        dt = datetime.datetime.strptime(str_date, '%Y-%m-%d %H:%M:%S')
        return dt
        
    
    def find_previous_morning(self):
        cal = self.get_market_calendar(True)
        previous_morning = cal.index[-1].to_pydatetime()
        
        return previous_morning
        


    def slice_date_list(self,date_list,start_date,end_date):
        sliced_list = []
        
        for d in date_list:
            if d>=start_date and d<=end_date:
                sliced_list.append(d)
                
        sliced_list = sorted(sliced_list)
        
        return sliced_list
    
    
    
    def split_intraday_data(self,date_list):

        intraday_list = []
        intraday = []
        first=True
        for d in date_list:
            if type(d)==str:
                date = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
            else:
                date = d
            
            if date.hour==9 and date.minute==30:
                if first:
                    first=False
                else:
                    intraday_list.append(intraday)
                intraday=[d]
            else:
                intraday.append(d)
                
        intraday_list.append(intraday)
                
        return intraday_list
        


    def create_custom_date_list(self,d_list,add_overnight=True):
        '''
        d_list: is a list of daily date which are used to create the datelist.
        '''
        
        freq='1min'
        d = {'date':0,'open':0,'high':0,'low':0,'close':0,'volume':0,'barCount':0
                         ,'average':0,'ticker':0}
        ser = pd.Series(data=d) 
        df_list = []
        for d in d_list:
            period = 390
            date_string = str(d.date())
            date_overnight = date_string +' 08:00:00'
            date_string = date_string+ ' 09:30'
            
            df = pd.DataFrame(columns=['date','open','high','low','close','volume','barCount','average','ticker'])
                       
            df['date'] = pd.date_range(date_string, periods=period, freq=freq)
            df.set_index('date', inplace = True)
            
            if add_overnight:
                overnight_time = datetime.datetime.strptime(date_overnight, '%Y-%m-%d %H:%M:%S')
                df.loc[overnight_time] = ser
        
            df_list.append(df)
        
        
        df = pd.concat(df_list)
        df.sort_index(inplace=True)
        
        date_list = sorted(df.index.to_list())
        
        return date_list
    
    
    def this_week_date_list(self):
        #dh  = date_manager.date_handler()
        calendar = self.get_market_calendar(num_days=5)
        calendar.sort_index(ascending=False,inplace=True)
        
        #Monday == 0 … Sunday == 6
        date_list = []
        first=True
        for index,row in calendar.iterrows():
            wd = index.weekday()
            
            if first:
                first=False
                previous_wd = wd
           
            if wd>previous_wd:
                break
            else:
                date_list.append(index)
            
            previous_wd = wd
            
        
        #date_list.append(index)
        sorted_date_list = sorted(date_list)   
        
        return sorted_date_list
    
    
    def remove_1_minute(self,d):

        new_d = d-timedelta(minutes=1)
        return new_d   


    def get_exchange_calendar(self,name,keep_2021=False,num_days=9999):
        
        '''
        This function provided the market calendar for the last 179 days. 
        '''
        
        today = datetime.datetime.today()
        hour = today.hour
        today = datetime.datetime.combine(datetime.date.today(),datetime.datetime.min.time()) 
       
        if name =='NYSE':
            market_cal = mcal.get_calendar('NYSE')
        elif name=='NASDAQ':
            market_cal = mcal.get_calendar('NASDAQ')
        elif name=='AMERICAN':
            market_cal = mcal.get_calendar('NYSE')
        
        
        if keep_2021:
            calendar = market_cal.schedule(start_date='2021-01-01',end_date=today.date())
        else:
            date_179_days = today - datetime.timedelta(days = 179)
            calendar = market_cal.schedule(start_date=date_179_days,end_date=today.date())
        
        calendar['open'] = 0
        calendar['close']=390
        calendar.drop(['market_open','market_close'],axis=1,inplace=True)
        
        if hour<16:
            dow = today.weekday()
            # dont want to drop today if
            if dow<5:
                if today in calendar.index:
                    calendar.drop(today,inplace=True)
                
                
        if num_days != 9999:
            calendar = calendar[-num_days:]
            
                
        return calendar

    
        
        

        
        
        
        
        
        
        
# end        


