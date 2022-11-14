# -*- coding: utf-8 -*-
"""
Created on Sun Mar 13 12:29:27 2022

@author: marti
"""


import sqlite3
import datetime
import pandas as pd
import queue
import time as t
import math
import numpy as np
import date_manager


class db_manager():
    
    '''
    The db manager is used to access a specific database. As all database share
    similar data the function are re used.
    '''
    
    def __init__(self,db_name,data_shape):
        
        '''
        The data shape inform us on the structure of the data to be downloaded.
        '''
        self.data_shape = data_shape 
        self.db_name = db_name
        
        self.dh = date_manager.date_handler()
        
        #the stock info never change. 
        self.stock_info_col = ['ticker','name','exchange','sec_type','common_share','institutionalPercent','shortInterest']
        
     
    def get_1m_training_features(self):
        if self.db_name=='otc_database':
             
            features = ['gains','gains3','gains5','gains10','gains20','RSI','d1_ema3_RSI','d2_ema3_RSI','normalize_bb','d1_ema3_bb','d2_ema3_bb',
                        'PPO_line','d1_ema3_PPO_line','d2_ema3_PPO_line','ema13_cash','d1_ema13_cash','d2_ema13_cash','candle_shape',
                        'candle_range','ema13_range','d1_ema13_range','d2_ema13_range','hl_range20','overnight_candle','vwap_ratio',
                        'no_trade_count','green_line'] 
                 
        elif self.db_name=='american_database':
            features = ['gains','gains3','gains5','gains10','gains20','RSI','d1_ema3_RSI','d2_ema3_RSI','normalize_bb','d1_ema3_bb','d2_ema3_bb',
                        'PPO_line','d1_ema3_PPO_line','d2_ema3_PPO_line','ema13_cash','d1_ema13_cash','d2_ema13_cash','candle_shape',
                        'candle_range','ema13_range','d1_ema13_range','d2_ema13_range','hl_range20','overnight_candle','vwap_ratio',
                        'no_trade_count','green_line'] 
        else:
            raise Exception('Training features not implemented')
          
        return features    
          
            
    def get_390m_training_features(self):
        features = ['average','gains','RSI','d1_ema3_RSI','d2_ema3_RSI','normalize_bb','d1_ema3_bb','d2_ema3_bb',
                            'PPO_line','d1_ema3_PPO_line','d2_ema3_PPO_line','PPO_histo','d1_ema3_PPO_histo',
                            'd2_ema3_PPO_histo','ema13_cash','d1_ema13_cash','d1_ema13_barcount','d2_ema13_barcount','barcount_ratio',
                            'candle_shape','candle_range','ema13_range','d1_ema13_range','d2_ema13_range',
                            'friday','vwap_ratio','ema13_oc_gains','d1_ema13_oc_gains','oc_gains'] 
        
        return features
        
        
    def get_features_col(self,table):
        if table=='FEATURES_1M':
            return self.data_shape.features
        elif table=='FEATURES_390M':
            return self.data_shape.features_390m
        elif table=='APPROX_390':
            return self.data_shape.features_390m
        elif table=='INTRADAY_DAILY':
            return self.data_shape.intraday_daily
        elif table=='TRADES':
            return self.data_shape.trade_col
        elif table=='DEPTH_SUMMARY':
            return self.data_shape.depth_col
        elif table=='STOCK':
            return self.data_shape.stock_info_col
        else:
            if len(table)>0:
                if table[0]=='A':
                    '''
                    We assume the requested table is one of the live approximation.
                    '''
                    return self.data_shape.features
            else:
                return []
            
            
    def get_inner_col(self,table_a,table_b):
        '''
        This function combines the column and change the duplicated
        names. 
        '''
        
        col_a = self.get_features_col(table_a)
        col_b = self.get_features_col(table_b)
        
        col_list = []
        
        for col in col_a:
            col_list.append(col)
        
        for col in col_b:
            if col in col_a:
                new_col = table_b+'_'+col
            else:
                new_col = col
                
            col_list.append(new_col)
            
            
        return col_list
        
        
    def get_news_headlines_col(self):
        '''
        The news headline are unlikely to change shape.
        '''
        
        col = ['story_id','ticker','stock_number','distributor','date','headline']
        return col
    
    
    def get_news_story_col(self):
        '''
        The news story are unlikely to change shape.
        '''
            
        col =['story_id','story']
        return col
                
    
    '''
    SAVING FUNCTIONS
    '''
                
                
    def save_ticker_data(self,df,table,ticker,check_duplicate=False):
        '''
        This function populates the feature data of the given ticker using the provided dataframe.
        The newly added features are also added at the end of this fonction. 
        
        The stock data_always contains the following features:
        ['date','open','high','low','close','volume','barCount','average','ticker']
        '''
        
        if df.empty:
            raise Exception('No data was provided')
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        
        df = df.sort_index()
        
        if check_duplicate:
            for index, row in df.iterrows():
            # verify if a given row is already present in the database.
            
                date = index.to_pydatetime()
                    
                sql_request = 'SELECT * FROM {} WHERE ticker =? AND date=?'.format(table)
                cur.execute(sql_request,(ticker,date))
                    
                #cur.execute("SELECT * FROM INTRADAY_PRICE WHERE ticker=? AND date=?",(row['ticker'],date))
                temp = cur.fetchall()
                if len(temp)>=1:
                    df.drop([index],inplace = True)
        
        # Store the new data row in the database.
        if len(df)>0:
            
            df.to_sql(name=table, con=con, if_exists='append')
            con.commit()
                
        print('Added '+str(len(df))+' '+ table+ ' data from '+ticker+' to the '+self.db_name)
    
        cur.close()
        con.close()
        
        
        
    def save_multiple_ticker(self,df,table):
        '''
        This function save a batch of data from the same datetime but having different
        tickers. This function does not check for duplicate. 
        
        Used to fill up the simulation and live database. 
        '''
        
        if df.empty:
            raise Exception('No data was provided')
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        if len(df)>0:
            
            df.to_sql(name=table, con=con, if_exists='append')
            con.commit() 
            
        print('Added '+str(len(df))+' '+ table +' data to the {}'.format(self.db_name))
        
        cur.close()
        con.close() 
        
        
    def update_approx_table(self,update_df,table):       
            
        if update_df.empty:
            raise Exception('No data was provided')
            
        #for now only use for approximating the 1 minutes live features.
        assert table=='APPROX_1M'
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        if len(update_df)>0:
            
            update_df.to_sql(name=table, con=con, if_exists='replace')
            con.commit() 
            
        today = datetime.datetime.today()
        second = int(today.second)
        print('Updated '+str(len(update_df))+' '+ table +' data to the {} {} second'.format(self.db_name,second))
        
        cur.close()
        con.close() 
        
        
        
    def save_headlines(self,df):
        
        '''
        Because multiple ticker can be associated with the same story_id i set 
        the index only at the very end. 
        '''
        
        if df.empty:
            raise Exception('No data was provided')
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        table='HEADLINES'
        
        for index, row in df.iterrows():
        # verify if a given row is already present in the database.
            ticker = row['ticker'] 
            story_id = row['story_id']
            sql_request = 'SELECT * FROM {} WHERE ticker=? AND story_id=?'.format(table)
            cur.execute(sql_request,(ticker,story_id))
                
            #cur.execute("SELECT * FROM INTRADAY_PRICE WHERE ticker=? AND date=?",(row['ticker'],date))
            temp = cur.fetchall()
            if len(temp)>=1:
                df.drop([index],inplace = True)
        
        # Store the new data row in the database.
        if len(df)>0:
            ticker_list = df['ticker'].unique()
            length = len(df)
            
            df.set_index('story_id',inplace=True)
            df.to_sql(name=table, con=con, if_exists='append')
            con.commit()
               
            print('Saved {} headlines from {} stocks'.format(length,len(ticker_list)))
        else:
            print('All the headlines were already saved')
    
        
        cur.close()
        con.close() 
        
        
    def save_a_story(self,df):
        
        '''
        Because multiple ticker can be associated with the same story_id i set 
        the index only at the very end. 
        '''
        
        if df.empty:
            raise Exception('No data was provided')
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        table='STORY'
        
        for story_id, row in df.iterrows():
            sql_request = 'SELECT * FROM {} WHERE story_id=?'.format(table)
            cur.execute(sql_request,(story_id,))
                
            temp = cur.fetchall()
            if len(temp)>=1:
                df.drop([story_id],inplace = True)
        
        # Store the new data row in the database.
        if len(df)>0:
           
            
            df.to_sql(name=table, con=con, if_exists='append')
            con.commit()
               
            print('Saved {} story'.format(len(df)))
        else:
            print('This story was already in the database')
    
        
        cur.close()
        con.close() 
        
        
    def add_stock_info(self,df_row,ticker,table='STOCK'):

        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        df = pd.DataFrame(columns = self.stock_info_col)    
            
        data_row = [ticker]
        for col in self.stock_info_col:
            if col !='ticker':
                value = df_row[col]
                data_row.append(value)
                        
        df.loc[ticker] = data_row
        df.set_index('ticker',inplace=True)
        df.index.name='ticker'
        
            
        sql_request = 'SELECT * FROM {} WHERE ticker=?'.format(table)
        
        cur.execute(sql_request,(ticker,))
        temp = cur.fetchall()
        #print(temp)
        if len(temp)>=1:
            print(ticker+' was already in our database') 
        else:
            df.to_sql(name=table, con=con, if_exists='append')
            print('Added '+ticker+' to our database')
    
        cur.close()
        con.close()
        
        
    '''
    DOWNLOADING FUNCTIONS
    
    Note, the primary key for the features is the date and ticker respectively.
    Therefore, the search is always faster when using the date is used first. 
    '''   
    
    def download_ticker(self,ticker,table,date_list,cond_list=[],data_struct='df'):
        '''
        Download the available features data for the given ticker. 
        For the time being there are only two candle size available. 1 minute and 390 minute (daily)
        
        ticker: the ticker being downloaded
        table: the specific table from the database
        date_list: a list of datetime which will be used to find the specific data
        cond_list: List of extra condition for the request.
        
        Here is an example of a cond list:
            
        a = ['ema13_cash','>=','10000']
        b = ['average_cash','>=','10000']
        c = ['close','>=','0.002']
        cond_list = [a,b,c]
        
        Note, every value in the cond_list is a string. 
        '''
        
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        sql_request = self.create_ticker_sql_request(table,cond_list)
        
        col = self.get_features_col(table)
        
       
        
        data_list = []
        for d in date_list:
            #print(d)
            
            date_morning = d.to_pydatetime()
            date_afternoon = date_morning + datetime.timedelta(hours = 23)
        
            request_tuple = (ticker,date_morning,date_afternoon)
            
            cur.execute(sql_request,request_tuple)
            data = cur.fetchall()
            
            if len(data)>0 and data_struct=='df':
                df = pd.DataFrame(data,columns=col)
                data_list.append(df)
            else:
                for row in data:
                    data_list.append(row)
                    
        cur.close()
        con.close()
        
        '''
        Creating the data structure
        '''
    
        
        if len(data_list)>0:
            if data_struct=='df':
                combined_df = pd.DataFrame(columns=col)
                combined_df = pd.concat(data_list)
               
                combined_df.set_index('date',inplace=True)
                combined_df = combined_df.sort_index()
                combined_df.index = pd.to_datetime(combined_df.index)
                
                return combined_df
            elif data_struct=='list':
                return data_list
            
            elif data_struct=='dict':
                data_dict = {}
                col_list = self.get_features_col(table)
                for row in data_list:
                    for x in range(len(col_list)):
                        if x==0:
                            data_dict[row[0]]={}
                        else:
                            col = col_list[x]
                            data_dict[row[0]][col] = row[x]
                            
                return data_dict
        else:
            return data_list
        
        
    
    def download_day_to_day_ticker_list(self,daily_ticker_dict,table,date_list,cond_list=[],data_struct='stock_dict',print_date=True):
        '''
        Download the available features data for the given ticker. 
        For the time being there are only two candle size available. 1 minute and 390 minute (daily)
        
        ticker: the ticker being downloaded
        table: the specific table from the database
        date_list: a list of datetime which will be used to find the specific data
        cond_list: List of extra condition for the request.
        
        Here is an example of a cond list:
            
        a = ['ema13_cash','>=','10000']
        b = ['average_cash','>=','10000']
        c = ['close','>=','0.002']
        cond_list = [a,b,c]
        
        Note, every value in the cond_list is a string. 
        '''
        
        if len(cond_list)==0:
            a = ['ema13_cash','>=','1000']
            b = ['average_cash','>=','1000']
            c = ['close','>=','0.002']
            d = ['hl_ratio20','<=','3.0']
            cond_list = [a,b,c,d]
        
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        col = self.get_features_col(table)
        
        data_list = []
        stock_dict = {}
        for date_morning in date_list:
            temps = t.time()
            
            ticker_list = daily_ticker_dict[date_morning]
            sql_request = self.create_ticker_list_sql_request(ticker_list,table,cond_list)
        
            #date_morning = d.to_pydatetime()
            #date_afternoon = date_morning + datetime.timedelta(hours = 23)
            date_afternoon = date_morning.replace(hour=15,minute=40)
        
            request_tuple = (date_morning,date_afternoon)
            
            cur.execute(sql_request,request_tuple)
            data = cur.fetchall()
            
            if len(data)>0 and data_struct=='df':
                df = pd.DataFrame(data,columns=col)
                data_list.append(df)
            elif len(data)>0 and data_struct=='stock_dict':
                df = pd.DataFrame(data,columns=col)
                if len(df)>0:
                   df.dropna(inplace=True)
                if len(df)>0:
                    stock_dict[date_morning]=df
            else:
                for row in data:
                    data_list.append(row)
                    
            temps = t.time()-temps
            
            if print_date:
                print('download the ticker list from {} tooks {} seconds'.format(date_morning,temps))
                    
        cur.close()
        con.close()
        
        '''
        Creating the data structure
        '''
        
        if len(data_list)>0 or len(stock_dict)>0:
            if data_struct=='df':
                combined_df = pd.DataFrame(columns=col)
                combined_df = pd.concat(data_list)
                
                combined_df.set_index('date',inplace=True)
                combined_df = combined_df.sort_index()
                combined_df.index = pd.to_datetime(combined_df.index)
                
                return combined_df
            elif data_struct=='list':
                return data_list
            elif data_struct=='stock_dict':
                return stock_dict
            elif data_struct=='dict':
                data_dict = {}
                col_list = self.get_features_col(table)
                for row in data_list:
                    for x in range(len(col_list)):
                        if x==0:
                            data_dict[row[0]]={}
                        else:
                            col = col_list[x]
                            data_dict[row[0]][col] = row[x]
                            
                return data_dict
        else:
            return data_list
        
        
    def download_delayed_day_to_day_ticker_list(self,daily_ticker_dict,table,date_list,cond_list=[],data_struct='stock_dict',print_date=True):
        '''
        Download the available features data for the given ticker. 
        For the time being there are only two candle size available. 1 minute and 390 minute (daily)
        
        ticker: the ticker being downloaded
        table: the specific table from the database
        date_list: a list of datetime which will be used to find the specific data
        cond_list: List of extra condition for the request.
        
        Here is an example of a cond list:
            
        a = ['ema13_cash','>=','10000']
        b = ['average_cash','>=','10000']
        c = ['close','>=','0.002']
        cond_list = [a,b,c]
        
        Note, every value in the cond_list is a string. 
        '''
        
        if len(cond_list)==0:
            a = ['ema13_cash','>=','1000']
            b = ['average_cash','>=','1000']
            c = ['close','>=','0.002']
            d = ['hl_ratio20','<=','3.0']
            cond_list = [a,b,c,d]
        
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        col = self.get_features_col(table)
        
        data_list = []
        stock_dict = {}
        first=True
        for date_morning in date_list:
            if first:
                first=False
                previous_morning = date_morning
            else:
                temps = t.time()
                
                ticker_list = daily_ticker_dict[previous_morning]
                sql_request = self.create_ticker_list_sql_request(ticker_list,table,cond_list)
            
                #date_morning = d.to_pydatetime()
                #date_afternoon = date_morning + datetime.timedelta(hours = 23)
                date_afternoon = date_morning.replace(hour=15,minute=40)
            
                request_tuple = (date_morning,date_afternoon)
                
                cur.execute(sql_request,request_tuple)
                data = cur.fetchall()
                
                if len(data)>0 and data_struct=='df':
                    df = pd.DataFrame(data,columns=col)
                    data_list.append(df)
                elif len(data)>0 and data_struct=='stock_dict':
                    df = pd.DataFrame(data,columns=col)
                    if len(df)>0:
                       df.dropna(inplace=True)
                    if len(df)>0:
                        stock_dict[date_morning]=df
                else:
                    for row in data:
                        data_list.append(row)
                        
                temps = t.time()-temps
                
                if print_date:
                    print('download the ticker list from {} tooks {} seconds'.format(date_morning,temps))
            
            previous_morning = date_morning
                    
        cur.close()
        con.close()
        
        '''
        Creating the data structure
        '''
        
        if len(data_list)>0 or len(stock_dict)>0:
            if data_struct=='df':
                combined_df = pd.DataFrame(columns=col)
                combined_df = pd.concat(data_list)
                
                combined_df.set_index('date',inplace=True)
                combined_df = combined_df.sort_index()
                combined_df.index = pd.to_datetime(combined_df.index)
                
                return combined_df
            elif data_struct=='list':
                return data_list
            elif data_struct=='stock_dict':
                return stock_dict
            elif data_struct=='dict':
                data_dict = {}
                col_list = self.get_features_col(table)
                for row in data_list:
                    for x in range(len(col_list)):
                        if x==0:
                            data_dict[row[0]]={}
                        else:
                            col = col_list[x]
                            data_dict[row[0]][col] = row[x]
                            
                return data_dict
        else:
            return data_list
        
        
    def download_all_ticker_list(self,ticker_list,table='FEATURES_390M',data_struct='df'):
    
        '''
        Download all the daily data 
        '''
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        ticker_tuple = tuple(ticker_list)
    
        sql_request = 'SELECT * FROM {} WHERE ticker IN {}'.format(table,ticker_tuple)
        cur.execute(sql_request,)
     
        data = cur.fetchall()
        
        cur.close()
        con.close()
        
        
        if data_struct=='df':
            col = self.get_features_col(table)
            df = pd.DataFrame(data,columns=col) 
            return df
        elif data_struct=='dict':
           
            data_dict = {}
            col_list = self.get_features_col(table)
            i = 0
            for row in data:
                data_dict[i]={}
                for x in range(len(col_list)):
                    col = col_list[x]
                    data_dict[i][col] = row[x]
                i = i+1
            return data_dict
        
        else:
            return data
    
        
        
        
    def download_ticker_list(self,ticker_list,table,date_list,cond_list=[],data_struct='df'):
        '''
        Download the available features data for the given ticker. 
        For the time being there are only two candle size available. 1 minute and 390 minute (daily)
        
        ticker: the ticker being downloaded
        table: the specific table from the database
        date_list: a list of datetime which will be used to find the specific data
        cond_list: List of extra condition for the request.
        
        Here is an example of a cond list:
            
        a = ['ema13_cash','>=','10000']
        b = ['average_cash','>=','10000']
        c = ['close','>=','0.002']
        cond_list = [a,b,c]
        
        Note, every value in the cond_list is a string. 
        '''
        
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        #sql_request = self.create_ticker_list_sql_request(table,cond_list)
        sql_request = self.create_ticker_list_sql_request(ticker_list,table,cond_list)
        
        col = self.get_features_col(table)
        
        data_list = []
        for d in date_list:
        
            date_morning = d.to_pydatetime()
            date_afternoon = date_morning + datetime.timedelta(hours = 23)
        
            request_tuple = (date_morning,date_afternoon)
            
            cur.execute(sql_request,request_tuple)
            data = cur.fetchall()
            
            if len(data)>0 and data_struct=='df':
                df = pd.DataFrame(data,columns=col)
                data_list.append(df)
            else:
                for row in data:
                    data_list.append(row)
                    
        cur.close()
        con.close()
        
        '''
        Creating the data structure
        '''
        
        if len(data_list)>0:
            if data_struct=='df':
                combined_df = pd.DataFrame(columns=col)
                combined_df = pd.concat(data_list)
                
                combined_df.set_index('date',inplace=True)
                combined_df = combined_df.sort_index()
                combined_df.index = pd.to_datetime(combined_df.index)
                
                return combined_df
            elif data_struct=='list':
                return data_list
            
            elif data_struct=='dict':
                data_dict = {}
                col_list = self.get_features_col(table)
                for row in data_list:
                    for x in range(len(col_list)):
                        if x==0:
                            data_dict[row[0]]={}
                        else:
                            col = col_list[x]
                            data_dict[row[0]][col] = row[x]
                            
                return data_dict
        else:
            return data_list
        
        
    def download_all_ticker_list(self,ticker_list,table='FEATURES_390M',data_struct='df'):
    
        '''
        Download all the daily data 
        '''
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        ticker_tuple = tuple(ticker_list)
    
        sql_request = 'SELECT * FROM {} WHERE ticker IN {}'.format(table,ticker_tuple)
        cur.execute(sql_request,)
     
        data = cur.fetchall()
        
        cur.close()
        con.close()
        
        
        if data_struct=='df':
            col = self.get_features_col(table)
            df = pd.DataFrame(data,columns=col) 
            return df
        elif data_struct=='dict':
           
            data_dict = {}
            col_list = self.get_features_col(table)
            i = 0
            for row in data:
                data_dict[i]={}
                for x in range(len(col_list)):
                    col = col_list[x]
                    data_dict[i][col] = row[x]
                i = i+1
            return data_dict
        
        else:
            return data
        
        
        
    def download_candle_ticker_list(self,ticker_list,table,date_candle,cond_list=[],data_struct='df'):
        '''
        Download the available features data for the given ticker. 
        For the time being there are only two candle size available. 1 minute and 390 minute (daily)
        
        ticker: the ticker being downloaded
        table: the specific table from the database
        date_list: a list of datetime which will be used to find the specific data
        cond_list: List of extra condition for the request.
        
        Here is an example of a cond list:
            
        a = ['ema13_cash','>=','10000']
        b = ['average_cash','>=','10000']
        c = ['close','>=','0.002']
        cond_list = [a,b,c]
        
        Note, every value in the cond_list is a string. 
        '''
        
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        #sql_request = self.create_ticker_list_sql_request(table,cond_list)
        sql_request = self.create_ticker_list_sql_request(ticker_list,table,cond_list)
        #print(sql_request)
            
        date_delta = date_candle + datetime.timedelta(seconds =30)
        request_tuple = (date_candle,date_delta)
        
        cur.execute(sql_request,request_tuple)
        data_list = cur.fetchall()
    
                    
        cur.close()
        con.close()
        
        '''
        Creating the data structure
        '''
        
        
        if len(data_list)>0:
            if data_struct=='df':
                col = self.get_features_col(table)
                df = pd.DataFrame(data_list,columns=col) 
                df.set_index('ticker',inplace=True)
                #df.drop(['date'],axis=1,inplace=True)
                return df
            elif data_struct=='list':
                return data_list
            
            elif data_struct=='dict':
                data_dict = {}
                col_list = self.get_features_col(table)
                for row in data_list:
                    ticker = row[8]
                    data_dict[ticker]={}
                    for x in range(len(col_list)):
                        col = col_list[x]
                        data_dict[ticker][col] = row[x]
                            
                return data_dict
        else:
            return data_list  
        
        
        
    def download_minute_candle(self,date_candle,table,cond_list=[],data_struct='df'):
        
        '''
        Download the available features data for the given ticker. 
        For the time being there are only two candle size available. 1 minute and 390 minute (daily)
        
        ticker: the ticker being downloaded
        table: the specific table from the database
        date_list: a list of datetime which will be used to find the specific data
        cond_list: List of extra condition for the request.
        
        Here is an example of a cond list:
            
        a = ['ema13_cash','>=','10000']
        b = ['average_cash','>=','10000']
        c = ['close','>=','0.002']
        cond_list = [a,b,c]
        
        Note, every value in the cond_list is a string. 
        '''
        
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        sql_request = self.create_candle_sql_request(table,cond_list)
        
        col = self.get_features_col(table)
        
        #date_candle = date_candle.to_pydatetime()
        date_delta = date_candle + datetime.timedelta(seconds =30)
        
        request_tuple = (date_candle,date_delta)
        
        cur.execute(sql_request,request_tuple)
        data_list = cur.fetchall()
                    
        cur.close()
        con.close()
        
        '''
        Creating the data structure
        '''
        
        if len(data_list)>0:
            if data_struct=='df':
                df = pd.DataFrame(data_list,columns=col) 
                df.set_index('ticker',inplace=True)
                #df.drop(['date'],axis=1,inplace=True)
                return df
            elif data_struct=='list':
                return data_list
            
            elif data_struct=='dict':
                data_dict = {}
                col_list = self.get_features_col(table)
                for row in data_list:
                    ticker = row[8]
                    data_dict[ticker]={}
                    for x in range(len(col_list)):
                        col = col_list[x]
                        data_dict[ticker][col] = row[x]
                            
                return data_dict
        else:
            return data_list
        
        
        
    def download_combined_specific_day(self,date_morning,table_a='FEATURES_1M',table_b='INTRADAY_DAILY',cond_list=[],data_struct='df'):
        '''
        This function was created specifically to join the FEATURES_1M
        and INTRADAY_DAILY table. 
        '''
        
        basic = 'SELECT * FROM {} INNER JOIN {} '.format(table_a,table_b) 
        first_on= 'ON FEATURES_1M.date = INTRADAY_DAILY.date '
        second_on = 'AND FEATURES_1M.ticker = INTRADAY_DAILY.ticker '
        basic_where = 'WHERE FEATURES_1M.date >=? AND FEATURES_1M.date <? '
        cond_statement= self.add_inner_cond_statement(table_a,cond_list)
        
        sql_request = basic+first_on+second_on+basic_where+cond_statement
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        
        date_delta = date_morning + datetime.timedelta(hours=23)
        
        request_tuple = (date_morning,date_delta)
        
        cur.execute(sql_request,request_tuple)
        data_list = cur.fetchall()
                    
        cur.close()
        con.close()
        
        col_list = self.get_inner_col(table_a,table_b)
        
        #data_struct='dict'
        if len(data_list)>0:
            if data_struct=='df':
                df = pd.DataFrame(data_list,columns=col_list) 
                #df.set_index('ticker',inplace=True)
                
                return df
            elif data_struct=='list':
                return data_list
            
            elif data_struct=='dict':
                data_dict = {}
                
                for row in data_list:
                    ticker = row[8]
                    data_dict[ticker]={}
                    for x in range(len(col_list)):
                        col = col_list[x]
                       
                        data_dict[ticker][col] = row[x]
                            
                return data_dict
        else:
            return data_list 
        
        
        
    def download_combined_ticker_specific_day(self,ticker,date_morning,table_a='FEATURES_1M',table_b='INTRADAY_DAILY',cond_list=[],data_struct='df'):
        '''
        This function was created specifically to join the FEATURES_1M
        and INTRADAY_DAILY table. 
        '''
        
        basic = 'SELECT * FROM {} INNER JOIN {} '.format(table_a,table_b) 
        first_on= 'ON FEATURES_1M.date = INTRADAY_DAILY.date '
        second_on = 'AND FEATURES_1M.ticker = INTRADAY_DAILY.ticker '
        basic_where = 'WHERE FEATURES_1M.date >=? AND FEATURES_1M.date <? AND FEATURES_1M.ticker =? '
        cond_statement= self.add_inner_cond_statement(table_a,cond_list)
        
        sql_request = basic+first_on+second_on+basic_where+cond_statement
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        
        date_delta = date_morning + datetime.timedelta(hours=23)
        
        request_tuple = (date_morning,date_delta,ticker)
        
        cur.execute(sql_request,request_tuple)
        data_list = cur.fetchall()
                    
        cur.close()
        con.close()
        
        col_list = self.get_inner_col(table_a,table_b)
        
        #data_struct='dict'
        if len(data_list)>0:
            if data_struct=='df':
                df = pd.DataFrame(data_list,columns=col_list) 
                #df.set_index('ticker',inplace=True)
                
                return df
            elif data_struct=='list':
                return data_list
            
            elif data_struct=='dict':
                data_dict = {}
                
                for row in data_list:
                    date_index = row[0]
                    data_dict[date_index]={}
                    for x in range(len(col_list)):
                        col = col_list[x]
                       
                        data_dict[date_index][col] = row[x]
                            
                return data_dict
        else:
            return data_list 
        
        
        
    def download_inner_join_candle(self,date_candle,table_a='FEATURES_1M',table_b='INTRADAY_DAILY',cond_list=[],data_struct='df'):
        '''
        This function was created specifically to join the FEATURES_1M
        and INTRADAY_DAILY table. 
        '''
        
        basic = 'SELECT * FROM {} INNER JOIN {} '.format(table_a,table_b) 
        first_on= 'ON FEATURES_1M.date = INTRADAY_DAILY.date '
        second_on = 'AND FEATURES_1M.ticker = INTRADAY_DAILY.ticker '
        basic_where = 'WHERE FEATURES_1M.date >=? AND FEATURES_1M.date <? '
        cond_statement= self.add_inner_cond_statement(table_a,cond_list)
        
        sql_request = basic+first_on+second_on+basic_where+cond_statement
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        
        date_delta = date_candle + datetime.timedelta(seconds =30)
        
        request_tuple = (date_candle,date_delta)
        
        cur.execute(sql_request,request_tuple)
        data_list = cur.fetchall()
                    
        cur.close()
        con.close()
        
        col_list = self.get_inner_col(table_a,table_b)
        
        #data_struct='dict'
        if len(data_list)>0:
            if data_struct=='df':
                df = pd.DataFrame(data_list,columns=col_list) 
                #df.set_index('ticker',inplace=True)
                
                return df
            elif data_struct=='list':
                return data_list
            
            elif data_struct=='dict':
                data_dict = {}
                
                for row in data_list:
                    ticker = row[8]
                    data_dict[ticker]={}
                    for x in range(len(col_list)):
                        col = col_list[x]
                       
                        data_dict[ticker][col] = row[x]
                            
                return data_dict
        else:
            return data_list 
        
        
    def download_inner_join_common_share(self,date_candle,table_a='FEATURES_390M',table_b='STOCK',cond_list=[],data_struct='df'):
        '''
        This function was created specifically to join the FEATURES_1M
        and INTRADAY_DAILY table. 
        '''
        
        basic = 'SELECT * FROM {} INNER JOIN {} '.format(table_a,table_b) 
        first_on= 'ON {}.ticker = {}.ticker '.format(table_a,table_b)
        #second_on = 'AND FEATURES_1M.ticker = INTRADAY_DAILY.ticker '
        basic_where = 'WHERE FEATURES_390M.date >=? AND FEATURES_390M.date <? '
        cond_statement= self.add_inner_cond_statement(table_a,cond_list)
        
        #sql_request = basic+first_on+second_on+basic_where+cond_statement
        sql_request = basic+first_on+basic_where+cond_statement
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        date_delta = date_candle + datetime.timedelta(hours = 23)
        #date_delta = date_candle + datetime.timedelta(seconds =30)
        
        request_tuple = (date_candle,date_delta)
        
        cur.execute(sql_request,request_tuple)
        data_list = cur.fetchall()
                    
        cur.close()
        con.close()
        
        col_list = self.get_inner_col(table_a,table_b)
        
        #data_struct='dict'
        if len(data_list)>0:
            if data_struct=='df':
                df = pd.DataFrame(data_list,columns=col_list) 
                #df.set_index('ticker',inplace=True)
                
                return df
            elif data_struct=='list':
                return data_list
            
            elif data_struct=='dict':
                data_dict = {}
                
                for row in data_list:
                    ticker = row[8]
                    data_dict[ticker]={}
                    for x in range(len(col_list)):
                        col = col_list[x]
                       
                        data_dict[ticker][col] = row[x]
                            
                return data_dict
        else:
            return data_list 
        
        
    def download_specific_candle(self,ticker,candle_date,table='FEATURES_1M',data_struct='dict'):
        '''
        Return a dict containing the data of the specified candle from ticker. 
        '''
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        upper_date = candle_date + datetime.timedelta(seconds=30)
    
        sql_request = 'SELECT * FROM {} WHERE ticker =? AND date>=? and date<?'.format(table)
        cur.execute(sql_request,(ticker,candle_date,upper_date))
        data = cur.fetchall()
        
        cur.close()
        con.close()
        
        if data_struct=='dict':
            row_dict = {}
            if len(data)>0:
                col_list = self.get_features_col(table)
                
                for x in range(len(col_list)):
                    if x>0:
                        col_name = col_list[x]
                        row_dict[col_name] = data[0][x]
            
            return row_dict
        
        
    def download_specific_day(self,date_morning,table,cond_list=[],data_struct='df'):
    
        '''
        Download all the stocks for a given date. Then set the index as the ticker since
        there cannot be two ticker associated to the same date. 
        '''
        
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        date_afternoon = date_morning + datetime.timedelta(hours = 23)
        
        #sql_request = 'SELECT * FROM {} WHERE date>=? and date<?'.format(table)
        sql_request = self.create_candle_sql_request(table,cond_list)
        
        cur.execute(sql_request,(date_morning,date_afternoon))
     
        data = cur.fetchall()
        
        cur.close()
        con.close()
        
        
        col_list = self.get_features_col(table)
        if data_struct=='df':
            df = pd.DataFrame(data,columns=col_list) 
            return df
        
        elif data_struct=='dict':
            data_dict = {}
            
            for row in data:
                
                date_index = row[0]
                ticker = row[8]
                #print('{} {}'.format(ticker,date_index))
                #print('data length {}'.format(len(data)))
                
                if ticker not in data_dict:
                    data_dict[ticker]={}
                
            
                data_dict[ticker][date_index]={}
                
                for x in range(len(col_list)):
                    if x>0:
                        col = col_list[x]
                        data_dict[ticker][date_index][col]=row[x]
                                    
            return data_dict
        else:
            
            return data
        
        
    def download_stock_list(self,ticker_list = [],table='STOCK',data_struct='df'):
        
        '''
        Download the list of all the stocks we are currently maintaining in our database. 
        
        At the time of this writing table could only be: STOCK or ACTIVE_STOCK
        '''
        
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        if len(ticker_list)==0:
            # download all ticker info
            sql_request = self.get_basic_sql_statement(table)
        else:
            # download ticker info from the list.
            sql_request = self.create_basic_ticker_in_request(ticker_list,table)
            
            
        cur.execute(sql_request)
        data = cur.fetchall()
               
        cur.close()
        con.close()
        
        
        if data_struct=='df':
            stock_col = self.stock_info_col
            df = pd.DataFrame(data,columns=stock_col)
            df.set_index('ticker',inplace=True)
            return df
        elif data_struct=='stock_list':
            return data
        
        
    def download_headlines(self,ticker_list,date_dict={},data_struct='df'):
        '''
        date_dict: a dictionary containing two key: begin_date,end_date. 
                   if empty all date are downloaded. 
        '''
        
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
    
        table='HEADLINES'
        
        if len(date_dict)==0:
            sql_request = self.create_basic_ticker_in_request(ticker_list,table)
            cur.execute(sql_request)
            data = cur.fetchall()
            
        else:
            sql_request = self.create_ticker_list_sql_request(ticker_list,table,cond_list=[])
            
            begin_date = date_dict['begin_date']
            end_date = date_dict['end_date']
            request_tuple = (begin_date,end_date)
            
            cur.execute(sql_request,request_tuple)
            data = cur.fetchall()
        #sql_request = 'SELECT * FROM {} WHERE ticker=?'.format(table)
        
            
        cur.close()
        con.close()
        
        if data_struct=='df':
            col_headline = self.get_news_headlines_col()
            df = pd.DataFrame(data,columns=col_headline)
            return df
        else:         
            return data
        
        
    def download_all_headlines(self,date_dict,data_struct='df'):
        '''
        date_dict: a dictionary containing two key: begin_date,end_date. 
                   if empty all date are downloaded. 
        '''
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        table='HEADLINES'
        
        sql_request = self.create_basic_date_request(table)
        
        begin_date = date_dict['begin_date']
        end_date = date_dict['end_date']
        request_tuple = (begin_date,end_date)
        
        cur.execute(sql_request,request_tuple)
        data = cur.fetchall()
            
        cur.close()
        con.close()
        
        if data_struct=='df':
            col_headline = self.get_news_headlines_col()
            df = pd.DataFrame(data,columns=col_headline)
            return df
        else:         
            return data
        
        
        
    def download_a_story(self,story_id,data_struct='df'):
        
        '''
        Return a dict containing the data of the specified candle from ticker. 
        '''
        story_id = int(story_id)
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
    
        table='STORY'
    
        sql_request = 'SELECT * FROM {} WHERE story_id=?'.format(table)
        cur.execute(sql_request,(story_id,))
        data = cur.fetchall()
            
        cur.close()
        con.close()
        
        if data_struct=='df':
            col_story = self.get_news_story_col()
            
            df = pd.DataFrame(data,columns=col_story)
            return df
        else:         
            return data
    
   
    '''
    UPDATE database info
    '''
    
    def update_stock_info(self,value,col,ticker):
        
        '''
        This is meant to update the last update, last cash or the short interest
        value of the stock table. 
        '''
        
        table='STOCK'
        update = self.create_stock_update_statement(table,col,key='ticker')
        
        if col=='date':
            value = value.to_pydatetime()
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        stock_info = self.download_stock_list([ticker],table)
        if len(stock_info)>0:
                 
    
            cur.execute(update,(value,ticker))
        
            con.commit() # note the db is locked until the transaction is commited.
        else:
             cur.close()
             con.close()
             raise Exception('stock info was not initialized')
    
        cur.close()
        con.close()
    
        
        
    '''
    SQL request builder functions
    ''' 
    
    
    def add_inner_cond_statement(self,table_a,cond_list):

        statement = ''
        for cond in cond_list:
            col = cond[0]
            condition = cond[1]
            value = cond[2]
            
            s = ' AND '+table_a+'.'+col+' '+condition+value
            
            statement = statement+s
            
        return statement
    
    
    def create_stock_update_statement(self,table,col,key):
        update = "UPDATE {} SET {} = ? WHERE {} =?".format(table,col,key)
        
        return update
    
    def get_basic_sql_statement(self,table):
        sql_request = 'SELECT * FROM {} '.format(table)
        return sql_request
    
    
    def add_where_statement(self,sql_request):
        sql_request = sql_request+'WHERE '
        return sql_request
    
    
    def add_condition_statement(self,sql_request,col,condition,):
        new =col+condition+'?'+' '
        sql_request = sql_request + new
        return sql_request
    
    
    def add_and_statement(self,sql_request):
        sql_request = sql_request +'AND '
        return sql_request
    
    
    
    def add_ticker_list(self,ticker_list,sql_request):
        ticker_tuple = tuple(ticker_list)
        list_statement = '{} '.format(ticker_tuple)
        sql_request = sql_request+list_statement
        return sql_request
    
    
    def create_basic_ticker_request(self,table='FEATURES_1M'):
        
        '''
        Create the basic sql request to download one ticker. 
        '''
        
        basic_sql = self.get_basic_sql_statement(table)    
        sql_request = self.add_where_statement(basic_sql)    
        
        sql_request = self.add_condition_statement(sql_request,col='ticker',condition='=') 
        sql_request = self.add_and_statement(sql_request)
        sql_request = self.add_condition_statement(sql_request,col='date',condition='>=')    
        sql_request = self.add_and_statement(sql_request)  
        sql_request = self.add_condition_statement(sql_request,col='date',condition='<=')  
        
        return sql_request
    
    
    def create_basic_ticker_in_request(self,ticker_list,table):
        basic_sql = self.get_basic_sql_statement(table)    
        sql_request = self.add_where_statement(basic_sql) 
        sql_request = sql_request +'ticker IN '
        
        sql_request = self.add_ticker_list(ticker_list, sql_request)
        
        return sql_request
    
    
    
    def create_basic_ticker_list_request(self,ticker_list,table='FEATURES_1M'):
        
        '''
        Create the basic sql request to download multiple ticker at once. 
        '''
        
        sql_request = self.create_basic_ticker_in_request(ticker_list,table)
        #basic_sql = self.get_basic_sql_statement(table)    
        #sql_request = self.add_where_statement(basic_sql) 
        #sql_request = sql_request +'ticker IN '
        
        #sql_request = self.add_ticker_list(ticker_list, sql_request)
        sql_request = self.add_and_statement(sql_request)
        sql_request = self.add_condition_statement(sql_request,col='date',condition='>=')    
        sql_request = self.add_and_statement(sql_request)  
        sql_request = self.add_condition_statement(sql_request,col='date',condition='<=')  
        
        return sql_request
        
        #sql_request = 'SELECT * FROM {} WHERE ticker IN {} and date>=? and date<?'.format(table,ticker_tuple)
       
    
    
    def create_basic_date_request(self,table='FEATURES_1M'):
        '''
        Create the basic sql request to download all ticker for a given
        candle. 
        '''
        
        basic_sql = self.get_basic_sql_statement(table)    
        sql_request = self.add_where_statement(basic_sql)    
        
        sql_request = self.add_condition_statement(sql_request,col='date',condition='>=')    
        sql_request = self.add_and_statement(sql_request)  
        sql_request = self.add_condition_statement(sql_request,col='date',condition='<=')  
        
        return sql_request
    
    
    def add_extra_conditions(self,cond_list):
        '''
        cond_list is a list of list. The smallest list has the following
        format:
            
            [column,condition,value]
            
        Column, condition and value are all string. This function add a list on 
        of conditions on top of the basic conditions. Therefore it assume that an 
        and statement is needed at the beginning.
            
        The return value is a string to be added to the original sql statement. 
        '''
        
        conditions = ''
        for row in cond_list:
           
           temp = 'AND '+row[0]+row[1]+row[2]+' '
           conditions = conditions + temp
           
        return conditions
    
    
    def create_ticker_sql_request(self,table,cond_list):
        sql_request = self.create_basic_ticker_request(table)
        
        if len(cond_list)>0:
            conditions = self.add_extra_conditions(cond_list)
            sql_request = sql_request + conditions
            
        return sql_request
    
    
    def create_ticker_list_sql_request(self,ticker_list,table,cond_list):
        sql_request = self.create_basic_ticker_list_request(ticker_list,table)
        
        if len(cond_list)>0:
            conditions = self.add_extra_conditions(cond_list)
            sql_request = sql_request + conditions
            
        return sql_request
    
    
    def create_candle_sql_request(self,table,cond_list):
        sql_request = self.create_basic_date_request(table)
        
        if len(cond_list)>0:
            conditions = self.add_extra_conditions(cond_list)
            sql_request = sql_request + conditions
            
        return sql_request
    
    
    '''
    DELETE FUNCTION
    '''
    
    def delete_stock(self,ticker):
        
        tables = ['FEATURES_1M','FEATURES_390M','INTRADAY_DAILY']
        stock_table = 'STOCK'
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        for table in tables:
            try:
                delete = "DELETE FROM {} WHERE ticker =?".format(table)
                cur.execute(delete,(ticker,)) 
                con.commit() # note the db is locked until the transaction is commited. 
                print('We have deleted', cur.rowcount,' records of {} from the table: {}'.format(ticker,table))
            except Exception as e:
                print(e)
            
        
        try:
            delete = "DELETE FROM {} WHERE ticker =?".format(stock_table)
            cur.execute(delete,(ticker,)) 
            con.commit() # note the db is locked until the transaction is commited. 
            print('We have deleted', cur.rowcount,' records of {} from the table: {}'.format(ticker,stock_table))
        except Exception as e:
            print(e)
            
        
        cur.close()
        con.close()
        
        
    def delete_390m_ticker(self,ticker):
        table='FEATURES_390M'
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        try:
            delete = "DELETE FROM {} WHERE ticker =?".format(table)
            cur.execute(delete,(ticker,)) 
            con.commit() # note the db is locked until the transaction is commited. 
            print('We have deleted', cur.rowcount,' records of {} from the table: {}'.format(ticker,table))
        except Exception as e:
            print(e)
            
        cur.close()
        con.close()
        
        
        
    
    def delete_features(self,ticker):
        
        table = 'FEATURES_1M'
        #stock_table = 'STOCK'
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        try:
            delete = "DELETE FROM {} WHERE ticker =?".format(table)
            cur.execute(delete,(ticker,)) 
            con.commit() # note the db is locked until the transaction is commited. 
            print('We have deleted', cur.rowcount,' records of {} from the table: {}'.format(ticker,table))
        except Exception as e:
            print(e)
        
               
        cur.close()
        con.close()
        
        
        
    def delete_a_row(self,ticker,candle_date,table):
        
        if table =='FEATURES_1M':
            print('do not touch that table!')
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        try:
            delete = "DELETE FROM {} WHERE ticker =? AND date=?".format(table)
            cur.execute(delete,(ticker,candle_date)) 
            con.commit() # note the db is locked until the transaction is commited. 
            
        except Exception as e:
            print(e)
               
        cur.close()
        con.close() 
    

    '''
    COMPLEXE FUNCTION NEEDED TO CREATE THE UPDATE AND LIVE DATA. 
    '''
    
    
    def get_filled_queue(self,ticker,date_morning,col):
        '''
        This function assume the database is properly updatted. 
        '''
      
        table='FEATURES_1M' 
        date_list = [date_morning]
        df = self.download_ticker(ticker,table,date_list,cond_list=[],data_struct='df')
        
        q = queue.Queue(maxsize=20)  
        df = df[-20:]
        for index,row in df.iterrows():
            if q.full()==False:
                q.put(row[col])
            else:
                q.get()
                q.put(row[col])
                
        return q
    
    
    def get_daily_filled_queue(self,ticker,date_list,col):
        '''
        This function assume the database is properly updatted.
        
        Note, date_list need to contain the previous 20 day. The date list speed up the download
        process. 
        '''
        
        table='FEATURES_390M'
        
        
        df = self.download_ticker(ticker,table,date_list,cond_list=[],data_struct='df')
        
        q = queue.Queue(maxsize=20)  
        df = df[-20:]
        for index,row in df.iterrows():
            if q.full()==False:
                q.put(row[col])
            else:
                q.get()
                q.put(row[col])
                
        return q
    
    
    def get_bb_dict(self,watchlist,date_morning):
        
        bb_dict = {}    
        for ticker in watchlist:
            bb_queue = self.get_filled_queue(ticker, date_morning, col='close')
            bb_dict[ticker] = bb_queue
        
        return bb_dict
    
    
    def get_vwap_queue_dict(self,watchlist,date_morning):
        
        vwap_dict = {}    
        for ticker in watchlist:
            price_q = self.get_filled_queue(ticker, date_morning, col='average')
            volume_q = self.get_filled_queue(ticker,date_morning,col='volume')
            
            vwap_dict[ticker]={}
            vwap_dict[ticker]['price_q']=price_q
            vwap_dict[ticker]['volume_q']=volume_q
        
        return vwap_dict
    
    
    def get_last_updated_candle(self,ticker):
        stock_info = self.download_stock_list([ticker])
        last_update = stock_info.loc[ticker,'last_update']
        last_candle = datetime.datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
        
        return last_candle
    
    def get_last_row(self,ticker):
        last_candle = self.get_last_updated_candle(ticker)
        last_row = self.download_specific_candle(ticker, last_candle)
            
        return last_row,last_candle
    
    
    def split_data_by_minute(self,date_list,cond_list=[]):
        '''
        This function download minute per minute all the data available from the database.
        This function take a lot of time and is only used to train the models. 
        '''
        
        
        if len(cond_list)==0:
            a =['close','>=','0.002']
            b =['ema13_cash','>=','10000']
            c =['average_cash','>=','10000']
            d =['gains','<=','1.5']
            cond_list = [a,b,c,d]
        
        temps = t.time()
        table='FEATURES_1M'
       
        #features = self.get_1m_training_features()
           
        date_dict = {}
        
        count_days = 0
        count = 0
        for index in date_list:
            date_candle = index.to_pydatetime() 
            
            df = self.download_minute_candle(date_candle,table,cond_list,data_struct='df')
           
            
            if len(df)>0:
               df.dropna(inplace=True) 
            
            
            if len(df)>0:
                #df = df[features]
                date_dict[date_candle]= df
                
            count = count + 1
            if count>=391:
                count = 0
                count_days = count_days+1
                print('Downloaded days: {} , length: {}, date: {}'.format(count_days,len(date_dict),date_candle))
                
        temps = t.time()-temps
        print('pulling the data tooks: {}'.format(temps))
                
        return date_dict
    
    
    def split_combined_by_minute(self,date_list,cond_list=[]):
        '''
        This function download minute per minute all the data available from the database.
        This function take a lot of time and is only used to train the models.
        
        The result is the combined features_1m data and intraday daily_data. 
        '''
        
        
        if len(cond_list)==0:
            a =['close','>=','0.002']
            b =['ema13_cash','>=','10000']
            c =['average_cash','>=','10000']
            d =['gains','<=','1.5']
            cond_list = [a,b,c,d]
        
        temps = t.time()
        table_a='FEATURES_1M'
        table_b='INTRADAY_DAILY'
           
        date_dict = {}
        
        count_days = 0
        count = 0
        for index in date_list:
            date_candle = index.to_pydatetime() 
            
            #df = self.download_minute_candle(date_candle,table,cond_list,data_struct='df')
            df = self.download_inner_join_candle(date_candle,table_a,table_b,cond_list,data_struct='df')
            
            if len(df)>0:
               df.dropna(inplace=True) 
            
            
            if len(df)>0:
                #df = df[features]
                date_dict[date_candle]= df
                
            count = count + 1
            if count>=391:
                count = 0
                count_days = count_days+1
                print('Downloaded days: {} , length: {}, date: {}'.format(count_days,len(date_dict),date_candle))
                
        temps = t.time()-temps
        print('pulling the data tooks: {}'.format(temps))
                
        return date_dict
    
    
    
    
    def split_data_by_days(self,date_list,cond_list=[]):
        
        
        '''
        This function download days by day all the data available from the database.
        This function is used to train the daily model. 
        '''
        
        if len(cond_list)==0:
            a =['close','>=','0.002']
            b =['ema13_cash','>=','400000']
            c =['average_cash','>=','1000000']
            d =['gains','<=','10']
            cond_list = [a,b,c,d]
        
        temps = t.time()
        table='FEATURES_390M'
       
        #features = self.get_390m_training_features()
           
        date_dict = {}
        
        for index in date_list:
            date_morning = index.to_pydatetime() 
            
            df = self.download_specific_day(date_morning,table,cond_list,data_struct='df')
            
            if len(df)>0:
               df.dropna(inplace=True) 
            
            
            if len(df)>0:
                #df = df[features]
                date_dict[date_morning]= df
                
          
                
        temps = t.time()-temps
        print('pulling the days to days data tooks: {}'.format(temps))
                
        return date_dict
    
    
    def split_data_by_days_with_shares(self,date_list,cond_list=[]):
        
        
        '''
        This function download days by day all the data available from the database.
        This function is used to train the daily model. 
        '''
        
        if len(cond_list)==0:
            a =['close','>=','0.002']
            b =['ema13_cash','>=','400000']
            c =['average_cash','>=','1000000']
            d =['gains','<=','10']
            cond_list = [a,b,c,d]
        
        temps = t.time()
        table='FEATURES_390M'
       
        #features = self.get_390m_training_features()
           
        date_dict = {}
        
        for index in date_list:
            date_morning = index.to_pydatetime() 
            
            #df = self.download_specific_day(date_morning,table,cond_list,data_struct='df')
            df = self.download_inner_join_common_share(date_morning,cond_list=cond_list)

            if len(df)>0:
               df.dropna(inplace=True) 
            
            
            if len(df)>0:
                #df = df[features]
                date_dict[date_morning]= df
                
          
                
        temps = t.time()-temps
        print('pulling the days to days data tooks: {}'.format(temps))
                
        return date_dict
        
        
    
    
    def split_liquid_stocks(self,min_daily_cash=3000000):
        '''
        This function enable the download of only liquid stock. 
        '''
        pass
    
    
    
    def find_liquid_stocks(self,date_list,min_daily_cash=3000000):
        pass
    
    
    
    def create_train_set(self,train_dict,drop_index=True):
        
        train_df = pd.concat(train_dict.values()) 
        train_df = train_df.sample(frac=1).reset_index(drop=drop_index)
        
        train_df.dropna(inplace=True)
        
        return train_df
    
    
    def split_cv_date(self,date_dict,n = 5):
        count = len(date_dict)
        split_size = math.floor(count/n)
        n_array = np.full((count,1),1)
        
        value = n
        counter = split_size
        
        for x in range(count):
            if counter>0:
                counter = counter-1
                n_array[x] = value
            else:
                counter = split_size
                value = value -1
                n_array[x]=value
            
            if value<=1:
                break
        
        np.random.shuffle(n_array)
        
        cv_dict = {}
        for key in date_dict:
            count = count-1
            cv_dict[key] = n_array[count]
            
        
        return cv_dict 
    
    
    """
    There is something wrong with this method. I spent nearly 2 weeks
    trying to figure out why it doesnt work all the time. 
    def train_test_split(self,train_dict,split_cv,split_index):
          
        training_dict = {}
        testing_dict = {}
        
        for date_candle in train_dict:
            df = train_dict[date_candle]
           
            index = split_cv[date_candle]
                
            if index==split_index:
                testing_dict[date_candle] = df
            else:
                training_dict[date_candle] = df
          
        #print('training_dict length is: {}'.format(len(training_dict)))
        train_df = self.create_train_set(training_dict)
        
            
        return train_df,testing_dict
    """
    
    
    def train_test_df(self,train_dict,split_cv,split_index):
          
        training_dict = {}
        testing_dict = {}
        
        for date_candle in train_dict:
            df = train_dict[date_candle]
           
            index = split_cv[date_candle]
                
            if index==split_index:
                testing_dict[date_candle] = df
            else:
                training_dict[date_candle] = df
              
        
        train_df = self.create_train_set(training_dict)
        test_df = self.create_train_set(testing_dict)
            
        return train_df,test_df
    
    
    
    def number_days_since_last_update(self,ticker):
        
        stock_info = self.download_stock_list([ticker])
        
        last_update = stock_info.loc[ticker,'last_update']
        last_candle = datetime.datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')
        
        today = datetime.datetime.today()
        
        diff = today-last_candle
        
        return diff.days
    
    
    
    def market_days_since_last_update(self,ticker,calendar):
        stock_info = self.download_stock_list([ticker])
        
        last_update = stock_info.loc[ticker,'last_update']
        last_candle = datetime.datetime.strptime(last_update, '%Y-%m-%d %H:%M:%S')  
        
        #calendar = self.dh.get_market_calendar(False)  
        new_calendar = calendar[last_candle:]
        
        return len(new_calendar)
    
    
    
    def create_from(self,data,create='df',data_struct='dict'):

        if create=='df':
            if data_struct=='dict':
                df = pd.DataFrame.from_dict(data,orient='index')
                return df
            
            
    def update_all_ng(self,ticker_dict,next_list,intraday_date_list,ticker,table='FEATURES_1M'):

        con = sqlite3.connect(self.db_name)
        #con = sqlite3.connect(db_name)
        cur = con.cursor()
         
        for d in intraday_date_list:
            
            candle_date = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
        
            #print(candle_date)
            for features in next_list:
                next_value = ticker_dict[d][features]
                
                if (next_value is None) or (math.isnan(next_value)):
                    pass
                else:
                
                    update = "UPDATE {} SET {} = {} WHERE ticker =? AND date=?".format(table,features,next_value)
                    #cur.execute("UPDATE STOCK SET score = 0.0 WHERE ticker=?",(index,))
                    cur.execute(update,(ticker,candle_date))
        
            con.commit() # note the db is locked until the transaction is commited. 
        
        cur.close()
        con.close()
        
        
    def download_completed_days(self):
        '''
        Return a dict containing the data of the specified candle from ticker. 
        '''
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
    
        table='COMPLETED_DAYS'
    
        sql_request = 'SELECT * FROM {} '.format(table)
        cur.execute(sql_request)
        data = cur.fetchall()
            
        cur.close()
        con.close()
            
        return data
    
    
    def download_all_trades(self,data_struct='df'):
    
        '''
        Download all the stocks for a given date. Then set the index as the ticker since
        there cannot be two ticker associated to the same date. 
        '''
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        table='TRADES'
        col = self.get_features_col(table)
        
        sql_request = 'SELECT * FROM {}'.format(table)
        cur.execute(sql_request)
     
        data = cur.fetchall()
        
        cur.close()
        con.close()
        
        if data_struct=='df':
            df = pd.DataFrame(data,columns=col) 
            return df
        else:
            
            return data
        
        
    def save_trades(self,df):
        
        if df.empty:
            raise Exception('No data was provided')
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        table='TRADES'
        
        for ticker, row in df.iterrows():
        # verify if a given row is already present in the database.
            
            buy_date = datetime.datetime.strptime(row['buy_date'], '%Y-%m-%d %H:%M:%S')
            #buy_date = row['buy_date'].to_pydatetime()
                
            sql_request = 'SELECT * FROM {} WHERE ticker=? AND buy_date=?'.format(table)
            cur.execute(sql_request,(ticker,buy_date))
                
            #cur.execute("SELECT * FROM INTRADAY_PRICE WHERE ticker=? AND date=?",(row['ticker'],date))
            temp = cur.fetchall()
            if len(temp)>=1:
                df.drop([ticker],inplace = True)
        
        # Store the new data row in the database.
        if len(df)>0:
            
            df.to_sql(name=table, con=con, if_exists='append')
            con.commit()
                
            print('Saved a trade from {} {}'.format(ticker,buy_date))
        else:
            print('{} {} was already saved'.format(ticker,buy_date))
    
        
        cur.close()
        con.close() 
        
        
    def save_completed_day(self,df):
        
        if df.empty:
            raise Exception('No data was provided')
        
        con = sqlite3.connect(self.db_name)
        cur = con.cursor()
        
        table='COMPLETED_DAYS'
        
        for index, row in df.iterrows():
        # verify if a given row is already present in the database.
            date = index.to_pydatetime()
                
            sql_request = 'SELECT * FROM {} WHERE date_morning=?'.format(table)
            cur.execute(sql_request,(date,))
                
            #cur.execute("SELECT * FROM INTRADAY_PRICE WHERE ticker=? AND date=?",(row['ticker'],date))
            temp = cur.fetchall()
            if len(temp)>=1:
                df.drop([index],inplace = True)
        
        # Store the new data row in the database.
        if len(df)>0:
            
            df.to_sql(name=table, con=con, if_exists='append')
            con.commit()
                
            print('Saved {} as completed'.format(date))
        else:
            print('{} was already completed'.format(date))
    
        
        cur.close()
        con.close() 
        
        
        
    def get_390m_data(self,cond_list):  
           
           calendar = self.dh.get_market_calendar(keep_2021=True)
           date_list = calendar.index.to_list()
           
           stock_dict = self.split_data_by_days(date_list,cond_list=cond_list)
           
           return stock_dict
       
        
    def get_390m_data_with_shares(self,cond_list):  
           
           calendar = self.dh.get_market_calendar(keep_2021=True)
           date_list = calendar.index.to_list()
           
           #stock_dict = dbm.split_data_by_days(date_list,cond_list=cond_list)
           stock_dict = self.split_data_by_days_with_shares(date_list,cond_list=cond_list)
           
           return stock_dict
      
        
    def create_data_df(self,train_dict):
        data_df = pd.concat(train_dict.values()) 
        data_df = data_df.sample(frac=1).reset_index(drop=True)
        
        return data_df


    def compute_market_cap(self,common_share,close):
        common_share = float(common_share)
        
        if common_share==0:
            return 0
        else:
            if close=='':
                return 0
            else:
                price = float(close)
                market_cap = price*common_share
                return market_cap



    def add_market_cap(self,df):
        df['market_cap'] = df.apply(lambda x: self.compute_market_cap(x['common_share'],x['close']),axis=1)
      

    def create_daily_ticker_list(self,cond_list =[],market_cap=3000000000):
        
        stock_info = self.download_stock_list()
        
        table_390m = 'FEATURES_390M'
        
        if len(cond_list)==0:
            a =['close','>=','0.002']
            b =['ema13_cash','>=','1000000']
            c =['average_cash','>=','3000000']
            d =['gains','<=','10']
            cond_list = [a,b,c,d]
        
        if self.db_name=='american_database':
            stock_dict = self.get_390m_data_with_shares(cond_list)
        elif self.db_name=='otc_database':
            stock_dict = self.get_390m_data(cond_list)
        
        dh = date_manager.date_handler()
        date_list = dh.create_date_list_from_dict(stock_dict)
        
        data_df = self.create_data_df(stock_dict)
        
        if self.db_name=='american_database':
            self.add_market_cap(data_df)
            data_df = data_df.loc[data_df['market_cap']<=market_cap]
        
        temps = t.time()
        daily_ticker_dict={}
        for index,row in data_df.iterrows():
            
            d = row['date']
            date_index = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
            date_index = date_index.replace(hour=0,minute=0)
            if date_index not in daily_ticker_dict:
                stocks = data_df.loc[data_df['date']==d]   
                ticker_list = stocks['ticker'].to_list()
                daily_ticker_dict[date_index]=ticker_list
                
        temps = t.time()-temps
        print('Creating the ticker list took {} seconds'.format(temps))
        
        return daily_ticker_dict,date_list
    
    
    def download_day_to_day_training_data(self,print_date=True,market_cap=3000000000,cond_list=[],delayed=False):
      
        daily_ticker_dict,date_list = self.create_daily_ticker_list(cond_list=cond_list,market_cap=market_cap)

        table='FEATURES_1M'

        #date_list = date_list[0:5]
        if delayed:
            stock_dict = self.download_delayed_day_to_day_ticker_list(daily_ticker_dict,table,date_list,cond_list=[],data_struct='stock_dict',print_date=print_date)
        else:
            stock_dict = self.download_day_to_day_ticker_list(daily_ticker_dict,table,date_list,cond_list=[],data_struct='stock_dict',print_date=print_date)
        
        return stock_dict
    
    
    def get_exchange_dict(self):

        stock_info = self.download_stock_list()
        exchange_dict={}
        
        for ticker,row in stock_info.iterrows():
            exchange = row['exchange']
            exchange_dict[ticker]=exchange
            
        return exchange_dict  
    
    
    def print_unique_fifth_letter(self,watchlist):

         fifth_list = []    
         for ticker in watchlist:
             if len(ticker)>4:
                fifth = ticker[4]
                fifth_list.append(fifth)
                
         unique_letter = set(fifth_list)
         print('The unique fifth letter are:')
         print(unique_letter) 
         
         
    def create_specific_day_watchlist(self,date_index,num_stocks=490,data_struct='dict',sort_by='average_cash'):    
      
        #sort_by='average_cash'
            
        table='FEATURES_390M'
        cond_list=[]
        df390 = self.download_specific_day(date_index,table,cond_list,data_struct='df')
           
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
            
        self.print_unique_fifth_letter(watchlist) 
        
        
        return watchlist
  
 

  
    
    
    
    
    
                
                
                
                
                
#end