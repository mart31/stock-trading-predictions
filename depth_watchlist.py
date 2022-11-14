# -*- coding: utf-8 -*-
"""
Created on Wed Feb 23 11:13:26 2022

@author: marti
"""

import sqlite3
import pandas as pd
import time as t

import otc_object
import prepare_live_data as pld


'''
This module creates a sqlite file named: watchlist_database. The watchlist database is used to track
live stocks. The watchlist change every day. 
'''


class depth_watchlist():
    
    def __init__(self,num_stocks=500,init=False,active_liquid=False):
        
        if init:
            self.create_watchlist(num_stocks)
        
        self.watchlist = self.download_watchlist('dict')    
        
        self.liquid_length = 20
        self.top_x_liquid = self.find_top_x_liquid()
        
        if active_liquid:
            self.activate_liquid_stock()
            
            
          
    def get_top_x_liquid(self):
        return self.top_x_liquid
        
    
    def get_watchlist(self):
        self.watchlist = self.download_watchlist('dict')
        return self.watchlist
    
    def update_watchlist(self):
        self.watchlist = self.download_watchlist('dict')
    
    
    def get_active_watchlist(self,add_pk=True):
        
        '''
        Just to be safe we reload the watchlist before creating the active watchlist.
        The active watchlist dictate which stock should be followed. 
        '''
        
        self.watchlist = self.download_watchlist('dict')
        active_watchlist={}
        
        for ticker in self.watchlist:
            active = self.watchlist[ticker]['active']
            invalid = self.watchlist[ticker]['invalid']
            db_name = self.watchlist[ticker]['db_name']
            
            if (active) and (invalid==0):
                if db_name=='otc_database' and add_pk:
                    pk_ticker = ticker + '.PK'
                    active_watchlist[pk_ticker]=self.watchlist[ticker]
                else:
                    active_watchlist[ticker]=self.watchlist[ticker]
                
                
        return active_watchlist
    
    
    def create_watchlist_database(self):
        con = sqlite3.connect('watchlist_database')
        cur = con.cursor()
    
        
        
        watchlist_table = '''CREATE TABLE WATCHLIST(
                                ticker text NOT NULL,
                                average_cash real,
                                candle_range real,
                                active integer NOT NULL,
                                db_name text NOT NULL,
                                invalid integer,
                                PRIMARY KEY (ticker)) '''
        
        
        cur.execute('DROP table WATCHLIST')
       
        cur.execute(watchlist_table)
        
        cur.close()
        con.close()
    
    
    def get_watchlist_col(self,include_ticker=True):
        if include_ticker:
            col = ['ticker','average_cash','candle_range','active','db_name','invalid']
        else:
            col = ['average_cash','candle_range','active','db_name','invalid']
            
        return col
    
    def create_watchlist(self,num_stocks=500):
        
        '''
        Initialize the watchlist database and add the stock that we will be
        watching today. 
        '''
        
        self.create_watchlist_database()
        
        watchlist = pld.get_watchlist_df()
        watchlist['invalid']=0
                
        con = sqlite3.connect('watchlist_database')
        cur = con.cursor()
        
        watchlist.to_sql(name='WATCHLIST', con=con, if_exists='append')
        con.commit()
        
        print('Created a watchlist with {} stocks'.format(len(watchlist)))
        
        cur.close()
        con.close() 
    
    
    def download_watchlist(self,data_struct='df'):
        
        
        con = sqlite3.connect('watchlist_database')
        cur = con.cursor()
        
        col = self.get_watchlist_col()
        
        table='WATCHLIST'
        sql_request = 'SELECT * FROM {}'.format(table)
        cur.execute(sql_request)
        data = cur.fetchall()
        
        if data_struct=='df':
            watchlist = pd.DataFrame(data,columns=col)
            watchlist = watchlist[watchlist['invalid']==0]
        elif data_struct=='dict':
            watchlist = {}
            
            for row in data:
              
                ticker = row[0]
                average_cash = row[1]
                candle_range = row[2]
                active=row[3]
                db_name = row[4]
                invalid = row[5]
                
                if invalid==0:
                    watchlist[ticker]= {}
                    watchlist[ticker]['average_cash']=average_cash
                    watchlist[ticker]['candle_range']=candle_range
                    watchlist[ticker]['active']=active
                    watchlist[ticker]['db_name']=db_name
                    watchlist[ticker]['invalid']=invalid
                    
        cur.close()
        con.close()
         
        
        return watchlist
    
    
    def activate_ticker(self,ticker):
        con = sqlite3.connect('watchlist_database')
        cur = con.cursor()
        
        table='WATCHLIST'
        active=1
        
        update = "UPDATE {} SET active = ? WHERE ticker =?".format(table)
        cur.execute(update,(active,ticker))
    
        con.commit() # note the db is locked until the transaction is commited.
        
        print('activated {} level 2 data'.format(ticker))
            
        cur.close()
        con.close()
        
        
    def activate_ticker_list(self,ticker_list):
        con = sqlite3.connect('watchlist_database')
        cur = con.cursor()
        
        table='WATCHLIST'
        active=1
        
        for ticker in ticker_list:
            update = "UPDATE {} SET active = ? WHERE ticker =?".format(table)
            cur.execute(update,(active,ticker))
    
        con.commit() # note the db is locked until the transaction is commited.
        
        #print('activated {} level 2 data'.format(ticker_list))
            
        cur.close()
        con.close()
        
        
    def remove_pk(self,ticker):
    
        sep = '.'
        stripped_ticker = ticker.split(sep,1)[0]
        
        return stripped_ticker
        
        
    
    def invalidate_ticker(self,invalid_ticker):
        con = sqlite3.connect('watchlist_database')
        cur = con.cursor()
        
        ticker = self.remove_pk(invalid_ticker)
        
        table='WATCHLIST'
        invalid=1
        
        update = "UPDATE {} SET invalid = ? WHERE ticker =?".format(table)
        cur.execute(update,(invalid,ticker))
    
        con.commit() # note the db is locked until the transaction is commited.
        
        print('')
        print('*****')
        print('invalidated {} level 2 data'.format(ticker))
        self.deactivate_ticker(ticker)
        print('*****')
        print('')
        
        self.update_watchlist()
        
        cur.close()
        con.close()
        
        
    def deactivate_ticker(self,ticker):
        con = sqlite3.connect('watchlist_database')
        cur = con.cursor()
        
        table='WATCHLIST'
        active=0
        
        update = "UPDATE {} SET active = ? WHERE ticker =?".format(table)
        cur.execute(update,(active,ticker))
    
        con.commit() # note the db is locked until the transaction is commited.
            
        print('deactivated {} level 2 data'.format(ticker))
        
        cur.close()
        con.close()
        
    def deactivate_ticker_list(self,ticker_list):
        con = sqlite3.connect('watchlist_database')
        cur = con.cursor()
        
        table='WATCHLIST'
        active=0
        
        for ticker in ticker_list:
            update = "UPDATE {} SET active = ? WHERE ticker =?".format(table)
            cur.execute(update,(active,ticker))
        
        con.commit() # note the db is locked until the transaction is commited.
            
        #print('deactivated {} level 2 data'.format(ticker))
        
        cur.close()
        con.close()
    
    def deactivate_all_ticker(self):
        
        for ticker in self.watchlist:
            self.deactivate_ticker(ticker)
        
        
    
    
    def activate_liquid_stock(self):
        
        for ticker in self.top_x_liquid:
       
            self.activate_ticker(ticker)
                
        print('Activated {} liquid stock'.format(len(self.top_x_liquid)))
        
        
    def print_active_watch(self):
        l = []
        
        active_watchlist = self.get_active_watchlist()
        
        for ticker in active_watchlist:
            l.append(ticker)
            
        print(l)
        
        
    def find_top_x_liquid(self):
        
        df_watchlist = self.download_watchlist(data_struct='df')
        
        #if self.db_name=='otc_database':
        #    df_watchlist.sort_values('average_cash',ascending=False,inplace=True)
        #elif self.db_name=='american_database':  
        df_watchlist.sort_values('candle_range',ascending=False,inplace=True)
        
        temp = df_watchlist[0:self.liquid_length]
        top_x_liquid = temp['ticker'].to_list()
    
        return top_x_liquid
              
       

#db_name='american_database'
#dwl = depth_watchlist(db_name=db_name,init=True)  
#dwl = depth_watchlist()    
#watchlist = dwl.get_watchlist()
#top_x_liquid = dwl.find_top_x_liquid()

#active_watchlist = dwl.get_active_watchlist(add_pk=False)
#active_watchlist = dwl.get_active_watchlist()
#dwl.deactivate_all_ticker()


"""
ticker_list = []
for ticker in active_watchlist:
    ticker_list.append(ticker)

import depth_data
dd = depth_data.depth_data()

depth_dict = dd.download_depth_list(ticker_list)
"""

















# end



