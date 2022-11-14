# -*- coding: utf-8 -*-
"""
Created on Thu Feb 24 12:35:49 2022

@author: marti
"""

import sqlite3
#import db_manager
import prepare_live_data as pld
import pandas as pd
import time as t

class depth_data():
    
    def __init__(self,init=False):
        
        if init: 
            self.initialize_database()
        
        

    def get_depth_columns(self,include_ticker=True):
        if include_ticker:
            col = ['ticker','ind','mmid','price','size']
        else:
            col = ['ind','mmid','price','size']
            
        return col
            
    
    def create_depth_database(self,drop=True):
            con = sqlite3.connect('depth_database')
            cur = con.cursor()
        
        
            ask_table = '''CREATE TABLE ASK(
                                    ticker text NOT NULL,
                                    ind int NOT NULL,
                                    mmid text,
                                    price real NOT NULL,
                                    size real NOT NULL,
                                    PRIMARY KEY (ticker,ind)) '''
        
        
            bid_table = '''CREATE TABLE BID(
                                    ticker text NOT NULL,
                                    ind integer NOT NULL,
                                    mmid text,
                                    price real NOT NULL,
                                    size real NOT NULL,
                                    PRIMARY KEY (ticker,ind)) '''
            
            if drop:
                cur.execute('DROP table ASK')
                cur.execute('DROP table BID')
           
            cur.execute(ask_table)
            cur.execute(bid_table)
            
            cur.close()
            con.close()
            
    
    def get_watchlist(self):
            
            #dbm = db_manager.db_manager('iq_database')
            #watchlist = dbm.today_watchlist(num_stocks=num_stocks)
            
            watchlist = pld.get_watchlist_df()
            
            return watchlist
    
    
    
    def create_init_df(self,index_nbr=10):
        
        watchlist = self.get_watchlist()
        ticker_list = watchlist.index.to_list()
        
        depth_list = []
        depth_col = self.get_depth_columns()
        
        for ticker in ticker_list:
            
            price=0
            size=0
            mmid='NA'
            
            for x in range(index_nbr):
                ind = x
                
                row = [ticker,ind,mmid,price,size]
                depth_list.append(row)
        
        df = pd.DataFrame(depth_list,columns=depth_col)
        
        df.set_index('ticker',inplace=True)
        
        return df
    
    
    def initialize_rows(self):
        
        '''
        This function assumes the depth database has been resetted. 
        The rows are already ready simplifying the live processes. 
        '''
        
        init_df = self.create_init_df()
        
        con = sqlite3.connect('depth_database')
        cur = con.cursor()
        
        table='ASK'
        init_df.to_sql(name=table, con=con, if_exists='append')
        
        table='BID'
        init_df.to_sql(name=table, con=con, if_exists='append')
        
        con.commit()
        
        print('The depth database was initialized using {} rows'.format(len(init_df)))
               
        cur.close()
        con.close() 
    
    
    def initialize_database(self):
        self.create_depth_database()
        self.initialize_rows()
        
        
    def update_row(self,row,table):
        '''
        The rows need to have this format:
            [ticker,ind,mmid,price,size]
            
        The table choice is either bid or ask. 
        '''
        
       
        con = sqlite3.connect('depth_database')
        cur = con.cursor()
        
        update = "UPDATE {} SET mmid = ? , price = ? , size = ? WHERE ticker =? AND ind = ?".format(table)
        cur.execute(update,(row[2],row[3],row[4],row[0],row[1]))
    
        con.commit() # note the db is locked until the transaction is commited.
        
            
        cur.close()
        con.close()
        
        
    def update_multiple_row(self,row_list,table):
        '''
        The rows need to have this format:
            [ticker,ind,mmid,price,size]
            
        The table choice is either bid or ask. 
        '''
        
       
        con = sqlite3.connect('depth_database')
        cur = con.cursor()
        
       
        for row in row_list:
            #print(row[2])
            #if (row[2]=='AAOI') or (row[2]=='ABEO') or (row[2]=='ACRX') or (row[2]=='ADIL'):
            #    print(row)
            
            update = "UPDATE {} SET mmid = ? , price = ? , size = ? WHERE ticker =? AND ind = ?".format(table)
            cur.execute(update,(row[2],row[3],row[4],row[0],row[1]))
    
        con.commit() # note the db is locked until the transaction is commited.
        
        print('Updated {} row of the {} table'.format(len(row_list),table))
        
        cur.close()
        con.close()
        
        
        
    def download_depth_data(self,ticker):
        
        con = sqlite3.connect('depth_database')
        cur = con.cursor()
        
        table='ASK'
        sql_request = 'SELECT * FROM {} WHERE ticker= ?'.format(table)
        cur.execute(sql_request,(ticker,))
        ask_data = cur.fetchall()
        
        table='BID'
        sql_request = 'SELECT * FROM {} WHERE ticker= ?'.format(table)
        cur.execute(sql_request,(ticker,))
        bid_data = cur.fetchall()
                    
        cur.close()
        con.close()
        
        return ask_data,bid_data
    
    
    def download_depth_list(self,ticker_list,data_struct='dict'):
                    
    
        ticker_tuple = tuple(ticker_list)
        
        con = sqlite3.connect('depth_database')
        cur = con.cursor()
        
        table='ASK'
        sql_request = 'SELECT * FROM {} WHERE ticker IN {}'.format(table,ticker_tuple)
        cur.execute(sql_request)
        ask_data = cur.fetchall()
        
        table='BID'
        sql_request = 'SELECT * FROM {} WHERE ticker IN {}'.format(table,ticker_tuple)
        cur.execute(sql_request)
        bid_data = cur.fetchall()
       
        cur.close()
        con.close()
        
        if data_struct=='dict':
            depth_dict = {}
            
            for row in ask_data:
                ticker = row[0]
                
                if ticker not in depth_dict:
                    depth_dict[ticker]={}
                    depth_dict[ticker]['ask']=[]
                    depth_dict[ticker]['bid']=[]
                    
                depth_dict[ticker]['ask'].append(list(row))
                
                
            for row in bid_data:
                ticker = row[0]
                
                if ticker not in depth_dict:
                    depth_dict[ticker]={}
                    depth_dict[ticker]['bid']=[]
                    depth_dict[ticker]['ask']=[]
                    
                depth_dict[ticker]['bid'].append(list(row))  
            
                
            return depth_dict
        else:
            return ask_data,bid_data
   
        
    def create_cash_dict(self,mid_price,data_list):
        
        '''
        step: the initial percent.
        iteration: number of iteration.
        
        This function compute the total cash a given % away from the mid price. 
        '''
        cash_dict={}
        cash_dict[5] =0
        cash_dict[10] =0
        cash_dict[20] =0
        
        total_cash = 0
        
        for row in data_list:
            price = row[3]
            size = row[4]
            
            if price>0:
                cash = price*size
                total_cash = cash + total_cash
        
                price_ratio = abs((mid_price/price)-1.0)
                #print('mid_price: {}, price: {}, ratio: {}, cash: {}'.format(mid_price,price,price_ratio,cash))
                
                if price_ratio<=0.05:
                    cash_dict[5] = total_cash
                elif price_ratio<=0.1:
                    cash_dict[10]=total_cash
                elif price_ratio<=0.2:
                    cash_dict[20]=total_cash
                else:
                    break
            else:
                break
            
        previous_value= 0
        for index in cash_dict:
            value = cash_dict[index]
            if value<previous_value:
                cash_dict[index]=previous_value
            previous_value = value
                         
        return cash_dict
    
    
    def compute_cash_ratio(self,ask_cash,bid_cash):
        if bid_cash>=ask_cash:
            if ask_cash>0:
                ratio = (bid_cash-ask_cash)/ask_cash
            else:
                if bid_cash>0:
                    ratio = 10
                else:
                    ratio=0
            
            if ratio>10:
                ratio = 10
        
        if ask_cash>=bid_cash:
            if bid_cash>0:
                ratio =-(ask_cash-bid_cash)/bid_cash
            else:
                if ask_cash>0:
                    ratio = -10
                else:
                    ratio=0
            
            if ratio<-10:
                ratio = -10
                               
        return ratio
    
    
    def create_depth_summary(self,depth_dict):
        #temps = t.time()
        depth_summary={}
        for ticker in depth_dict:
            
            ticker_depth = depth_dict[ticker]
            highest_bid = ticker_depth['bid'][0][3]
            highest_ask = ticker_depth['ask'][0][3]
            
            if highest_bid ==0 or highest_ask ==0:
                pass
            else:
                spread = highest_ask/highest_bid
                mid_price = (highest_ask+highest_bid)/2
                #print('')
                #print('ask price')
                ask_cash_dict = self.create_cash_dict(mid_price,ticker_depth['ask'])
                #print('')
                #print('bid price')
                bid_cash_dict = self.create_cash_dict(mid_price,ticker_depth['bid'])
                
                depth_summary[ticker]={}
                #depth_summary[ticker]['ask_cash'] = ask_cash_dict
                #depth_summary[ticker]['bid_cash']=bid_cash_dict
                depth_summary[ticker]['spread']=spread
                depth_summary[ticker]['highest_bid'] = highest_bid
                depth_summary[ticker]['highest_ask'] = highest_ask
                depth_summary[ticker]['mid_price'] = mid_price
                
                for index in ask_cash_dict:
                    ask_cash = ask_cash_dict[index]
                    bid_cash = bid_cash_dict[index]
                    
                    ratio = self.compute_cash_ratio(ask_cash,bid_cash)
                        
                        
                    str_index = str(index)    
                    ask_index = 'ask_'+str_index
                    bid_index = 'bid_'+str_index
                    ratio_index = 'ratio_'+str_index
                    
                    depth_summary[ticker][ask_index] = ask_cash
                    depth_summary[ticker][bid_index] = bid_cash
                    depth_summary[ticker][ratio_index] = ratio
                    
                    
        #temps = t.time()-temps
        #print('tooks {} seconds'.format(temps))   
    
        return depth_summary 
    
 
    


def download_watchlist(data_struct='df'):
       
     con = sqlite3.connect('watchlist_database')
     cur = con.cursor()
     
     col = ['ticker','average_cash','active','db_name','invalid']
     
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
             active=row[2]
             db_name = row[3]
             invalid = row[4]
             
             if invalid==0:
                 watchlist[ticker]= {}
                 watchlist[ticker]['average_cash']=average_cash
                 watchlist[ticker]['active']=active
                 watchlist[ticker]['db_name']=db_name
                 watchlist[ticker]['invalid']=invalid
                 
     cur.close()
     con.close()
      
     
     return watchlist

"""
#watchlist = download_watchlist()    
#ticker_df = watchlist[:50]
#ticker_list = ticker_df['ticker'].to_list()

import depth_watchlist
dwl = depth_watchlist.depth_watchlist(active_liquid=False)
active_watchlist = dwl.get_active_watchlist(add_pk=False)


dd = depth_data()  

#initialize_database()

#row = ['GBTC',3,'AAA',9.0,1000]

#dd.update_row(row,'ASK')

ticker='BEAT'
ask_data,bid_data = dd.download_depth_data(ticker)


ticker_list =['BEAT','TOPS','WHLR','VLON','TRKA']
depth_dict = dd.download_depth_list(ticker_list)
depth_summary = dd.create_depth_summary(depth_dict)

for ticker in depth_summary:
    print(depth_summary[ticker]['mid_price'])

"""




#end 