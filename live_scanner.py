# -*- coding: utf-8 -*-
"""
Created on Fri Dec 10 11:25:40 2021

@author: marti
"""


import db_manager
import db_histo_manager as dhm
import live_object
import otc_object
import level2_object
import american_object
import prepare_live_data as pld

import date_manager
import math

from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5 import QtWidgets
from PyQt5.QtCore import *
from PyQt5 import QtCore
from PyQt5.QtWidgets import QMainWindow
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt, QPointF
from PyQt5.QtWidgets import QApplication, QMainWindow, QSystemTrayIcon, QMenu

from PyQt5.uic import loadUiType

import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

import sounddevice as sd
import soundfile as sf

import pandas as pd
import datetime
import numpy as np

import time as t
import sys, os

import live_ticker

import depth_data
import depth_watchlist


def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


FORM_CLASS,_=loadUiType(resource_path("live_scanner.ui"))



class scanner_window(QMainWindow, FORM_CLASS):
    def __init__(self,parent=None,candles=60,top_pick=15):
        
        temps = t.time()
        
        self.db_name='american_database'
        
        super(scanner_window,self).__init__(parent)
        self.setupUi(self)   
        
        self.bet_size = 500
        
        self.min_alternate_cash = 30000
        self.min_ema_cash = 7000
        self.min_cash390 = 700000
        self.alternate_pred_col ='pred_hl_ratio20'
        self.approx_pred_col='pred_hl_ratio20'
        self.liquid_type='liquid'
        

        self.green = QColor('#7fc97f')
        self.gold = QColor(255,215,0)
        self.red = QColor(240,86,88)
        self.white = QColor(255,255,255)
        
        self.red_css = "background-color:rgb(240,86,88)"
        self.green_css ="background-color:#7fc97f"
        self.gold_css ="background-color:rgb(255,215,0)"
        self.white_css ="background-color:rgb(255,255,255)"
        self.grey_css ="background-color:rgb(220,220,220)"
        
        self.high_pred_buttons = {}
        self.alternate_buttons = {}
        self.approx_buttons = {}
        self.stories_buttons = {}
        self.high_pred_mute_buttons = {}
        self.alternate_mute_buttons = {}
        self.approx_mute_buttons = {}
        self.muted_dict = {}
        
        self.scanner_col = ['ticker','ema13_cash','average_cash','green_line','close','volatility',
                            'pred_high20','hl_ratio','pred_hl_ratio20','pred_ng20','mute']
        self.scanner_col_decimal = ['0','0','2','4','2','2','2','2','2']
        self.headlines_col = ['date','ticker','headline']
        self.init_tables = False
        
        self.trades_days=1
        self.trade_table_col= ['buy_date','gains','profit','ticker','ema13_cash',
                               'pred_high20','hl_ratio20','pred_hl_ratio20','pred_ng20','type']
        self.trade_decimals = ['null','4','1','null','0','2','2','2','2','null']
        
        self.nbr_trades=1000
        self.candles = candles
        self.top_pick = top_pick
        
        self.approx_index = 0 # goes up to 18 (19 different value including the 0)
        self.approx_label_text ='APPROX 0'
        self.approx_dict = {}
        
        '''
        This section initialize the date. In the real system we will need 
        the current date instead.
        '''
        
      
        self.dbm = self.get_db_manager(db_name='live_database')
        
        self.dbm_histo = self.get_db_manager(db_name=self.db_name)
        self.trades_dbm = db_manager.db_manager(db_name='trades_database') 
        self.depth_dbm = self.get_db_manager(db_name='level2_database')
        
        self.dh  = date_manager.date_handler()
        self.previous_morning = self.dh.find_previous_morning()
        self.date_morning = self.dh.get_today_morning()
        self.first_candle = self.dh.get_today_first_candle()
        self.overnight_index = self.date_morning.replace(hour=8,minute=0)
        self.last_market_candle = self.dh.get_last_market_candle() 
        self.live_candle = self.get_live_candle()
        self.test_index = self.first_candle
        self.update_test_index()
        self.time_index = 0
        self.date_index = self.overnight_index
        # note the date index is properly set in the refill function if it start after the market are open
        #self.update_date_index() 
        
        self.scan_time='recent'
        
        self.previous_date_list = self.dh.get_specific_date_list(self.previous_morning)
        self.date_list = self.dh.get_specific_date_list(self.date_morning)
        
        self.watchlist = self.get_watchlist()
        
        self.overnight_init = False
        
        
        self.pred_dict = self.init_pred_dict() # the previous day is filled at this point. 
        self.set_date_label()
        
        self.table_col390 = ['ticker','date','close','ema13_cash','average_cash','range',
                             'pred_next_oc','pred_next_high','pred_hl_ratio']
        self.decimals_390m = ['4','0','0','2','2','2','2']
        self.df390 = self.get_df390()
        self.prepare_table_df390()
        self.show_390m_table()
        self.show_oc_pred_table()
        self.show_390m_range_table()
        
        self.daily_candles_df = self.download_daily_candles_df()
        
        self.gl_list = self.get_gl_list()
        
        self.Handle_Buttons()
        
        self.ticker_windows = {}
        
        
        '''
        Initialize the alarm
        '''
        self.sound_data, self.sound_fs = sf.read('cash_register_x.wav', dtype='float32')  
        self.coin_flip_sound,self.coin_flip_fs = sf.read('coin_flip.wav', dtype='float32') 
        #sd.play(self.coin_flip_sound,self.coin_flip_fs)
        self.alarm_headline = True
        
        sd.play(self.sound_data, self.sound_fs)
        self.alarm = True
        self.alarm_percent = 2
        
        self.alarm_approx_fired = False
        self.alarm_approx = True
        self.alarm_percent_approx = 2
        
        self.alarm_volatility = False
        self.alarm_percent_volatility = 2
            
    
        '''
        If the market is already open show the current predictions
        '''
        if self.test_index>self.first_candle:
            print('updating table')
            self.show_predictions()
        
        
        '''
        initialize the depth watchlist. Note only the live scanner can write
        to the depth watchlist once depth feed has been started. 
        '''
        
        self.dwl = depth_watchlist.depth_watchlist(active_liquid=True)
        self.active_watchlist = self.dwl.get_active_watchlist(add_pk=False)
        self.active_ticker_list = self.create_ticker_list(self.active_watchlist)
        
        '''
        The depth feed save the dept from the watchlist to the depth database.
        The depth data object is used to download the depth data. 
        '''
        self.dd = depth_data.depth_data()  
        self.depth_dict = self.dd.download_depth_list(self.active_ticker_list)
        self.depth_summary = self.dd.create_depth_summary(self.depth_dict)
        self.create_depth_summary_features()
        
        l2 = level2_object.level2_object()
        self.l2_col = l2.depth_col
        self.l2_col.remove('ticker')
        
        '''
        Download the latest news headlines
        '''
        
        self.headlines = self.get_headlines()
        self.show_headlines()
        self.show_stories_headlines()
        
        '''
        Initialize the qtimer
        '''
        
        self.timer=QTimer()
        self.timer.timeout.connect(self.time_management)
        self.timer.start(200)
        
        temps = t.time()-temps
        print('initializing the interface tooks {} seconds'.format(temps))
        
        
        
    def get_headlines(self):
       
        #date_dict contains the begin date and end date only.
        begin_date = self.last_market_candle
        #begin_date = self.previous_morning
        end_date = self.date_morning + datetime.timedelta(hours = 23)
        date_dict = {}
        date_dict['begin_date']=begin_date
        date_dict['end_date']=end_date
        
        headlines = self.dbm_histo.download_all_headlines(date_dict,data_struct='df')
        headlines['date'] = pd.to_datetime(headlines['date'])
        #headlines = headlines.loc[headlines['stock_number']==1]
        headlines = headlines.sort_values(by=['date'],ascending=False)
        
        cleaned_headlines = self.clean_headlines(headlines)
        
        return cleaned_headlines
    
    
    def clean_headlines(self,headlines):

        index_list = []
        index=0
        for ind,row in headlines.iterrows():
            ticker = row['ticker']
            if ticker in self.pred_dict[self.last_market_candle]:
                index_list.append(index)
                #print(ticker)
            index = index+1    
            
        new_headlines = headlines.iloc[index_list]
        
        return new_headlines
        
    
    def update_headlines(self,length=30):
        headlines = self.get_headlines()
        self.headlines = headlines[:length]
      
    
    def update_depth_data(self):
        self.active_watchlist = self.dwl.get_active_watchlist(add_pk=False)
        self.active_ticker_list = self.create_ticker_list(self.active_watchlist)
        
        self.depth_dict = self.dd.download_depth_list(self.active_ticker_list)
        self.depth_summary = self.dd.create_depth_summary(self.depth_dict)
        self.create_depth_summary_features()
        
    
    def create_depth_summary_features(self):
        
        '''
        Compute the depth cash ratio. The value is in seconds
        '''
        
    
        for ticker in self.depth_summary:
            depth_ticker = self.depth_summary[ticker]
            
            '''
            The overnight candle is only created after 9:30 and we 
            cant be sure which one has been initialized
            '''
            if self.date_index >= self.first_candle:
                current_candle = self.get_current_row()            
                ema_cash = current_candle[ticker]['ema13_cash']
                depth_ticker['ema13_cash']=ema_cash
                # only aiming for liquid stock
                if ema_cash<10000:
                    ema_cash=10000
            else:
                ema_cash=30000
            
            cash_ratio5 = 60*(depth_ticker['bid_5']-depth_ticker['ask_5'])/ema_cash
            depth_ticker['cash_ratio5']= int(cash_ratio5)
            
            cash_ratio10=60*(depth_ticker['bid_10']-depth_ticker['ask_10'])/ema_cash
            depth_ticker['cash_ratio10']=int(cash_ratio10)
            
            cash_ratio20= 60*(depth_ticker['bid_20']-depth_ticker['ask_20'])/ema_cash
            depth_ticker['cash_ratio20']= int(cash_ratio20)
            
            #print(self.depth_summary[ticker])
            
    
    def create_depth_df(self,depth_summary):
        today = datetime.datetime.today()
        d = today.replace(second=0,microsecond=0)      
        #dh  = date_manager.date_handler()           
        date_index = self.dh.remove_1_minute(d)    
        for ticker in depth_summary:
            depth_summary[ticker]['date']=date_index        
                
        #l2 = level2_object.level2_object()
        #l2_col = l2.depth_col
        #l2_col.remove('ticker')
            
        df = pd.DataFrame.from_dict(depth_summary,columns=self.l2_col,orient='index')        
        df.index.name ='ticker' 
        
        return df 
    
    
    def save_depth_summary(self):
        depth_df = self.create_depth_df(self.depth_summary) 
        table='DEPTH_SUMMARY'
        self.depth_dbm.save_multiple_ticker(depth_df, table)
            
            
        
    def get_db_manager(self,db_name):
    
        if db_name=='otc_database':
            data_shape = otc_object.otc_object()
        elif db_name=='live_database':
            data_shape = live_object.live_object()
        elif db_name=='level2_database':
           data_shape = level2_object.level2_object()
        elif db_name=='american_database':
           data_shape = american_object.american_object()
            
        dbm = dhm.db_manager(db_name,data_shape)
        
        return dbm
        
    
    def create_depth_watchlist(self):
        '''
        The depth watchlist is created from two different part. The first part
        being the top 15 most volatile stocks at the moment. The second part, is
        from the ticker that we are currently watching. This is done in order to
        speed up the downloading. 
        
        This functions is called every minute. 
        '''
        
        # This is the list of the 30 most active stocks from the previous day.
        top_x_liquid = self.dwl.get_top_x_liquid()
        
        ticker_list = self.find_top_volatile_ticker()
        window_ticker_list = self.create_ticker_list(self.ticker_windows)
        
        ticker_dict={}
         
        for ticker in top_x_liquid:
            ticker_dict[ticker]=True
            
        for ticker in ticker_list:
            ticker_dict[ticker]=True
            
        for ticker in window_ticker_list:
            ticker_dict[ticker]=True
            
        return ticker_dict
    
    
    def update_depth_watchlist(self):
        # the list of ticker that we want to have access
        # to the level 2 data.
        new_watchlist = self.create_depth_watchlist()
        
        # the old list of ticker.
        self.active_watchlist = self.dwl.get_active_watchlist(add_pk=False)
        
        added_ticker,removed_ticker = self.update_ticker_list(new_watchlist,self.active_watchlist)
        
        self.dwl.activate_ticker_list(added_ticker)
        self.dwl.deactivate_ticker_list(removed_ticker)
        
        print('Watching the depth of {} stocks'.format(len(new_watchlist)))
    
        
        
    def update_ticker_list(self,new_watchlist,old_watchlist): 
        '''
        This function finds the added and removed ticker from the new list. It
        will be used to update the depth watchlist. 
        '''
        
        added_ticker =[]
        removed_ticker = []
        
        for ticker in new_watchlist:
            if ticker not in old_watchlist:
                added_ticker.append(ticker)
                
        for ticker in old_watchlist:
            if ticker not in new_watchlist:
                removed_ticker.append(ticker)
                
        return added_ticker,removed_ticker
        
        
        
    def find_top_volatile_ticker(self,nbr_stocks=15): 
        
        if self.date_index >= self.first_candle:
        
            pred_col = 'volatility'
            current_row = self.get_current_row()
            ordered_ticker = self.find_top_picks(current_row, pred_col)
            
            if len(ordered_ticker)>nbr_stocks:
                ticker_list = ordered_ticker[:nbr_stocks]
            else:
                ticker_list = ordered_ticker
                
            return ticker_list
        else:
            return []
        
         
      
     
    def create_ticker_list(self,ticker_dict):
        ticker_list =[]
        for ticker in ticker_dict:
            ticker_list.append(ticker)
            
        return ticker_list
    
    
    def sound_alarm(self,ticker):
        
        if ticker not in self.muted_dict:
            sd.play(self.sound_data, self.sound_fs)
        else:
            print('no alarm for {}'.format(ticker))
    
        
    def update_time_index(self):
        
        if self.date_index >=self.first_candle:
            self.time_index = self.date_list.index(self.date_index)
        
   
    def update_muted_dict(self):
        
        unmute_list = []
        for ticker in self.muted_dict:
            remaining_count = self.muted_dict[ticker]
            
            if remaining_count<=0:
                unmute_list.append(ticker)
            else:
                self.muted_dict[ticker] = remaining_count-1 # removing 1 minute from the count.
    
        for ticker in unmute_list:
            del self.muted_dict[ticker]
            print('{} is unmuted'.format(ticker))
            
            
        
    def mute_ticker(self,y,pred_col,origin='high_pred'):
        if origin =='high_pred':
            current_row = self.get_current_row()
            ordered_ticker = self.find_top_picks(current_row, pred_col)
        elif origin=='volatility':
            if self.scan_time =='overnight':
                current_row = self.get_overnight_row()
            else:
                current_row = self.get_current_row()
            pred_col = self.alternate_pred_col
            if self.liquid_type=='liquid':
                ordered_ticker = self.find_top_picks(current_row, pred_col)
            else:
                ordered_ticker = self.find_top_picks(current_row,pred_col,remove_illiquid=False)
        elif origin=='approx':
            
            current_row = self.get_approx_row()
            if self.liquid_type=='liquid':
                ordered_ticker = self.find_top_picks(current_row, pred_col)
            else:
                ordered_ticker = self.find_top_picks(current_row,pred_col,remove_illiquid=False)
        
        length = len(ordered_ticker)       
        
        if length>0:
            ticker = ordered_ticker[y]
            
            if ticker in self.muted_dict:
                muted = False
                del self.muted_dict[ticker]
                text = 'manually unmuted {}'.format(ticker)
                print(text)
                self.set_system_msg(text)
            else:
                muted=True
                self.muted_dict[ticker]=5 # mute the ticker for 5 minute. 
                
                text = 'muted {} for 5 minute'.format(ticker)
                print(text)
                self.set_system_msg(text)
                
            
            if origin=='high_pred':
                if muted:
                    self.high_pred_mute_buttons[y].setStyleSheet(self.red_css)
                else:
                    self.high_pred_mute_buttons[y].setStyleSheet(self.grey_css)
            elif origin=='volatility':
                if muted:
                    self.alternate_mute_buttons[y].setStyleSheet(self.red_css)
                else:
                    self.alternate_mute_buttons[y].setStyleSheet(self.grey_css)
            elif origin=='approx':
                if muted:
                    self.approx_mute_buttons[y].setStyleSheet(self.red_css)
                else:
                    self.approx_mute_buttons[y].setStyleSheet(self.grey_css)
        else:
            print('Could not find: y: {} pred_col: {} origin: {}'.format(y,pred_col,origin))
    
        
    def open_ticker_window(self,y,pred_col,origin='high_pred'):
        temps = t.time()
        
        
        if origin =='high_pred':
            current_row = self.get_current_row()
            self.high_pred_buttons[y].setStyleSheet(self.grey_css)
            ordered_ticker = self.find_top_picks(current_row, pred_col)
        elif origin=='volatility':
            if self.scan_time =='overnight':
                current_row = self.get_overnight_row()
            else:
                current_row = self.get_current_row()
            pred_col = self.alternate_pred_col
            self.alternate_buttons[y].setStyleSheet(self.grey_css)
            if self.liquid_type=='liquid':
                ordered_ticker = self.find_top_picks(current_row, pred_col)
            else:
                ordered_ticker = self.find_top_picks(current_row,pred_col,remove_illiquid=False)
        elif origin=='approx':
            #pred_col = 'hl_ratio'
            self.approx_buttons[y].setStyleSheet(self.grey_css)
            current_row = self.get_approx_row()
            if self.liquid_type=='liquid':
                ordered_ticker = self.find_top_picks(current_row, pred_col)
            else:
                ordered_ticker = self.find_top_picks(current_row,pred_col,remove_illiquid=False)
        
        length = len(ordered_ticker)       
        
        if length>0:
            ticker = ordered_ticker[y]
            if ticker not in self.ticker_windows:
                ticker_dict = self.get_ticker_dict(ticker)
                candle_390,cash_390 = self.get_daily_candles(ticker)
                
                self.dwl.activate_ticker(ticker)
                self.update_depth_data()
                depth_ticker = self.depth_dict[ticker]
                if ticker in self.depth_summary:
                    depth_ticker_summary = self.depth_summary[ticker]
                else:
                    depth_ticker_summary = {}
                
                window = live_ticker.ticker_window(ticker,ticker_dict,self.time_index,self.date_list,self.previous_date_list,candle_390,cash_390,self.bet_size,depth_ticker,depth_ticker_summary)
                window.show()
                window.init_pred_table()
                window.init_depth_table()
                window.show_depth_data()
                window.init_tick_table()
                self.ticker_windows[ticker]=window
            
                self.print_windows_info()
            else:
                if self.ticker_windows[ticker].isVisible():
                  
                    text = '{} is already open!!!'.format(ticker)
                    print(text)
                    self.set_system_msg(text)
                    
                else:
                    
                    ticker_dict = self.get_ticker_dict(ticker)
                    candle_390,cash_390 = self.get_daily_candles(ticker)
                    
                    self.dwl.activate_ticker(ticker)
                    self.update_depth_data()
                    depth_ticker = self.depth_dict[ticker]
                    
                    if ticker in self.depth_summary:
                        depth_ticker_summary = self.depth_summary[ticker]
                    else:
                        depth_ticker_summary = {}
                        
                    window = live_ticker.ticker_window(ticker,ticker_dict,self.time_index,self.date_list,self.previous_date_list,candle_390,cash_390,self.bet_size,depth_ticker,depth_ticker_summary)
                    window.show()
                    window.init_pred_table()
                    window.init_depth_table()
                    window.show_depth_data()
                    window.init_tick_table()
                    self.ticker_windows[ticker]=window
            
                    self.print_windows_info()
        else:
         
            text = 'The approximation are not ready yet'
            print(text)
            self.set_system_msg(text)
                  
        self.update_windows_reference()
        
        if origin=='approx':
            self.update_ticker_approximation(open_window=True)
        
        temps = t.time()-temps
        print('opening a new window tooks {} seconds'.format(temps))
        
        
        
        
    def open_window(self,ticker):
        
        if self.live_candle>self.first_candle:
            t_index = self.time_index
        else:
            t_index=-1
            
        if ticker in self.pred_dict[self.last_market_candle]:
            if ticker not in self.ticker_windows:
                if t_index==-1:
                    ticker_dict = self.get_ticker_dict(ticker,remove_overnight=True)
                else:
                    ticker_dict = self.get_ticker_dict(ticker)
                candle_390,cash_390 = self.get_daily_candles(ticker)
                
                self.dwl.activate_ticker(ticker)
                self.update_depth_data()
                depth_ticker = self.depth_dict[ticker]
                
                if ticker in self.depth_summary:
                    depth_ticker_summary = self.depth_summary[ticker]
                else:
                    depth_ticker_summary = {}
                    
                window = live_ticker.ticker_window(ticker,ticker_dict,t_index,self.date_list,self.previous_date_list,candle_390,cash_390,self.bet_size,depth_ticker,depth_ticker_summary)
                window.show()
                window.init_pred_table()
                window.init_depth_table()
                window.show_depth_data()
                window.init_tick_table()
                self.ticker_windows[ticker]=window
            
                self.print_windows_info()
            else:
                if self.ticker_windows[ticker].isVisible():
       
                    text = '{} is already open'.format(ticker)
                    print(text)
                    self.set_system_msg(text)
                else:
                    ticker_dict = self.get_ticker_dict(ticker)
                    candle_390,cash_390 = self.get_daily_candles(ticker)
                    
                    self.dwl.activate_ticker(ticker)
                    self.update_depth_data()
                    depth_ticker = self.depth_dict[ticker]
                    if ticker in self.depth_summary:
                        depth_ticker_summary = self.depth_summary[ticker]
                    else:
                        depth_ticker_summary = {}
                    window = live_ticker.ticker_window(ticker,ticker_dict,t_index,self.date_list,self.previous_date_list,candle_390,cash_390,self.bet_size,depth_ticker,depth_ticker_summary)
                    window.show()
                    window.init_pred_table()
                    window.init_depth_table()
                    window.show_depth_data()
                    window.init_tick_table()
                    self.ticker_windows[ticker]=window
            
                    self.print_windows_info()
                      
            self.update_windows_reference()
        else:
            
            text = '{} is not in our watchlist'.format(ticker)
            print(text)
            self.set_system_msg(text)
        
        
    def open_new_window(self):
        text = self.open_edit.text()
        ticker = text.upper()
        
        if ticker =='':
            text = 'No ticker was provided'
            print(text)
            self.set_system_msg(text)
        else:
            
            if ticker in self.pred_dict[self.last_market_candle]:
                self.open_window(ticker)
            else:
                
                text = '{} is not in our dataaset'.format(ticker)
                print(text)
                self.set_system_msg(text)
        
        
    def print_windows_info(self):
        print('there is now {} active windows'.format(len(self.ticker_windows)))
        
        for ticker in self.ticker_windows:
            visible = self.ticker_windows[ticker].isVisible()
            
            
    def update_windows_reference(self):
        
        destroy_tickers = []
        for ticker in self.ticker_windows:
            visible = self.ticker_windows[ticker].isVisible()
            if visible==False:
                destroy_tickers.append(ticker)
                
        if len(destroy_tickers)>0:
            for ticker in destroy_tickers:
                del self.ticker_windows[ticker]
                print('killed {} window'.format(ticker))
                
            print('there is now {} active windows'.format(len(self.ticker_windows)))
            
    
    def update_ticker_window(self):
        if len(self.ticker_windows)>0:
            pred_row = self.pred_dict[self.date_index]
            
            self.update_depth_data()
            for ticker in self.ticker_windows:
                data = pred_row[ticker]
                
                self.ticker_windows[ticker].update_data(self.time_index,data)
                depth_ticker = self.depth_dict[ticker]
                if ticker in self.depth_summary:
                    depth_ticker_summary = self.depth_summary[ticker]
                else:
                    depth_ticker_summary = {}
                self.ticker_windows[ticker].update_depth_data(self.time_index,depth_ticker,depth_ticker_summary)
                
    
    def update_ticker_approximation(self,open_window=False):
        if len(self.ticker_windows)>0:
            
            self.update_depth_data()
            
            pred_row = self.approx_dict['APPROX_1M']
            for ticker in self.ticker_windows:
                if ticker in pred_row:
                    data = pred_row[ticker]
                    self.ticker_windows[ticker].update_data((self.time_index+1),data,self.approx_label_text)
                    
                depth_ticker = self.depth_dict[ticker]
                if ticker in self.depth_summary:
                    depth_ticker_summary = self.depth_summary[ticker]
                else:
                    depth_ticker_summary = {}
                self.ticker_windows[ticker].update_depth_data((self.time_index),depth_ticker,depth_ticker_summary,self.approx_label_text)
                
    
    
    def get_df390(self):
        
        table='FEATURES_390M'
        df390 = self.dbm.download_specific_day(self.previous_morning,table,cond_list=[],data_struct='df')
        
        df390['hl_ratio'] = df390['pred_next_high']+df390['pred_next_low']-1
        
      
        return df390
    
    
    def show_390m_table(self):
        
        table1 = self.df390
        table2 = table1.loc[table1['average_cash']>=self.min_cash390]
        #table2.sort_values(by='hl_ratio',ascending=False,inplace=True)
        table3 = table2.sort_values(by='pred_hl_ratio',ascending=False)
        table_df390 = table3[:self.top_pick]
        
        self.pred_table_390m.setRowCount(0) 
        
        y = 0
        for index,row in table_df390.iterrows():
            x = 0
            self.pred_table_390m.insertRow(y)
            
            for col in self.table_col390:
                
                if col=='ticker':
                    btn = QPushButton(self.pred_table_390m)
                   
                    v= str(row[col])
                    btn.setText(v)
                    btn.clicked.connect(lambda ch, v=v: self.open_window(v))
                    #self.high_pred_buttons[y]=btn
                    self.pred_table_390m.setCellWidget(y,x,btn)
                elif (col=='date'):
                    value = str(row[col])
                    self.pred_table_390m.setItem(y,x,QTableWidgetItem(value))
                else:
                    value = row[col]
                    dec = self.decimals_390m[x-2]
                    text = "{:."+dec+"f}"
                    value = text.format(value)
                    self.pred_table_390m.setItem(y,x,QTableWidgetItem(value))
                
                if col=='pred_hl_ratio':
                    if row[col]>=4:
                        item = self.pred_table_390m.item(y,x)
                        item.setBackground(self.green)
                    if row[col]<=-4:
                        item = self.pred_table_390m.item(y,x)
                        item.setBackground(self.red)
                
                x = x+1
                
            y = y+1
            
        self.pred_table_390m.resizeColumnsToContents()
        
        
        
    
    def show_390m_range_table(self):
        
        table1 = self.df390
        table2 = table1.loc[table1['average_cash']>=self.min_cash390]
        #table2.sort_values(by='hl_ratio',ascending=False,inplace=True)
        table3 = table2.sort_values(by='range',ascending=False)
        table_df390 = table3[:self.top_pick]
        
        self.pred_range_table.setRowCount(0) 
        
        y = 0
        for index,row in table_df390.iterrows():
            x = 0
            self.pred_range_table.insertRow(y)
            
            for col in self.table_col390:
                
                if col=='ticker':
                    btn = QPushButton(self.pred_range_table)
                   
                    v= str(row[col])
                    btn.setText(v)
                    btn.clicked.connect(lambda ch, v=v: self.open_window(v))
                    #self.high_pred_buttons[y]=btn
                    self.pred_range_table.setCellWidget(y,x,btn)
                elif (col=='date'):
                    value = str(row[col])
                    self.pred_range_table.setItem(y,x,QTableWidgetItem(value))
                else:
                    value = row[col]
                    dec = self.decimals_390m[x-2]
                    text = "{:."+dec+"f}"
                    value = text.format(value)
                    self.pred_range_table.setItem(y,x,QTableWidgetItem(value))
                
                if col=='pred_hl_ratio':
                    if row[col]>=4:
                        item = self.pred_range_table.item(y,x)
                        item.setBackground(self.green)
                    if row[col]<=-4:
                        item = self.pred_range_table.item(y,x)
                        item.setBackground(self.red)
                
                x = x+1
                
            y = y+1
            
        self.pred_range_table.resizeColumnsToContents()


        
    def show_oc_pred_table(self):
        
        table1 = self.df390
        table2 = table1.loc[table1['average_cash']>=self.min_cash390]
        #table2.sort_values(by='hl_ratio',ascending=False,inplace=True)
        table3 = table2.sort_values(by='pred_next_oc',ascending=False)
        oc_390 = table3[:self.top_pick]
        
        self.pred_oc_table.setRowCount(0) 
        
        y = 0
        for index,row in oc_390.iterrows():
            x = 0
            self.pred_oc_table.insertRow(y)
            
            for col in self.table_col390:
                
                if col=='ticker':
                    btn = QPushButton(self.pred_oc_table)
                    v= str(row[col])
                    btn.setText(v)
                    btn.clicked.connect(lambda ch, v=v: self.open_window(v))
                    #self.high_pred_buttons[y]=btn
                    self.pred_oc_table.setCellWidget(y,x,btn)
                elif (col=='date'):
                    value = str(row[col])
                    self.pred_oc_table.setItem(y,x,QTableWidgetItem(value))
                else:
                    value = row[col]
                    dec = self.decimals_390m[x-2]
                    text = "{:."+dec+"f}"
                    value = text.format(value)
                    self.pred_oc_table.setItem(y,x,QTableWidgetItem(value))
                
                if col=='pred_next_oc':
                    if row[col]>=4:
                        item = self.pred_oc_table.item(y,x)
                        item.setBackground(self.green)
                    if row[col]<=-4:
                        item = self.pred_oc_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='pred_hl_ratio':
                    if row[col]>=4:
                        item = self.pred_oc_table.item(y,x)
                        item.setBackground(self.green)
                    if row[col]<=-4:
                        item = self.pred_oc_table.item(y,x)
                        item.setBackground(self.red)
                
                x = x+1
                
            y = y+1
            
        self.pred_oc_table.resizeColumnsToContents()
    
    
    def Handle_Buttons(self):
        
        self.nbr_bets.clicked.connect(self.set_nbr_trades)
        self.trades_days_button.clicked.connect(self.set_trades_days)
        self.bet_size_button.clicked.connect(self.set_bet_size)
        
        self.open_button.clicked.connect(self.open_new_window)
        self.refresh_button.clicked.connect(self.refresh_trade)
        
        self.scan_type.currentIndexChanged.connect(self.change_scan)
        self.scan_type_approx.currentIndexChanged.connect(self.change_scan_approx)
        self.liquid_scan.currentIndexChanged.connect(self.change_liquid)
        self.scan_period.currentIndexChanged.connect(self.change_scan_period)
        
        self.alarm_state.currentIndexChanged.connect(self.activate_alarm)
        self.alarm_threshold.currentIndexChanged.connect(self.change_alarm_threshold)
        
        self.alarm_state_approx.currentIndexChanged.connect(self.activate_alarm_approx)
        self.alarm_threshold_approx.currentIndexChanged.connect(self.change_alarm_threshold_approx)
    
        self.alarm_state_volatility.currentIndexChanged.connect(self.activate_alarm_volatility)
        self.alarm_threshold_volatility.currentIndexChanged.connect(self.change_alarm_threshold_volatility)
    
        
    def activate_alarm(self):
        alarm_state = self.alarm_state.currentText()
        
        if alarm_state =='alarm_on':
            self.alarm=True
            
            text = 'Alarm state is on'
            print(text)
            self.set_system_msg(text)
        else:
            text = 'Alarm state is off'
            print(text)
            self.set_system_msg(text)
            
            self.alarm=False
            
    
    def change_alarm_threshold(self):
        percent_str = self.alarm_threshold.currentText()
        self.alarm_percent = float(percent_str[0])
        
        text = 'The alarm percent is: {}, and the alarm state is: {}'.format(self.alarm_percent,self.alarm)
        print(text)
        self.set_system_msg(text)
        
        
    def activate_alarm_approx(self):
        alarm_state_approx = self.alarm_state_approx.currentText()
        
        if alarm_state_approx =='alarm_on':
            text = 'Approx Alarm is on'
            print(text)
            self.set_system_msg(text)
            self.alarm_approx=True
        else:
            text = 'Approx Alarm is off'
            print(text)
            self.set_system_msg(text)
            self.alarm_approx=False
            
    
    def change_alarm_threshold_approx(self):
        percent_str = self.alarm_threshold_approx.currentText()
        self.alarm_percent_approx = float(percent_str[0])
     
        text = 'The alarm percent is: {}, and the alarm state is: {}'.format(self.alarm_percent_approx,self.alarm_approx)
        print(text)
        self.set_system_msg(text)    
        
    def activate_alarm_volatility(self):
        alarm_state_volatility = self.alarm_state_volatility.currentText()
        
        if alarm_state_volatility =='alarm_on':
            text = 'volatility alarm ON'
            print(text)
            self.set_system_msg(text)   
            self.alarm_volatility=True
        else:
            text = 'volatility alarm OFF'
            print(text)
            self.set_system_msg(text)
            self.alarm_volatility=False
            
    
    def change_alarm_threshold_volatility(self):
        percent_str = self.alarm_threshold_volatility.currentText()
        
        first_digit = float(percent_str[0])
        second_digit = percent_str[1]
        
        if second_digit=='%':
            self.alarm_percent_volatility = float(percent_str[0])
        else:
            second_digit = float(second_digit)
            combined_digit = (first_digit*10)+second_digit
            self.alarm_percent_volatility = combined_digit
        
        text = 'The volatility alarm percent is: {}, and the alarm state is: {}'.format(self.alarm_percent_volatility,self.alarm_volatility)
        print(text)
        self.set_system_msg(text)
        
        
        
        
    def change_scan(self):
        self.alternate_pred_col = self.scan_type.currentText()
        self.init_alternate_table()
        
        text='the scan was changed to: {}'.format(self.alternate_pred_col)
        print(text)
        self.set_system_msg(text)
        
    def change_scan_approx(self):
        self.approx_pred_col = self.scan_type_approx.currentText()
        
        text='the scan was changed to: {}'.format(self.approx_pred_col)
        print(text)
        self.set_system_msg(text)
        
        
    def change_scan_period(self):
        self.scan_time = self.scan_period.currentText()
        self.init_alternate_table()
        
        text='the scan time was changed to: {}'.format(self.scan_time)
        print(text)
        self.set_system_msg(text)
        
        
    def change_liquid(self):
        self.liquid_type = self.liquid_scan.currentText()
        self.init_alternate_table()
        
        text='the scan was changed to: {}'.format(self.liquid_type)
        print(text)
        self.set_system_msg(text)
       
    
    
    def update_pred_dict(self):
        self.pred_dict[self.date_index]=self.get_pred_row()
        
        
    def update_approx_dict(self):
        table = 'APPROX_1M'
        self.approx_dict[table] = self.get_approx_row()
        
    
    def update_overnight_row(self):
        self.pred_dict[self.overnight_index] = self.get_overnight_row()
        print('overnight contain: {} data rows'.format(len(self.pred_dict[self.overnight_index])))
        
        
    def get_current_row(self):
        current_row = self.pred_dict[self.date_index]
        return current_row
    
    
    def get_current_approx_row(self):
        table = 'APPROX_1M'
        approx_row = self.approx_dict[table]
        return approx_row
        
    
    def show_headlines(self):
        self.headlines_table.setRowCount(0) 
        current_date = self.date_index
        self.alarm_headline=True
        
        y = 0
        for index,row in self.headlines.iterrows():
            
            ticker = str(row['ticker'])
            headline_date = row['date']
            
            delta = current_date-headline_date
            
            seconds = delta.total_seconds()
            
            if ticker not in self.pred_dict[self.last_market_candle]:
                color = self.red
            elif seconds <=100:
                color = self.gold
                
                if self.alarm_headline and (self.date_index>=self.first_candle):
                    sd.play(self.coin_flip_sound,self.coin_flip_fs)
                    self.alarm_headline=False
            elif (seconds >100): # testing if the news was overnight
                #overnight_delta = headline_date-self.previous_close_index
                overnight_delta = headline_date-self.last_market_candle
                sec = overnight_delta.total_seconds()
      
                if sec>=0:
                    color = self.green
                else:
                    color = self.white
            else:
                color = self.white
            
            
            x = 0
            
            self.headlines_table.insertRow(y)
            
            for col in self.headlines_col:
        
                if col=='ticker':
                    
                    btn = QPushButton(self.headlines_table)
                    
                    btn.setText(ticker)
                    
                    btn.clicked.connect(lambda ch, ticker=ticker: self.open_window(ticker))
                    self.headlines_table.setCellWidget(y,x,btn)
                    
                else:
                    
                    
                    value = str(row[col])
                    self.headlines_table.setItem(y,x,QTableWidgetItem(value))
                    item = self.headlines_table.item(y,x)
                    item.setBackground(color)
                    
                x = x+1
                
             
            y = y+1
            
        self.headlines_table.resizeColumnsToContents()  
    
    
        
    def show_stories_headlines(self):
        '''
        Similar function as show_headlines. But it add a button on each row. 
        '''
        
        self.stories_table.setRowCount(0) 
        current_date = self.date_index
        
        y = 0
        for index,row in self.headlines.iterrows():
            
            ticker = str(row['ticker'])
            headline_date = row['date']
            
            delta = current_date-headline_date
            
            seconds = delta.total_seconds()
            
            if ticker not in self.pred_dict[self.last_market_candle]:
                color = self.red
            elif seconds <=600:
                color = self.gold
            
            elif (seconds >600): # testing if the news was overnight
                overnight_delta = headline_date-self.last_market_candle
                sec = overnight_delta.total_seconds()
               
                if sec>=0:
                    color = self.green
                else:
                    color = self.white
            else:
                color = self.white
            
            
            x = 1 # since the first row is for the button.
            self.stories_table.insertRow(y)
            for col in self.headlines_col:
                value = str(row[col])
                
                self.stories_table.setItem(y,x,QTableWidgetItem(value))
                
                item = self.stories_table.item(y,x)
                item.setBackground(color)
                
                x = x+1
                
                
            #Create the open story button
            btn = QPushButton(self.stories_table)
            btn.setText('open')
            btn.clicked.connect(lambda ch, y=y: self.show_story(y))
            self.stories_table.setCellWidget(y,0,btn)
                
            y = y+1
            
        self.stories_table.resizeColumnsToContents() 
        
        
    def show_story(self,i):
      
        temps = t.time()
        last_headline = self.headlines.iloc[i]
        
        if len(last_headline)>0:
            story_id = last_headline['story_id']
            
            story_data = self.dbm_histo.download_a_story(story_id)
            
            if len(story_data)>0:
                story = story_data.iloc[0]['story']
            else:
                story = 'There was no story' 
        else:
            story = 'There was no story' 
        
        font = QtGui.QFont()
        font.setPointSize(12)
        self.story_window.setCurrentFont(font)
        self.story_window.setPlainText(story)
        
        temps = t.time()-temps
        print('getting a story took {} seconds'.format(temps))
        
       
    def init_pred_table(self):
        self.init_high_pred()
        self.init_alternate_table()
        
        
    def show_predictions(self):
        self.init_pred_table()
        
        
    
    def init_high_pred(self):
        #pred_col = 'pred_high20'
        pred_col = 'volatility'
        
        current_row = self.get_current_row()
        ordered_ticker = self.find_top_picks(current_row, pred_col)
        
        self.pred_high_table.setRowCount(0)
       
        for y in range(len(ordered_ticker)):
            x=0
            self.pred_high_table.insertRow(y)
            
            ticker_data = current_row[ordered_ticker[y]]
            
            
            if self.alarm_volatility and y ==0:
               highest_value = ticker_data[pred_col]
               if highest_value >= self.alarm_percent_volatility:
                   ticker = ticker_data['ticker']
                   self.sound_alarm(ticker)
            
            
            for col in self.scanner_col:
                if col=='green_line':
                    current_close = ticker_data['close']
                    ticker = ticker_data['ticker']
                    
                    if ticker in self.pred_dict[self.last_market_candle]:
                        last_close = self.pred_dict[self.last_market_candle][ticker]['close']
                    else:
                        last_close=current_close
                        
                    green_line = current_close/last_close
                    value = (green_line*100)-100
                elif col=='mute':
                    value = 'mute'
                else:
                    value = ticker_data[col]
                    
                if col =='ticker':
                    #Create the open ticker window button
                    btn = QPushButton(self.pred_high_table)
                    v= value
                    btn.setText(v)
                    #btn.setStyleSheet(self.green_css)
                    btn.clicked.connect(lambda ch, y=y,p=pred_col: self.open_ticker_window(y,p))
                    self.high_pred_buttons[y]=btn
                    self.high_pred_buttons[y].setStyleSheet(self.grey_css)
                    self.pred_high_table.setCellWidget(y,x,btn)
                    #self.pred_high_table.setItem(y,x,QTableWidgetItem(value))
                elif col =='mute':
                    btn = QPushButton(self.pred_high_table)
                    v= value
                    btn.setText(v)
                    #btn.setStyleSheet(self.green_css)
                    btn.clicked.connect(lambda ch, y=y,p=pred_col: self.mute_ticker(y,p))
                    self.high_pred_mute_buttons[y]=btn
                    
                    ticker = ticker_data['ticker']
                    if ticker in self.muted_dict:
                        self.high_pred_mute_buttons[y].setStyleSheet(self.red_css)
                    else:
                        self.high_pred_mute_buttons[y].setStyleSheet(self.grey_css)
                    self.pred_high_table.setCellWidget(y,x,btn)
                else:
                    
                    dec = self.scanner_col_decimal[x-1]
                    text = "{:."+dec+"f}"
                    value_str = text.format(value)
                    self.pred_high_table.setItem(y,x,QTableWidgetItem(value_str))
                
                if col==pred_col:
                    if value>=4:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                        ticker = ticker_data['ticker']
                        if ticker not in self.ticker_windows:
                            self.high_pred_buttons[y].setStyleSheet(self.green_css)
                    if value<=-4:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                        
                elif col=='green_line':
                    if value>=10:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-10:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                        
                elif col=='pred_hl_ratio20':   
                    if value>=4:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='pred_ng20':   
                    if value>=4:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='hl_ratio':   
                    if value>=4:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='gains':   
                    if value>=3:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-3:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                       
               
                x = x + 1
                
        self.pred_high_table.resizeColumnsToContents()
        
        
    def show_high_pred(self):
        #pred_col = 'pred_high20'
        pred_col = 'volatility'
        current_row = self.get_current_row()
        ordered_ticker = self.find_top_picks(current_row, pred_col)
        
        for y in range(len(ordered_ticker)):
            x=0
            
            ticker_data = current_row[ordered_ticker[y]]
            for col in self.scanner_col:
                if col=='green_line':
                    current_close = ticker_data['close']
                    ticker = ticker_data['ticker']
                    last_close = self.pred_dict[self.last_market_candle][ticker]['close']
                    green_line = current_close/last_close
                    value = (green_line*100)-100
                else:
                    value = ticker_data[col]
                    
                if col =='ticker':
                    #Create the open ticker window button
                    #btn = QPushButton(self.pred_high_table)
                    
                    self.high_pred_buttons[y].setText(value)
                    self.high_pred_buttons[y].setStyleSheet(self.grey_css)
                  
                else:
                    
                    dec = self.scanner_col_decimal[x-1]
                    text = "{:."+dec+"f}"
                    value_str = text.format(value)
                    self.pred_high_table.setItem(y,x,QTableWidgetItem(value_str))
                
                if col==pred_col:
                   if value>=4:
                       item = self.pred_high_table.item(y,x)
                       item.setBackground(self.green)
                       ticker = ticker_data['ticker']
                       if ticker not in self.ticker_windows:
                           self.high_pred_buttons[y].setStyleSheet(self.green_css)
                       
                   elif value<=-4:
                       item = self.pred_high_table.item(y,x)
                       item.setBackground(self.red)
                       
                elif col=='green_line':
                    if value>=10:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-10:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                        
                elif col=='pred_hl_ratio20':   
                    if value>=4:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='pred_ng20':   
                    if value>=4:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                        
                elif col=='hl_ratio':   
                    if value>=4:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='gains':   
                    if value>=3:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-3:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='volatility':
                    if value>=10:
                        item = self.pred_high_table.item(y,x)
                        item.setBackground(self.green) 
                       
                x = x + 1
                
        #self.pred_high_table.resizeColumnsToContents()
        
    
    def init_approx_table(self):
        #pred_col = 'volatility'
        
        pred_col = self.approx_pred_col
        
        current_row = self.get_current_approx_row()
        
        if self.liquid_type=='liquid':
            ordered_ticker = self.find_top_picks(current_row, pred_col)
        else:
            ordered_ticker = self.find_top_picks(current_row,pred_col,remove_illiquid=False)
        
        self.approx_table.setRowCount(0)
        
        for y in range(len(ordered_ticker)):
            x=0
            self.approx_table.insertRow(y)
            
            ticker_data = current_row[ordered_ticker[y]]
            current_close = ticker_data['close']
            
            if self.alarm_approx and y ==0:
                highest_value = ticker_data[pred_col]
                if highest_value >= self.alarm_percent_approx:
                    if self.alarm_approx_fired==False and self.approx_index>10:
                        self.alarm_approx_fired=True
                        ticker = ticker_data['ticker']
                        self.sound_alarm(ticker)
            
            
            if current_close is not None:
                for col in self.scanner_col:
                    if col=='green_line':
                        ticker = ticker_data['ticker']
                        
                        if ticker in self.pred_dict[self.last_market_candle]:
                            last_close = self.pred_dict[self.last_market_candle][ticker]['close']
                        else:
                            last_close = current_close
                            
                        green_line = current_close/last_close
                        value = (green_line*100)-100
                    elif col=='mute':
                        value = 'mute'
                    else:
                        value = ticker_data[col]
                        
                    if value is not None:
                        if col =='ticker':
                            btn = QPushButton(self.approx_table)
                            v= value
                            btn.setText(v)
                            btn.clicked.connect(lambda ch, y=y,p=pred_col,v='approx': self.open_ticker_window(y,p,v))
                            self.approx_buttons[y]=btn
                            self.approx_buttons[y].setStyleSheet(self.grey_css)
                            self.approx_table.setCellWidget(y,x,btn)
                        elif col =='mute':
                            btn = QPushButton(self.approx_table)
                            v= value
                            btn.setText(v)
                            #btn.setStyleSheet(self.green_css)
                            btn.clicked.connect(lambda ch, y=y,p=pred_col,v='approx': self.mute_ticker(y,p,v))
                            self.approx_mute_buttons[y]=btn
                            
                            ticker = ticker_data['ticker']
                            if ticker in self.muted_dict:
                                self.approx_mute_buttons[y].setStyleSheet(self.red_css)
                            else:
                                self.approx_mute_buttons[y].setStyleSheet(self.grey_css)
                            self.approx_table.setCellWidget(y,x,btn)   
                        else:
                            
                            dec = self.scanner_col_decimal[x-1]
                            text = "{:."+dec+"f}"
                            value_str = text.format(value)
                            self.approx_table.setItem(y,x,QTableWidgetItem(value_str))
                            
                            
                        if col=='hl_ratio':
                            
                            if value>=4:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.gold) 
                                ticker = ticker_data['ticker']
                                if ticker not in self.ticker_windows:
                                    self.approx_buttons[y].setStyleSheet(self.green_css)
                            elif value>=2:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.green)
                            elif value<=-2:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.red)
                        elif col=='pred_hl_ratio20':
                            
                            if value>=4:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.gold) 
                                ticker = ticker_data['ticker']
                                if ticker not in self.ticker_windows:
                                    self.approx_buttons[y].setStyleSheet(self.green_css)
                            elif value>=2:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.green)
                            elif value<=-2:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.red)
                        elif col=='pred_ng20':
                            
                            if value>=4:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.gold) 
                                ticker = ticker_data['ticker']
                                if ticker not in self.ticker_windows:
                                    self.approx_buttons[y].setStyleSheet(self.green_css)
                            elif value>=2:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.green)
                            elif value<=-2:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.red)
                        elif col=='green_line':
                            if value>=10:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.green)
                            elif value<=-10:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.red)        
                      
                        elif col=='gains':
                            if value>=3:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.green) 
                            elif value<=-3:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.red) 
                        elif col=='volatility':
                            if value>=10:
                                item = self.approx_table.item(y,x)
                                item.setBackground(self.green) 
                    else:
                        self.approx_table.setItem(y,x,QTableWidgetItem('None'))        
                        
                    x = x + 1
                
        self.approx_table.resizeColumnsToContents()    
    
    
    def init_alternate_table(self):
        
        pred_col = self.alternate_pred_col
        scan_time = self.scan_time
        
        if scan_time =='overnight':
            current_row=self.get_overnight_row()   
        else:
            current_row = self.get_current_row()
            
        if self.liquid_type=='liquid':
            ordered_ticker = self.find_top_picks(current_row, pred_col)
        else:
            ordered_ticker = self.find_top_picks(current_row,pred_col,remove_illiquid=False)
        
        self.alternate_table.setRowCount(0)
        
        for y in range(len(ordered_ticker)):
            x=0
            self.alternate_table.insertRow(y)
            
            ticker_data = current_row[ordered_ticker[y]]
            
            if self.alarm and y ==0:
                highest_value = ticker_data[pred_col]
                if highest_value >= self.alarm_percent:
                    ticker = ticker_data['ticker']
                    self.sound_alarm(ticker)
            
            for col in self.scanner_col:
                if col=='green_line':
                    current_close = ticker_data['close']
                    ticker = ticker_data['ticker']
                    
                    if ticker in self.pred_dict[self.last_market_candle]:
                        last_close = self.pred_dict[self.last_market_candle][ticker]['close']
                    else:
                        last_close = current_close
                        
                    green_line = current_close/last_close
                    value = (green_line*100)-100
                elif col=='mute':
                    value = 'mute'
                else:
                    value = ticker_data[col]
                    
                    
                if col =='ticker':
                    btn = QPushButton(self.alternate_table)
                    v= value
                    btn.setText(v)
                    btn.clicked.connect(lambda ch, y=y,p=pred_col,v='volatility': self.open_ticker_window(y,p,v))
                    self.alternate_buttons[y]=btn
                    self.alternate_buttons[y].setStyleSheet(self.grey_css)
                    self.alternate_table.setCellWidget(y,x,btn)
                elif col =='mute':
                    btn = QPushButton(self.alternate_table)
                    v= value
                    btn.setText(v)
                    #btn.setStyleSheet(self.green_css)
                    btn.clicked.connect(lambda ch, y=y,p=pred_col,v='volatility': self.mute_ticker(y,p,v))
                    self.alternate_mute_buttons[y]=btn
                    
                    ticker = ticker_data['ticker']
                    if ticker in self.muted_dict:
                        self.alternate_mute_buttons[y].setStyleSheet(self.red_css)
                    else:
                        self.alternate_mute_buttons[y].setStyleSheet(self.grey_css)
                    self.alternate_table.setCellWidget(y,x,btn)      
                else:
                    if value is not None:
                        dec = self.scanner_col_decimal[x-1]
                        text = "{:."+dec+"f}"
                        value_str = text.format(value)
                        self.alternate_table.setItem(y,x,QTableWidgetItem(value_str))
                    else:
                        self.alternate_table.setItem(y,x,QTableWidgetItem('None'))
                    
                if col=='hl_ratio':
                    
                    if value>=5:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.gold) 
                    elif value>=2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red)
                        
                elif col=='pred_hl_ratio20':   
                    if value>=4:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='pred_ng20':   
                    if value>=4:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='green_line':
                    if value>=10:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-10:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red)        
                
                elif col=='gains':
                    if value>=3:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green) 
                    elif value<=-3:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red) 
                elif col=='volatility':
                    if value>=10:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green) 
                        ticker = ticker_data['ticker']
                        if ticker not in self.ticker_windows:
                            self.alternate_buttons[y].setStyleSheet(self.green_css)
                    
                x = x + 1
            
        self.alternate_table.resizeColumnsToContents()
        
        
    def show_alternate_table(self):
        #pred_col = 'volatility'
        pred_col = self.alternate_pred_col
        current_row = self.get_current_row()
        
        if self.liquid_type=='liquid':
            ordered_ticker = self.find_top_picks(current_row, pred_col)
        else:
            ordered_ticker = self.find_top_picks(current_row,pred_col,remove_illiquid=False)
        
        
        for y in range(len(ordered_ticker)):
            x=0
            
            ticker_data = current_row[ordered_ticker[y]]
            for col in self.scanner_col:
                if col=='green_line':
                    current_close = ticker_data['close']
                    ticker = ticker_data['ticker']
                    last_close = self.pred_dict[self.last_market_candle][ticker]['close']
                    green_line = current_close/last_close
                    value = (green_line*100)-100
                else:
                    value = ticker_data[col]
                if col =='ticker':
                    self.alternate_buttons[y].setText(value) 
                    self.alternate_buttons[y].setStyleSheet(self.grey_css)
                else:
                    if value is not None:
                        dec = self.scanner_col_decimal[x-1]
                        text = "{:."+dec+"f}"
                        value_str = text.format(value)
                        self.alternate_table.setItem(y,x,QTableWidgetItem(value_str))
                    else:
                        self.alternate_table.setItem(y,x,QTableWidgetItem('None'))
                
                if col=='gains':
                    if value>=3:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green) 
                    elif value<=-3:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='green_line':
                    if value>=10:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-10:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='pred_hl_ratio20':   
                    if value>=4:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='pred_ng20':   
                    if value>=4:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.gold)
                    elif value>=2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red)
                elif col=='hl_ratio':
                    if value>=5:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.gold) 
                    elif value>=2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green)
                    elif value<=-2:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.red) 
                elif col=='volatility':
                    if value>=10:
                        item = self.alternate_table.item(y,x)
                        item.setBackground(self.green) 
                        ticker = ticker_data['ticker']
                        if ticker not in self.ticker_windows:
                            self.alternate_buttons[y].setStyleSheet(self.green_css)
               
                x = x + 1
                
       
             
    
    def set_date_label(self):
        self.date_label.setText(str(self.date_index))
        
    def set_approx_label(self,ready=True):
        if ready:
            self.approx_label_text = 'APPROX '+str(self.approx_index)
            self.approx_label.setText(self.approx_label_text)
        else:
            self.approx_label.setText('Not Ready')
            
            
    def set_system_msg(self,text):
        
        minute = self.date_index.minute
        if minute<10:
            str_minute = '0'+str(minute)
        else:
            str_minute = str(minute)
            
        hour_str = str(self.date_index.hour)
        
        msg = hour_str+':'+str_minute+' '+text
        
        self.system_msg.setText(msg) 
        
    
    def modify_pred_dict(self,pred_dict):
        
        for d in pred_dict:
            for ticker in pred_dict[d]:
                pred_high = pred_dict[d][ticker]['pred_high20']
                pred_low = pred_dict[d][ticker]['pred_low20']
                
                if pred_high is not None: 
                    value =pred_high - pred_low
                    volatility = (value*100)
                    pred_dict[d][ticker]['volatility'] = volatility
                else:
                    pred_dict[d][ticker]['hl_ratio'] = float('NaN')
                    pred_dict[d][ticker]['volatility'] = float('NaN')
                
        pred_col = ['gains','pred_high20','pred_low20','hl_ratio','pred_hl_ratio20','pred_ng20']
        
        for d in pred_dict:
            for ticker in pred_dict[d]:
                for col in pred_col:
                    value = pred_dict[d][ticker][col]
                    if value is not None:
                        value = (value*100)-100
                        pred_dict[d][ticker][col]=value
     
                        
    def add_previous_day_time(self,pred_dict):

        time_index = -391
        for d in self.previous_date_list:
            for ticker in pred_dict[d]:
                pred_dict[d][ticker]['time'] = time_index
                
            time_index = time_index + 1
        
        
    def init_pred_dict(self):
        
        '''
        This functions create the previous day price dict. Then fill the remaining available data.
        '''
        
        temps = t.time()
            
        pred_dict = pld.create_specific_live_price_dict(self.previous_morning,self.watchlist,self.db_name)
        
        
        temps = t.time()-temps
        print('creating the prediction dict tooks {} seconds'.format(temps))   
        
        self.add_previous_day_time(pred_dict)
        
        
        if self.test_index>self.first_candle:
            self.overnight_init=True
            self.refill_pred_dict(pred_dict)
    
            
        self.modify_pred_dict(pred_dict)
        
        return pred_dict
    
    
    def prepare_pred_row(self,ticker_dict,overnight=False):
        #temps = t.time()
        
        for ticker in ticker_dict:
            pred_high = ticker_dict[ticker]['pred_high20']
            pred_low = ticker_dict[ticker]['pred_low20']
            
            if pred_high is not None:
                volatility = (pred_high-pred_low)*100
                ticker_dict[ticker]['volatility']=volatility
            else:
                ticker_dict[ticker]['hl_ratio'] = float('NaN')
                ticker_dict[ticker]['volatility']=float('NaN')
                
        #pred_col = ['gains','pred_high15','pred_low15','hl_ratio']
        pred_col = ['gains','pred_high20','pred_low20','hl_ratio','pred_hl_ratio20','pred_ng20']
        
        
        for ticker in ticker_dict:
            for col in pred_col:
                value = ticker_dict[ticker][col]
                if value is not None:
                    value = (value*100)-100
                    ticker_dict[ticker][col]=value
                    
                if overnight:
                    ticker_dict[ticker]['time']=0
                else:
                    ticker_dict[ticker]['time']=self.time_index
        
        #temps = t.time()-temps
    
        
        
    def prepare_approx_row(self,ticker_dict):
        #temps = t.time()
        
        for ticker in ticker_dict:
            pred_high = ticker_dict[ticker]['pred_high20']
            pred_low = ticker_dict[ticker]['pred_low20']
            
            if pred_high is not None:
                volatility = (pred_high-pred_low)*100
                ticker_dict[ticker]['volatility']=volatility
            else:
                ticker_dict[ticker]['hl_ratio'] = float('NaN')
                ticker_dict[ticker]['volatility']=float('NaN')
                
        #pred_col = ['gains','pred_high15','pred_low15','hl_ratio']
        pred_col = ['gains','pred_high20','pred_low20','hl_ratio','pred_hl_ratio20','pred_ng20']
        
        
        for ticker in ticker_dict:
            for col in pred_col:
                value = ticker_dict[ticker][col]
                if value is not None:
                    value = (value*100)-100
                    ticker_dict[ticker][col]=value
                    
                ticker_dict[ticker]['time']=self.time_index+1


    def get_pred_row(self):

        ticker_dict = self.dbm.download_minute_candle(self.date_index,table='FEATURES_1M',cond_list=[],data_struct='dict')
       
        if len(ticker_dict)==0:
            print('!!!')
            print(self.date_index)
            print('ticker dict was empty second try!!!!')
            print('!!!')
            t.sleep(0.8)
            ticker_dict = self.dbm.download_minute_candle(self.date_index,table='FEATURES_1M',cond_list=[],data_struct='dict')
       
            
        if len(ticker_dict)==0:
            print('!!!')
            print(self.date_index)
            print('ticker dict was empty third try!!!!')
            print('!!!')
            t.sleep(0.8)
            ticker_dict = self.dbm.download_minute_candle(self.date_index,table='FEATURES_1M',cond_list=[],data_struct='dict')
       
        if len(ticker_dict)==0:
            print('')
            print('')
            raise Exception('EXCEPTION: ticker dict was empty!!!')
       
        self.prepare_pred_row(ticker_dict)
        
        return ticker_dict
    
    
    def get_approx_row(self):
        
        table='APPROX_1M'
        ticker_dict = self.dbm.download_minute_candle(self.test_index,table=table,cond_list=[],data_struct='dict')
        
        #ticker_dict = self.dbm.download_all_approximation(table,self.test_index,data_struct='dict') 
        self.prepare_approx_row(ticker_dict)
        
        return ticker_dict
    
    
    def show_approx_390(self):
        
        table='APPROX_390'
        self.df390 = self.dbm.download_minute_candle(self.date_index,table=table,cond_list=[],data_struct='df')
        self.df390['ticker']= self.df390.index
       
        self.prepare_table_df390()
        self.show_390m_table()
        self.show_oc_pred_table()
        self.show_390m_range_table()
    
    
    def get_overnight_row(self):
        table='FEATURES_1M'
        ticker_dict = self.dbm.download_minute_candle(self.overnight_index,table=table,cond_list=[],data_struct='dict')
        
        #ticker_dict = self.dbm.download_all_specific_candle(candle_size=1, candle_date=self.overnight_index,data_struct='dict')
        self.prepare_pred_row(ticker_dict,overnight=True)
        
        return ticker_dict
    
    
    
    def get_ticker_dict(self,ticker,remove_overnight=False):
    
        ticker_dict = {}
        
        
        for d in self.pred_dict:
            try: 
                if remove_overnight:
                    if d == self.overnight_index:
                        break
                
                
                ticker_dict[d] = self.pred_dict[d][ticker]
        
            except:
                length_ticker_dict = len(self.pred_dict[d])
                length_pred_dict = len(self.pred_dict)
                print('**************************************')
                print('**************************************')
                print(d)
                print('ticker dict: {} rows, pred_dict: {} rows,index: {}'.format(length_ticker_dict,length_pred_dict,d))
                print('**************************************')
                print('**************************************')
        
        return ticker_dict
     
        
    def prepare_table_df390(self):
        pred_col390 = ['pred_next_gain','pred_next_oc','pred_next_high','pred_next_low','pred_hl_ratio']
            
        for p in pred_col390:
            self.df390[p] = (self.df390[p]*100)-100
            
        self.df390['range']=self.df390['pred_next_high']-self.df390['pred_next_low']
        
    
    """   
    def prepare_oc_table(self):
        pred_col390 = ['pred_next_gain','pred_next_oc','pred_next_high','pred_next_low','pred_hl_ratio']
        for p in pred_col390:
            self.oc_390[p] = (self.oc_390[p]*100)-100
              
        self.oc_390['range']=self.oc_390['pred_next_high']-self.oc_390['pred_next_low']
    """
      
        
    def get_green_line(self,ticker):
        
        if ticker in self.pred_dict[self.last_market_candle]:
            previous_close = self.pred_dict[self.last_market_candle][ticker]['close']
            current_close = self.pred_dict[self.date_index][ticker]['close']
            gl = current_close/previous_close
            gl = (gl*100)-100
            
            return gl
        else:
            return -999
        
    
    def find_top_picks(self,ticker_dict,col,remove_illiquid=True):
        #temps = t.time()
        
        high_list = []
        high_ticker = []
        
        minimum = 0
        min_index=0
        
        first=True
        for ticker in ticker_dict:
            if col=='green_line':
                value = self.get_green_line(ticker)
            else:
                value = ticker_dict[ticker][col]
            
            if remove_illiquid:
                ema_cash = ticker_dict[ticker]['ema13_cash']
                avg_cash = ticker_dict[ticker]['average_cash']
                if (value is not None) and (ema_cash is not None):
                    if math.isnan(value)==False:
                        
                        if (ema_cash >= self.min_ema_cash) and (avg_cash>0):
                            if len(high_list)<self.top_pick:
                                high_list.append(value)
                                high_ticker.append(ticker)
                            else:
                                if first:
                                    first=False
                                    minimum,min_index = self.find_min_info(high_list)
                                
                                if value > minimum:
                                    high_list[min_index]=value
                                    high_ticker[min_index]=ticker
                                    minimum,min_index = self.find_min_info(high_list)
                        else:
                            
                            if avg_cash >= self.min_alternate_cash:
                                if len(high_list)<self.top_pick:
                                    high_list.append(value)
                                    high_ticker.append(ticker)
                                else:
                                    if first:
                                        first=False
                                        minimum,min_index = self.find_min_info(high_list)
                                    
                                    if value > minimum:
                                        high_list[min_index]=value
                                        high_ticker[min_index]=ticker
                                        minimum,min_index = self.find_min_info(high_list)  
            else:
                  
                if value is not None:
                    if math.isnan(value)==False:
                        if len(high_list)<self.top_pick:
                            high_list.append(value)
                            high_ticker.append(ticker)
                        else:
                            if first:
                                first=False
                                minimum,min_index = self.find_min_info(high_list)
                            
                            if value > minimum:
                                high_list[min_index]=value
                                high_ticker[min_index]=ticker
                                minimum,min_index = self.find_min_info(high_list) 
    
        ordered_ticker = []
        
        for x in range(len(high_ticker)):
            maximum,max_index = self.find_max_info(high_list)
            ordered_ticker.append(high_ticker[max_index])
            
            del high_ticker[max_index]
            del high_list[max_index]
    
       
        
        return ordered_ticker 
    
    
    def get_gl_list(self):
        
        table='FEATURES_390M'
        stock_list = self.dbm_histo.download_specific_day(self.previous_morning,table,cond_list=[],data_struct='df')
        stock_list.sort_values(['average_cash'],inplace=True)
        stock_list = stock_list[-50:]
        gl_list = stock_list['ticker'].to_list()
        
        return gl_list
    
    
    def compute_otc_gl(self):
        length = 0
        otc_gl = 0
        for ticker in self.gl_list:
            if ticker in self.pred_dict[self.last_market_candle]:
                previous_close = self.pred_dict[self.last_market_candle][ticker]['close']
                current_close = self.pred_dict[self.date_index][ticker]['close']
                green_line = current_close/previous_close
                
                otc_gl = green_line + otc_gl
                length = length + 1
            
        otc_gl = otc_gl/length
        otc_gl = (otc_gl*100)-100
        
        return otc_gl
    
    
    def approx_otc_gl(self):
        '''
        Same function as compute otc gl but the current close is from the level 2
        data instead of the candle data. Which enable us to compute the otc gl
        before the market open
        '''
        
        length = 0
        otc_gl = 0
        for ticker in self.depth_summary:
            if ticker in self.pred_dict[self.last_market_candle]:
                previous_close = self.pred_dict[self.last_market_candle][ticker]['close']
                current_close = self.depth_summary[ticker]['mid_price']
                green_line = current_close/previous_close
                
                otc_gl = green_line + otc_gl
                length = length + 1
            
        otc_gl = otc_gl/length
        otc_gl = (otc_gl*100)-100
        
        return otc_gl
    
    
    def show_otc_data(self):
        self.otc_gl = self.compute_otc_gl()
        green_line_value = self.otc_gl
        
        text = "{:.2f}"
        green_line = text.format(green_line_value) + '%'
        
        self.otc_green_line.setPlainText(green_line)
         
        
        if green_line_value>=1:
            p = self.otc_green_line.viewport().palette()
            p.setColor(self.otc_green_line.viewport().backgroundRole(), self.green)
            self.otc_green_line.viewport().setPalette(p)
        elif green_line_value<=-1:
            p = self.otc_green_line.viewport().palette()
            p.setColor(self.otc_green_line.viewport().backgroundRole(), self.red)
            self.otc_green_line.viewport().setPalette(p)
        else:
            p = self.otc_green_line.viewport().palette()
            p.setColor(self.otc_green_line.viewport().backgroundRole(), self.white)
            self.otc_green_line.viewport().setPalette(p)
            
    
    def show_approx_otc_data(self):
        green_line_value = self.approx_otc_gl()
        
        text = "{:.2f}"
        green_line = text.format(green_line_value) + '%'
        
        self.approx_otc_green_line.setPlainText(green_line)
         
        if green_line_value>=1:
            p = self.approx_otc_green_line.viewport().palette()
            p.setColor(self.approx_otc_green_line.viewport().backgroundRole(), self.green)
            self.approx_otc_green_line.viewport().setPalette(p)
        elif green_line_value<=-1:
            p = self.approx_otc_green_line.viewport().palette()
            p.setColor(self.approx_otc_green_line.viewport().backgroundRole(), self.red)
            self.approx_otc_green_line.viewport().setPalette(p)
        else:
            p = self.approx_otc_green_line.viewport().palette()
            p.setColor(self.approx_otc_green_line.viewport().backgroundRole(), self.white)
            self.approx_otc_green_line.viewport().setPalette(p)
    
        
   
    def set_bet_size(self):
        bet_size = self.bet_size_edit.text()
        
        if bet_size =='':
            text = 'the bet size wasnt provided'
           
        else:
            self.bet_size = int(bet_size)
            
            text = 'the bet size was changed to: {}'.format(bet_size)
            
        print(text)
        self.set_system_msg(text)
   
    
    def set_nbr_trades(self):
           nbr_bet = self.nbr_bet_edit.text()
           
           if nbr_bet =='':
               print('the number of bet wasnt provided')
           else:
               self.nbr_trades = int(nbr_bet)
               self.refresh_trade()
               
               
    def set_trades_days(self):
        nbr_bet = self.trades_days_edit.text()
        
        if nbr_bet =='':
            print('the number of days wasnt provided')
        else:
            self.trades_days = int(nbr_bet)
            self.refresh_trade()
      
            
    def refresh_trade(self):
         
        trades_df = self.trades_dbm.download_all_trades()
        
        trades_df = self.remove_extra_days(trades_df)
        
        if len(trades_df)>self.nbr_trades:
            trades_df = trades_df[-self.nbr_trades:]
        
        
        self.fill_trades_table(trades_df)
        self.set_trades_labels(trades_df)
        
        self.fill_active_trades_table()
       
          
    def set_trades_labels(self,trades_df):
        
        if len(trades_df)>0:
            avg_profit = trades_df['profit'].mean()
            avg_profit = "{:.2f}".format(avg_profit)
            
            percent_profit = (trades_df['gains'].mean()-1)*100
            percent_profit = "{:.2f}".format(percent_profit)
            
            number_bets = len(trades_df)
            number_bets = "{:.0f}".format(number_bets)
            
            avg_bet_size = trades_df['bet_size'].mean()
            avg_bet_size = "{:.2f}".format(avg_bet_size)
        else:
            avg_profit = '0'
            percent_profit='0'
            number_bets='0'
            avg_bet_size='0'  
            
        self.avg_profit.setText(avg_profit)
        self.percent_profit.setText(percent_profit)
        self.number_bets.setText(number_bets)
        self.avg_bet_size.setText(avg_bet_size)
       
        
        
    def fill_trades_table(self,trades_df):
        index = len(trades_df)-1
        
        y=0
        
        self.trades_table.setRowCount(0)
        for i in range(len(trades_df)):
            
            x=0
            
            row = trades_df.iloc[index-i]
            
            self.trades_table.insertRow(y)
            for col in self.trade_table_col:
                
                value = row[col]
                dec = self.trade_decimals[x]
                
                if dec !='null':
                    text = "{:."+dec+"f}"
                    value = text.format(value)
                self.trades_table.setItem(y,x,QTableWidgetItem(value))
                
                x = x+1
        
            y = y + 1 
        
        self.trades_table.resizeColumnsToContents()
        
        
    def fill_active_trades_table(self):
        
        self.active_trade_table.setRowCount(0)
        y=0
        for ticker in self.ticker_windows:
            
            trade = self.ticker_windows[ticker].get_active_trade()
            if len(trade)>0:
                ticker = trade[0]['ticker']
                buy_date = trade[0]['buy_date']
                buy_price = trade[0]['buy_price']
                bet_size = trade[0]['bet_size']
                hl_ratio = trade[0]['hl_ratio']
                
                actual_price = self.pred_dict[self.date_index][ticker]['close']
                actual_gain = actual_price/buy_price
                actual_profit = (actual_gain-1)*bet_size
            
                self.active_trade_table.insertRow(y)
                
                value = ticker
                self.active_trade_table.setItem(y,0,QTableWidgetItem(value))
                
                value = buy_date
                self.active_trade_table.setItem(y,1,QTableWidgetItem(value))
                
                text = "{:.4f}"
                value = text.format(buy_price)
                self.active_trade_table.setItem(y,2,QTableWidgetItem(value))
                
                text = "{:.0f}"
                value = text.format(bet_size)
                self.active_trade_table.setItem(y,3,QTableWidgetItem(value))
                
                text = "{:.2f}"
                value = text.format(hl_ratio)
                self.active_trade_table.setItem(y,4,QTableWidgetItem(value))
                
                text = "{:.2f}"
                actual_gain = (actual_gain*100)-100
                value = text.format(actual_gain)
                self.active_trade_table.setItem(y,5,QTableWidgetItem(value))
                
                text = "{:.0f}"
                value = text.format(actual_profit)
                self.active_trade_table.setItem(y,6,QTableWidgetItem(value))
                
                y = y + 1
        
        self.active_trade_table.resizeColumnsToContents()
            
    def remove_extra_days(self,trades_df):
        
        trades_df.sort_index(ascending=False,inplace=True)
        d=0
        count = 0
        index = 0
        for i,row in trades_df.iterrows():
            sell_date = row['sell_date']
            sell_date = datetime.datetime.strptime(sell_date, '%Y-%m-%d %H:%M:%S')
            
            test_day = sell_date.day
            
            #print(sell_date)
            if test_day != d:
                count = count + 1
                d = test_day
            
            if count > self.trades_days:
                break
            
            index = index+1
            
        trades_df.sort_index(ascending=True,inplace=True)
        trades_df = trades_df[-index:]
        #print(len(trades_df))
        
        return trades_df
    
        
    def get_daily_candles(self,ticker):
        ticker_df = self.daily_candles_df.loc[self.daily_candles_df['ticker']==ticker].copy()
        ticker_df.set_index('date',inplace=True)
        ticker_df.sort_index(inplace=True)
        
        time_list = []
        start = -len(ticker_df)+1
        for x in range(len(ticker_df)):
            time_list.append(start+x)
        
        
        ticker_df['time']=time_list
        
        col = ['time','open','close','low','high']
        temp_df = ticker_df[col]
        candle_390 = temp_df.values.tolist()
        
        col = ['time','average_cash']
        temp_df = ticker_df[col]
        cash_390 = temp_df.values.tolist()
        
        return candle_390,cash_390
        
        
        
    def find_min_info(self,high_list):
        minimum = 9999
        index = 0
        min_index = 0
        for v in high_list:
            if v<minimum:
                minimum = v
                min_index = index
            
            index = index+1
            
        return minimum,min_index
    
    
    def find_max_info(self,high_list):
        maximum = -9999
        index = 0
        max_index = 0
        for v in high_list:
            if v>maximum:
                maximum = v
                max_index = index
            
            index = index+1
            
        return maximum,max_index
            
    
    def get_watchlist(self):
        #dbm = db_manager.db_manager(db_name='iq_database') 
    
        #watchlist = dbm.today_watchlist()
        #watchlist = watchlist.index.to_list()
        
        watchlist = pld.create_watchlist(self.db_name,data_struct='list')
        
        return watchlist
    
    
    def refill_pred_dict(self,pred_dict):
        '''
        This function assumes the pred dict only have the previous day data. 
        '''  
        #candle_size=1
        
        for candle_date in self.date_list:
            if candle_date<self.test_index:
                ticker_dict = self.dbm.download_minute_candle(candle_date,table='FEATURES_1M',cond_list=[],data_struct='dict')
                pred_dict[candle_date]=ticker_dict
                #pred_dict[candle_date] = self.dbm.download_multiple_specific_candle(candle_size,self.watchlist,candle_date,data_struct='dict')
                self.date_index= candle_date 
                self.update_time_index()
                
                for ticker in pred_dict[candle_date]:
                    pred_dict[candle_date][ticker]['time'] = self.time_index
                    
                    
    def download_daily_candles_df(self):
        temp_df = self.dbm_histo.download_all_ticker_list(self.watchlist)
        col = ['date','open','close','low','high','average_cash','ticker']
        daily_candles_df = temp_df[col]
        
        return daily_candles_df
        
                
    
    def update_test_index(self):
        today = datetime.datetime.today()
        self.test_index = today.replace(second=0,microsecond=0)
    
    def get_live_candle(self):
        live_candle = datetime.datetime.today()
        return live_candle
    
    
    def update_live_candle(self):
        self.live_candle = datetime.datetime.today()
        
                    
    def update_date_index(self):
        
        if self.test_index >= self.first_candle:
            self.date_index = self.test_index
        
       
        
        
    def test_date_index(self):
        
        '''
        Checking if the candle is completed and letting 1 second pass before download the
        predictions
        '''
        td = self.live_candle-self.test_index
        
        if td.seconds>=61:
            return True
        else:
            return False
        
        
    def next_candle(self):
        temps = t.time()
        self.update_windows_reference()
        if self.time_index<=390:
            
            self.update_pred_dict()
            
            self.show_predictions()
            
            if self.db_name=='otc_database':
                self.show_otc_data()  
                self.show_approx_otc_data()
            
            self.update_ticker_window()
            
            self.refresh_trade()
        
        temps = t.time()-temps
        temps = round(temps,3)
        
        di = '{}:{}'.format(self.date_index.hour,self.date_index.minute)
        ti = '{}:{}'.format(self.test_index.hour,self.test_index.minute)
        print('Compute: {} secs , DI: {}, TI: {}'.format(temps,di,ti))
        
        
        
    def next_approximation(self):
        #temps = t.time()
        self.update_windows_reference()
        if self.time_index<=390:
            
            self.set_approx_label()
            
            self.update_approx_dict()
            
            self.init_approx_table()
            
            self.update_ticker_approximation()
            
        #temps = t.time()-temps
        #print('preparing the approximation data took {} second'.format(temps))
                    
            
    def time_management(self):
        
        # test if new minute data is available
        self.update_live_candle()
        test_date = self.test_date_index()
        if test_date:
            
            self.alarm_approx_fired=False
            
            self.approx_index = 0
            self.update_date_index()
            self.set_date_label() 
            self.update_test_index()
            self.update_time_index()
            
            if self.overnight_init==False:
                if self.date_index >= self.first_candle:
                    print('PULLING THE OVERNIGHT DATA !!!!')
                    t.sleep(1)
                    self.overnight_init=True
                    self.update_overnight_row()
                
            if self.date_index >= self.first_candle:  
                self.next_candle()
                
            if self.live_candle.hour>=15:
                '''
                Pull and show daily approximation
                '''
                if self.live_candle.minute>1:
                    self.show_approx_390()
            
            self.update_depth_watchlist()
            
            self.update_muted_dict()
            
            test_temps = t.time() 
            self.update_headlines()
            self.show_headlines()
            self.show_stories_headlines()
            test_temps = t.time()-test_temps
            print('Updating the headlines tooks {} seconds'.format(test_temps))
            
            """
            if self.date_index >= self.first_candle:
                test_temps = t.time()
                self.save_depth_summary()
                test_temps = t.time()-test_temps
                print('saving the depth summary tooks {} seconds'.format(test_temps))
            """
           
        else:
            
            if self.db_name=='otc_database':
                self.show_approx_otc_data()
            
            second = int(self.live_candle.second)
            if second>=3:
                if second>self.approx_index:
                    self.approx_index = second
                    self.next_approximation()
                
            else:
                self.set_approx_label(ready=False)
               
            if self.test_index == self.first_candle:        
                if self.approx_index<=5:
                    self.update_overnight_row()
                    self.init_alternate_table()
                    t.sleep(1)
                    
        
                    
                
  

def main():
    
    app = QApplication(sys.argv)
    
    trayIcon = QSystemTrayIcon(QIcon('freedom.png'),parent=app)
    trayIcon.setToolTip('test freedom')
    trayIcon.show()
    
    window = scanner_window()
    window.show()
    #window.init_pred_table()
    #window.show_predictions()
    window.show_390m_table()
    #window.show_oc_pred_table()
  
    app.exec_()
    
 

if __name__=='__main__':
    main()











































# end