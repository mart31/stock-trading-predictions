# -*- coding: utf-8 -*-
"""
Created on Fri Dec 10 11:25:57 2021

@author: marti
"""

import candlestick as cs
import cash_graph as cg
import db_manager
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


from PyQt5.QtCore import Qt
#import PyQt5 as pg
from PyQt5.uic import loadUiType

import pyqtgraph as pg
from pyqtgraph import QtCore, QtGui

import pandas as pd
import datetime
import numpy as np

import time as t
import sys, os



def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)


FORM_CLASS,_=loadUiType(resource_path("live_ticker.ui"))

class ticker_window(QMainWindow, FORM_CLASS):
    def __init__(self,ticker,ticker_dict,time_index,date_list,previous_date_list,candle_390,cash_390,bet_size,depth_dict,depth_summary,parent=None):
        super(ticker_window,self).__init__(parent)
        self.setupUi(self)
        
        #print(depth_summary)
        self.finra_otc_size = {0.0:10000,0.1:5000,0.2:2500,0.51:1000,1.0:100,175:1}
        
        self.green = QColor('#7fc97f')
        self.gold = QColor(255,215,0)
        self.red = QColor(240,86,88)
        self.white = QColor(255,255,255)
        self.blue = QColor(51,51,255)
        
        self.table_col = ['tod','time','ema13_cash','average_cash','gains','close','pred_high20',
                          'hl_ratio','pred_hl_ratio20','pred_ng20']
        self.decimals = ['0','0','0','2','4','2','2','2','2']
        
        self.table_col390 = ['date','ema13_cash','average_cash','close','pred_next_gain','pred_next_oc',
                            'pred_next_high','hl_ratio']
        self.decimals_390m = ['0','0','4','2','2','2','2']
        
        self.trade_col = ['bet_size','buy_date','sell_date','buy_price','sell_price','gains','profit','ema13_cash',
                          'pred_high20','pred_low20','hl_ratio','pred_hl_ratio20','pred_ng20']
        
     
        
        self.ticker= ticker
        self.ticker_label.setText(ticker)
        self.ticker_dict=ticker_dict
        #self.df390 = df390  was removed in favor the candle data, cash data.
        self.candle_390 = candle_390
        self.cash_390 = cash_390
        self.flag_390m = False
        
        self.time_index=time_index
        self.date_list = date_list
        self.previous_date_list = previous_date_list
        self.date_index = self.get_date_index()
        #self.date_label.setText(str(self.date_index))
        self.set_date_label()
        
        self.candles = 60
        self.pred_length = 60
        
        self.trade_flag = False # cant have two trades from the same stock at the same time. 
        self.trades={}
        self.trade_index=0
        self.bet_size = bet_size
        
        
        '''
        Preparing the depth dict
        '''
        
        self.depth_dict = depth_dict
        self.add_depth_features(time_index)
        
        self.depth_summary = depth_summary
        self.show_depth_summary()
        self.show_depth_cash_ratio()
        
        '''
        This section initialize the candlestick chart. 
        '''
        
        self.previous_candles,self.previous_cash = self.create_previous_candles()
        self.candles_data,self.cash_data = self.get_candles_data(candle_size=1)
        
        self.item = cs.CandlestickItem(self.candles_data)
        
        self.graphWidget = pg.PlotWidget()
        self.plot = pg.plot()
        self.plot.addItem(self.item)
        
        self.flag = True

        self.chart_container.setContentsMargins(0,0,0,0)
        layout = QtWidgets.QHBoxLayout(self.chart_container)
        #layout.setContentsmargins(0,0,0,0)
        layout.addWidget(self.plot)
        
        '''
        This section initialize the cash chart.
        '''
        
        self.cash_item = cg.cash_graph(self.cash_data)
        
        self.cash_graphWidget = pg.PlotWidget()
        self.cash_plot = pg.plot()
        self.cash_plot.addItem(self.cash_item)
        

        self.cash_container.setContentsMargins(0,0,0,0)
        cash_layout = QtWidgets.QHBoxLayout(self.cash_container)
        #layout.setContentsmargins(0,0,0,0)
        cash_layout.addWidget(self.cash_plot)
        
        self.update_chart()
        
        self.Handle_Buttons()
        
        self.ticks_list = [[self.date_index,0,0],[self.date_index,0,0]]
        
        if self.time_index>0:
            self.previous_update = self.ticker_dict[self.date_index]
        else:
            self.previous_update = self.ticker_dict[self.previous_date_list[-1]]
        
        
        
    
    def get_green_line(self):
        
        if self.date_list[0] in self.ticker_dict:
            previous_close = self.ticker_dict[self.date_list[0]]['open']
            current_close = self.ticker_dict[self.date_index]['close']
        else:
            return 0
        
        if current_close is not None:
            gl = current_close/previous_close
            gl = (gl*100)-100
            return gl
        else:
            return 0
    
        
    def set_green_line(self):
        
        if self.time_index>=1:
            gl = self.get_green_line()
        else:
            gl=0
        
        if gl !=9999:
            if gl>=5:
                p = self.green_line_text.viewport().palette()
                p.setColor(self.green_line_text.viewport().backgroundRole(), self.green)
                self.green_line_text.viewport().setPalette(p)
            elif gl<=-5:
                p = self.green_line_text.viewport().palette()
                p.setColor(self.green_line_text.viewport().backgroundRole(), self.red)
                self.green_line_text.viewport().setPalette(p)
            else:
                p = self.green_line_text.viewport().palette()
                p.setColor(self.green_line_text.viewport().backgroundRole(), self.white)
                self.green_line_text.viewport().setPalette(p)
            
            text = '{:.2f}'
            gl = text.format(gl)
            gl = 'gl: '+gl
            
            self.green_line_text.setPlainText(str(gl))
        
    
    def add_depth_features(self,time_index):
        
        '''
        This function add the cash per order and the expected time to fill
        this order. 
        '''
        
        if time_index<=0:
            date_index = self.previous_date_list[-1]
        else:
            date_index = self.date_list[time_index]
        
        #print('!!!!')
        #print(date_index)
        #print('!!!!')
        
        if date_index in self.ticker_dict:
            last_row = self.ticker_dict[date_index]
            avg_cash = last_row['ema13_cash']
        else:
            avg_cash=7000
        
        if avg_cash <7000:
            avg_cash = 7000
            
        cash_per_second = avg_cash/60
        
        for side in self.depth_dict:
            for row in self.depth_dict[side]:
                cash = row[3]*row[4]
                expected_time = math.floor(cash/cash_per_second)
                            
                row.append(cash)
                row.append(expected_time)
                
                #print('!!!!')
                #print(type(row))
                #print(row)
                
    
    def show_depth_summary(self):
        
        if len(self.depth_summary)>0:
            ratio_5 = self.depth_summary['ratio_5']
            ratio_10 = self.depth_summary['ratio_10']
            ratio_20 = self.depth_summary['ratio_20']
            
            '''
            Setting the ratio 5%
            '''
            if ratio_5>=3:
                p = self.ratio5_text.viewport().palette()
                p.setColor(self.ratio5_text.viewport().backgroundRole(), self.gold)
                self.ratio5_text.viewport().setPalette(p)
            elif ratio_5>=1:
                p = self.ratio5_text.viewport().palette()
                p.setColor(self.ratio5_text.viewport().backgroundRole(), self.green)
                self.ratio5_text.viewport().setPalette(p)
            elif ratio_5<=-1:
                p = self.ratio5_text.viewport().palette()
                p.setColor(self.ratio5_text.viewport().backgroundRole(), self.red)
                self.ratio5_text.viewport().setPalette(p)
            else:
                p = self.ratio5_text.viewport().palette()
                p.setColor(self.ratio5_text.viewport().backgroundRole(), self.white)
                self.ratio5_text.viewport().setPalette(p)
                
            text = '{:.2f}'
            ratio_5 = text.format(ratio_5)
            ratio_5 = 'ratio 5%: '+ratio_5
            self.ratio5_text.setPlainText(ratio_5)
            
            
            '''
            Setting the ratio 10%
            '''
            if ratio_10>=3:
                p = self.ratio10_text.viewport().palette()
                p.setColor(self.ratio10_text.viewport().backgroundRole(), self.gold)
                self.ratio10_text.viewport().setPalette(p)
            elif ratio_10>=1:
                p = self.ratio10_text.viewport().palette()
                p.setColor(self.ratio10_text.viewport().backgroundRole(), self.green)
                self.ratio10_text.viewport().setPalette(p)
            elif ratio_10<=-1:
                p = self.ratio10_text.viewport().palette()
                p.setColor(self.ratio10_text.viewport().backgroundRole(), self.red)
                self.ratio10_text.viewport().setPalette(p)
            else:
                p = self.ratio10_text.viewport().palette()
                p.setColor(self.ratio10_text.viewport().backgroundRole(), self.white)
                self.ratio10_text.viewport().setPalette(p)
                
            text = '{:.2f}'
            ratio_10 = text.format(ratio_10)
            ratio_10 = 'ratio 10%: '+ratio_10
            self.ratio10_text.setPlainText(ratio_10)
            
            
            '''
            Setting the ratio 20%
            '''
            
            """
            if ratio_20>=3:
                p = self.ratio20_text.viewport().palette()
                p.setColor(self.ratio20_text.viewport().backgroundRole(), self.gold)
                self.ratio20_text.viewport().setPalette(p)
            elif ratio_20>=1:
                p = self.ratio20_text.viewport().palette()
                p.setColor(self.ratio20_text.viewport().backgroundRole(), self.green)
                self.ratio20_text.viewport().setPalette(p)
            elif ratio_20<=-1:
                p = self.ratio20_text.viewport().palette()
                p.setColor(self.ratio20_text.viewport().backgroundRole(), self.red)
                self.ratio20_text.viewport().setPalette(p)
            else:
                p = self.ratio20_text.viewport().palette()
                p.setColor(self.ratio20_text.viewport().backgroundRole(), self.white)
                self.ratio20_text.viewport().setPalette(p)
                
            text = '{:.2f}'
            ratio_20 = text.format(ratio_20)
            ratio_20 = 'ratio 20%: '+ratio_20
            self.ratio20_text.setPlainText(ratio_20)
            """
        else:
            self.ratio5_text.setPlainText('N/A')
            self.ratio10_text.setPlainText('N/A')
            #self.ratio20_text.setPlainText('N/A')
            
            
            
    def show_depth_cash_ratio(self):
        
        if len(self.depth_summary)>0:
            cash_ratio_5 = self.depth_summary['cash_ratio5']
            cash_ratio_10 = self.depth_summary['cash_ratio10']
            cash_ratio_20 = self.depth_summary['cash_ratio20']
            
            '''
            Setting the cash ratio 5%
            '''
            if cash_ratio_5>=60:
                p = self.cash_ratio5_text.viewport().palette()
                p.setColor(self.cash_ratio5_text.viewport().backgroundRole(), self.gold)
                self.cash_ratio5_text.viewport().setPalette(p)
            elif cash_ratio_5>=15:
                p = self.cash_ratio5_text.viewport().palette()
                p.setColor(self.cash_ratio5_text.viewport().backgroundRole(), self.green)
                self.cash_ratio5_text.viewport().setPalette(p)
            elif cash_ratio_5<=-15:
                p = self.cash_ratio5_text.viewport().palette()
                p.setColor(self.cash_ratio5_text.viewport().backgroundRole(), self.red)
                self.cash_ratio5_text.viewport().setPalette(p)
            else:
                p = self.cash_ratio5_text.viewport().palette()
                p.setColor(self.cash_ratio5_text.viewport().backgroundRole(), self.white)
                self.cash_ratio5_text.viewport().setPalette(p)
                
            text = '{:.0f}'
            cash_ratio_5 = text.format(cash_ratio_5)
            cash_ratio_5 = 'ratio 5%: '+cash_ratio_5
            self.cash_ratio5_text.setPlainText(cash_ratio_5)
            
            
            '''
            Setting the cash ratio 10%
            '''
            if cash_ratio_10>=60:
                p = self.cash_ratio10_text.viewport().palette()
                p.setColor(self.cash_ratio10_text.viewport().backgroundRole(), self.gold)
                self.cash_ratio10_text.viewport().setPalette(p)
            elif cash_ratio_10>=15:
                p = self.cash_ratio10_text.viewport().palette()
                p.setColor(self.cash_ratio10_text.viewport().backgroundRole(), self.green)
                self.cash_ratio10_text.viewport().setPalette(p)
            elif cash_ratio_10<=-15:
                p = self.cash_ratio10_text.viewport().palette()
                p.setColor(self.cash_ratio10_text.viewport().backgroundRole(), self.red)
                self.cash_ratio10_text.viewport().setPalette(p)
            else:
                p = self.cash_ratio10_text.viewport().palette()
                p.setColor(self.cash_ratio10_text.viewport().backgroundRole(), self.white)
                self.cash_ratio10_text.viewport().setPalette(p)
                
            text = '{:.0f}'
            cash_ratio_10 = text.format(cash_ratio_10)
            cash_ratio_10 = 'ratio 10%: '+cash_ratio_10
            self.cash_ratio10_text.setPlainText(cash_ratio_10)
            
            '''
            Setting the cash ratio 20%
            '''
            """
            if cash_ratio_20>=60:
                p = self.cash_ratio20_text.viewport().palette()
                p.setColor(self.cash_ratio20_text.viewport().backgroundRole(), self.gold)
                self.cash_ratio20_text.viewport().setPalette(p)
            elif cash_ratio_20>=15:
                p = self.cash_ratio20_text.viewport().palette()
                p.setColor(self.cash_ratio20_text.viewport().backgroundRole(), self.green)
                self.cash_ratio20_text.viewport().setPalette(p)
            elif cash_ratio_20<=-15:
                p = self.cash_ratio20_text.viewport().palette()
                p.setColor(self.cash_ratio20_text.viewport().backgroundRole(), self.red)
                self.cash_ratio20_text.viewport().setPalette(p)
            else:
                p = self.cash_ratio20_text.viewport().palette()
                p.setColor(self.cash_ratio20_text.viewport().backgroundRole(), self.white)
                self.cash_ratio20_text.viewport().setPalette(p)
                
            text = '{:.0f}'
            cash_ratio_20 = text.format(cash_ratio_20)
            cash_ratio_20 = 'ratio 20%: '+cash_ratio_20
            self.cash_ratio20_text.setPlainText(cash_ratio_20)
            """
            
            
        else:
            self.cash_ratio5_text.setPlainText('N/A')
            self.cash_ratio10_text.setPlainText('N/A')
            #self.cash_ratio20_text.setPlainText('N/A')
            
      
                
       
    
    def update_depth_data(self,time_index,depth_dict,depth_summary,approx_label='Actual'):
        self.depth_dict=depth_dict
        self.depth_summary=depth_summary
        
        
        try:
            if approx_label=='Actual':
                state ='AAAAAAA'
                self.add_depth_features(time_index)
            else:
                state ='ZZZZZZ'
                self.add_depth_features(time_index-1)
        except Exception as e:
            print('')
            print('!!!')
            print(e)
            print('ticker: {} approx: {} date index: {}'.format(self.ticker,approx_label,self.get_date_index()))
            print(state)
            print('!!!')
            print('')
            
        self.show_depth_data()
        self.show_depth_summary()
        self.show_depth_cash_ratio()
        
    
        
    def update_data(self,time_index,data,approx_label='Actual'):
        
        
        test_null = data['close']
        if test_null is None:
            '''
            Test null can only be None if it an approximation.
            '''
            pass
        
        else:
            if self.time_index!=time_index:
                new_candle=True
            else:
                new_candle=False
            
            self.time_index = time_index
            self.date_index = self.get_date_index()
            #self.date_label.setText(str(self.date_index))
            self.set_date_label()
            
            self.approx_label.setText(approx_label)
            self.ticker_dict[self.date_index]=data
            self.show_predictions()
            
            self.candles_data,self.cash_data = self.get_candles_data(candle_size=1)
            
            if self.flag_390m==False:
                self.update_chart(candle_size=1)
                
            self.verify_ticks(new_candle,data)
                
    
    
    def verify_ticks(self,new_candle,data):
    
        if self.time_index>=1:
            if new_candle:
                barcount = data['barCount']
                if barcount>0:
                    price = data['close']
                    volume = data['volume']
                    self.update_last_tick(price,volume)
            else:
                if self.ticker_dict[self.date_index]['barCount']>self.previous_update['barCount']:
                    last_volume = self.previous_update['volume']
                    current_volume = self.ticker_dict[self.date_index]['volume']
                    volume = current_volume-last_volume
                    price = self.ticker_dict[self.date_index]['close']
                    self.update_last_tick(price,volume)
                
                
                
                
    def update_last_tick(self,price,volume):
        #print('price: {},volume: {}'.format(price,volume))
        today = datetime.datetime.today()
        cash = price*volume
        #cash = volume
        
        trade_row = [today,price,cash]
        self.ticks_list.append(trade_row)
        
        self.previous_update = self.ticker_dict[self.date_index]   
        
        self.show_tick_table()
        
    
    def create_time_string(self,today):
        #today = datetime.datetime.today()
        second = today.second
        if second<10:
            second = '0'+str(second)
        else:
            second = str(second)
        
        minute = today.minute
        if minute<10:
            minute = '0'+str(minute)
        else:
            minute = str(minute)
            
        hour = str(today.hour)
        time_str = '{}:{}:{}'.format(hour,minute,second)  
        
        return time_str
    
    
    def init_tick_table(self):
        self.tick_table.setRowCount(0)
        
        for y in range(10):
            self.tick_table.insertRow(y)
            
        self.show_tick_table()  
        self.tick_table.resizeColumnsToContents()
        

    
    def show_tick_table(self):
        '''
        The tick data has the format: (time,price,cash)
        '''
        
        length = len(self.ticks_list)-1
        for y in range(10):
            index = length-y
            
            if index>=0:
                tick = self.ticks_list[index]
                
                t_index = tick[0]
                price = str(tick[1])
                cash = str(int(tick[2]))
                
                time_str = self.create_time_string(t_index)
                
                self.tick_table.setItem(y,0,QTableWidgetItem(time_str))
                self.tick_table.setItem(y,1,QTableWidgetItem(price))
                self.tick_table.setItem(y,2,QTableWidgetItem(cash))
           
             
    
    def set_date_label(self):
        date_key = self.date_index
        hour = str(date_key.hour)
        minute = str(date_key.minute)
        
        if len(hour)==1:
            hour = '0'+hour
        if len(minute)==1:
            minute = '0'+minute
       
        tod = hour + ':'+minute
        
        self.date_label.setText(tod)
    
    
    def get_active_trade(self):
        return self.trades
    
    
    def update_390m(self,row):
        pass
        
        
    def Handle_Buttons(self):
        #self.refresh_button.clicked.connect(self.refresh_everything)
        self.buy_button.clicked.connect(self.buy)
        self.sell_button.clicked.connect(self.sell)
        self.daily_button.clicked.connect(self.show_390m)
        self.intraday_button.clicked.connect(self.show_1m)
        
    
    def init_pred_table(self):
        self.pred_table.setRowCount(0)
        
        for y in range(self.pred_length):
            self.pred_table.insertRow(y)
            
        self.show_predictions()
        self.pred_table.resizeColumnsToContents()
        
        
    def init_depth_table(self):
        self.ask_table.setRowCount(0)
        self.bid_table.setRowCount(0)
        
        for y in range(10):
            self.ask_table.insertRow(y)
            self.bid_table.insertRow(y)
         
        self.show_depth_data()    
         
        self.ask_table.resizeColumnsToContents()
        self.bid_table.resizeColumnsToContents()
        
        
        
        
        
    def update_number_shares(self,price):
        nbr_share = int(self.bet_size/price)
        nbr_share_str = str(nbr_share)
        self.nbr_shares_text.setPlainText(nbr_share_str) # bug here
        
        min_share = 10000
        for key in self.finra_otc_size:
            if price>=key:
                min_share = self.finra_otc_size[key]
            else:
                break
                
        #print('min_share: {}'.format(min_share))
        
        if nbr_share<min_share:
            p = self.nbr_shares_text.viewport().palette()
            p.setColor(self.nbr_shares_text.viewport().backgroundRole(), self.red)
            self.nbr_shares_text.viewport().setPalette(p)
        else:
            if price>0.7:
                p = self.nbr_shares_text.viewport().palette()
                p.setColor(self.nbr_shares_text.viewport().backgroundRole(), self.green)
                self.nbr_shares_text.viewport().setPalette(p) 
                
        self.transaction_cost(nbr_share)
                
    
    def transaction_cost(self,nbr_share):
        '''
        A couple of assumption has been made. 
        
        First I assume that i will trade much more then
        300 000 shares a month. Which give the per share amount to be 0.002 (x2).
        Which bring the cost between 0.004 or 2% from IB.
        
        Second, the exchange fee are at most 0.3% and are charge on the buy or sell side depending
        on if you add or remove liquidity. 
        
        Third, clearing fee are at 0.0002 per share. Max of 0.5% of trade value
        
        Fourth, finra transaction fee only when selling, is 0.000119 and cap at 5.95$. The other
        costs seems to be negligeable. 
        
        Finaly in comparison the fixed fee is 0.005 per share. 
        '''
        
        # compute fixed price.
        fixed_price = nbr_share*0.01
        if fixed_price>(self.bet_size*0.02):
            fixed_price = self.bet_size*0.02
        
            
        #compute tiered price
        tiered_price = nbr_share*0.004
        
        #ib transaction cost
        if tiered_price>(self.bet_size*0.02):
            tiered_price = self.bet_size*0.02
        
        #exchange fee only for tiered price.
        tiered_price = tiered_price + (self.bet_size*0.003)
        
        #clearing fee
        clearing_fee = nbr_share*0.0002
        
        if clearing_fee > (self.bet_size*0.005):
            clearing_fee = self.bet_size*0.005
        
        tiered_price = tiered_price + clearing_fee
        
        finra_cost = nbr_share*0.000119
        
        if finra_cost > 5.95:
            finra_cost = 5.95
            
        tiered_price = tiered_price + finra_cost
        fixed_price = fixed_price + finra_cost
        
        temp = fixed_price/self.bet_size
        temp = temp*100
        temp = round(temp,2)
        #fixed_percent = 'F:{}%'.format(temp)
        ib_percent = 'IB:{}%'.format(temp)
        
        #temp = tiered_price/self.bet_size
        #temp = temp*100
        #temp = round(temp,2)
        #tiered_percent = 'T:{}%'.format(temp)
        
        '''
        RBC cost are 6.95$ per trade
        '''
        
        temp = 13.9/self.bet_size
        temp = temp*100
        temp = round(temp,2)
        rbc_percent = 'RBC:{}%'.format(temp)
        
        #cost_str = tiered_percent + ' ' + fixed_percent+' '+rbc_percent
        cost_str = ib_percent+' '+rbc_percent
        
        self.transaction_cost_text.setPlainText(cost_str) # bug here
        
        """
        if tiered_price<fixed_price:
            p = self.transaction_cost_text.viewport().palette()
            p.setColor(self.transaction_cost_text.viewport().backgroundRole(), self.green)
            self.transaction_cost_text.viewport().setPalette(p)
        else:
            
            p = self.transaction_cost_text.viewport().palette()
            p.setColor(self.transaction_cost_text.viewport().backgroundRole(), self.red)
            self.transaction_cost_text.viewport().setPalette(p) 
        """
        
        if fixed_price<13.9: #13.9$ is the cost of the rbc transaction
            p = self.transaction_cost_text.viewport().palette()
            p.setColor(self.transaction_cost_text.viewport().backgroundRole(), self.green)
            self.transaction_cost_text.viewport().setPalette(p)
        else:
            p = self.transaction_cost_text.viewport().palette()
            p.setColor(self.transaction_cost_text.viewport().backgroundRole(), self.blue)
            self.transaction_cost_text.viewport().setPalette(p) 
        

    
    def create_depth_row(self,row):
        mmid = row[2]
        price = row[3]
        size = row[4]
        cash = row[5]
        expected_time = row[6]
        
        depth_row = [mmid,price,size,cash,expected_time]
        return depth_row
    
    
    def show_depth_data(self):
        '''
        The depth list format is [ticker,index,mmid,price,size,cash,expected_time]
        The table format is [mmid,price,size,cash,E(time)]
        '''
        
        for side in self.depth_dict:
            row_list = self.depth_dict[side]
            for y in range(10):
                row = row_list[y]
                depth_row = self.create_depth_row(row)
                
                expected_time = depth_row[4]
            
                for x in range(len(depth_row)):
                    if x==1:
                        # the price need 4 digit of precision. 
                        dec = '4'
                    else:
                        dec='0'
                    
                    value = depth_row[x]
                    if x>=1: # the first one is already a string.
                        text = "{:."+dec+"f}"
                        value = text.format(value)
                        
                    if side=='ask':
                        self.ask_table.setItem(y,x,QTableWidgetItem(value))
                        
                        if expected_time>=60:
                            item = self.ask_table.item(y,x)
                            item.setBackground(self.red)
                        
                    elif side=='bid':
                        self.bid_table.setItem(y,x,QTableWidgetItem(value))
                
                        if expected_time>=60 and expected_time<180:
                            item = self.bid_table.item(y,x)
                            item.setBackground(self.green)
                        elif expected_time>=180:
                            item = self.bid_table.item(y,x)
                            item.setBackground(self.gold)
                        else:
                            item = self.bid_table.item(y,x)
                            item.setBackground(self.white)
           
                
    
    def show_predictions(self):
        
        index = self.time_index
        
        for y in range(self.pred_length):
            
            x = 0
            
            date_key = self.find_date_index(index)
            '''
            When restarting the system, sometime we have data rows missing. 
            '''
            
            if date_key in self.ticker_dict:
                row = self.ticker_dict[date_key]
                
                if y ==0:
                    '''
                    Compute the number of share to be bought with the current bet size
                    '''
                    price = row['close']
                    self.update_number_shares(price)
                
                for col in self.table_col:
                    
                    if col=='tod':
                        hour = str(date_key.hour)
                        minute = str(date_key.minute)
                        
                        if len(hour)==1:
                            hour = '0'+hour
                        if len(minute)==1:
                            minute = '0'+minute
                       
                        tod = hour + ':'+minute
                        self.pred_table.setItem(y,x,QTableWidgetItem(tod))
                               
                    else:
                        value = row[col]
                        
                        if value is not None:
                            #if value is not None:
                            dec = self.decimals[x-1]
                            text = "{:."+dec+"f}"
                            #print('d: {} value: {} type: {} col: {}'.format(date_key,value,type(value),col))
                            value = text.format(value)
                            self.pred_table.setItem(y,x,QTableWidgetItem(value))
                            
                            if col=='hl_ratio':
                                if row[col]>=5:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.gold)
                                elif row[col]>=2:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.green)
                                elif row[col]<=-2:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.red) 
                            elif col=='pred_hl_ratio20':
                                if row[col]>=5:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.gold)
                                elif row[col]>=2:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.green)
                                elif row[col]<=-2:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.red)
                            elif col=='pred_ng20':
                                if row[col]>=5:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.gold)
                                elif row[col]>=2:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.green)
                                elif row[col]<=-2:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.red) 
                            elif col=='pred_high20':
                                if row[col]>=5:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.green)
                                elif row[col]<=0:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.red)
                                    
                            elif col=='gains':
                                if row[col]>=4:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.green)
                                elif row[col]<=-4:
                                    item = self.pred_table.item(y,x)
                                    item.setBackground(self.red)
                        else:
                            value = 'NaN'
                            self.pred_table.setItem(y,x,QTableWidgetItem(value))
                    
                    x = x+1
            
            else:
                x=0
                for col in self.table_col:
                    
                    self.pred_table.setItem(y,x,QTableWidgetItem('N/A'))
                    item = self.pred_table.item(y,x)
                    item.setBackground(self.red)
                    
                    x = x+1
            
            index = index-1
        
        self.set_green_line()
        #self.pred_table.resizeColumnsToContents()
        
        
    def get_date_index(self):
        date_index = self.date_list[self.time_index]
        return date_index
    
    
    def find_date_index(self,index):
        
        if index>=0:
            date_index = self.date_list[index]
        if index<0:
            date_index = self.previous_date_list[index]
            
        return date_index
        
    
    def update_chart(self,candle_size=1):
        
        self.candles_data, self.cash_data = self.get_candles_data(candle_size)
        
        self.plot.removeItem(self.item)
        self.resize_chart()
        self.item = cs.CandlestickItem(self.candles_data)
        self.plot.addItem(self.item)
        
        self.cash_plot.removeItem(self.cash_item)
        self.resize_cash_chart()
        self.cash_item = cg.cash_graph(self.cash_data)
        self.cash_plot.addItem(self.cash_item)
        
        
    def resize_chart(self):
        high_y,low_y,high_x,low_x = self.find_hl(self.candles_data)
        
        r = (high_y-low_y)/6
        minimum = low_y-r
        maximum = high_y+r
        
        if minimum <=0:
            minimum = 0
            
        padding = 0
        self.plot.setYRange(minimum,maximum)
        self.plot.setXRange((low_x-padding),(high_x+padding))
      
        
    def resize_cash_chart(self):
        high_y,low_y,high_x,low_x = self.find_cash_hl(self.cash_data)
        
        r = (high_y)/6
        #minimum = low_y-r
        maximum = high_y+r
        
        padding = 0
        self.cash_plot.setYRange(0,maximum)
        self.cash_plot.setXRange((low_x-padding),(high_x+padding))
        
        
    def find_hl(self,candles_list):
    
        temp_list = candles_list[-self.candles:]
        
        high_y = 0
        low_y = 99999
        for t_,o,c,l,h in temp_list:
            if h> high_y:
                high_y = h
            if l < low_y:
                low_y = l
                
        low_x = candles_list[-self.candles][0]
        high_x = candles_list[-1][0]        
                
        return high_y,low_y,high_x,low_x
    
    
    def find_cash_hl(self,candles_list):
    
        temp_list = candles_list[-self.candles:]
        
        high_y = 0
        low_y = 0
        for t_,avg_cash in temp_list:
            
            if avg_cash is not None:
                if avg_cash> high_y:
                    high_y = avg_cash
                    
              
        low_x = candles_list[-self.candles][0]
        high_x = candles_list[-1][0]        
                
        return high_y,low_y,high_x,low_x
     
        
    def get_candles_data(self,candle_size):
        
        if candle_size==1:
            
            if self.time_index >=0:
                # fields are (time, open, close, min, max).
                d_list = self.date_list[:(self.time_index+1)]
                current_candles = []
                current_cash = []
                
                for d in d_list:
                    #print(d)
                    if d in self.ticker_dict:
                        minute_list = []
                        minute_list.append(self.ticker_dict[d]['time'])
                        minute_list.append(self.ticker_dict[d]['open'])
                        minute_list.append(self.ticker_dict[d]['close'])
                        minute_list.append(self.ticker_dict[d]['low'])
                        minute_list.append(self.ticker_dict[d]['high'])
                        current_candles.append(minute_list)
                        
                        cash_list = []
                        cash_list.append(self.ticker_dict[d]['time'])
                        cash_list.append(self.ticker_dict[d]['average_cash'])
                        current_cash.append(cash_list)
                    
                    
                candles_data = self.previous_candles + current_candles
                cash_data = self.previous_cash + current_cash
            else:
                candles_data = self.previous_candles
                cash_data = self.previous_cash
        elif candle_size==390:
            candles_data = self.candle_390
            cash_data = self.cash_390
        
        return candles_data,cash_data   
        
    def create_previous_candles(self):
    
        # fields are (time, open, close, min, max).
        previous_candles = []
        previous_cash= []
        
        for d in self.previous_date_list:
            minute_list = []
            minute_list.append(self.ticker_dict[d]['time'])
            minute_list.append(self.ticker_dict[d]['open'])
            minute_list.append(self.ticker_dict[d]['close'])
            minute_list.append(self.ticker_dict[d]['low'])
            minute_list.append(self.ticker_dict[d]['high'])
            previous_candles.append(minute_list)
            
            cash_list = []
            cash_list.append(self.ticker_dict[d]['time'])
            cash_list.append(self.ticker_dict[d]['average_cash'])
            previous_cash.append(cash_list)
            
        return previous_candles,previous_cash
    

    def show_390m(self):
        self.flag_390m = True
        self.update_chart(candle_size=390)  

    def show_1m(self):
        self.flag_390m = False
        self.update_chart(candle_size=1)         


    def buy(self):
        last_row = self.ticker_dict[self.get_date_index()]
        if  (self.trade_flag):
            print('ONE TRADE AT A TIME FOR A GIVEN TICKER!!!')
        else:
            
            if (last_row['ema13_cash']/5) > self.bet_size:
                betting_size = self.bet_size
            else:
                betting_size = last_row['ema13_cash']/5
            
            self.trade_flag = True
            self.trades[self.trade_index]={}
            self.trades[self.trade_index]['ticker']= self.ticker
            self.trades[self.trade_index]['bet_size']=betting_size
            self.trades[self.trade_index]['buy_date']= str(self.get_date_index())
            self.trades[self.trade_index]['sell_date']=''
            self.trades[self.trade_index]['buy_price']= last_row['close']
            self.trades[self.trade_index]['sell_price']=0
            self.trades[self.trade_index]['gains']=0
            self.trades[self.trade_index]['profit']=0
            self.trades[self.trade_index]['ema13_cash']= last_row['ema13_cash']
            
            self.trades[self.trade_index]['pred_high20']=last_row['pred_high20']
            self.trades[self.trade_index]['pred_low20']=last_row['pred_low20']
            self.trades[self.trade_index]['hl_ratio']=last_row['hl_ratio']
            self.trades[self.trade_index]['pred_hl_ratio20']=last_row['pred_hl_ratio20']
            self.trades[self.trade_index]['pred_ng20']=last_row['pred_ng20']
            
            self.trades[self.trade_index]['type']='intraday'
            
            #print(self.trades[self.trade_index]) 
            print('bought {} {} at {}'.format(self.ticker,self.get_date_index(),last_row['close']))
         
    
    def sell(self):
        
        if self.trade_flag:
            self.trade_flag=False
            
            last_row = self.ticker_dict[self.get_date_index()]
            
            self.trades[self.trade_index]['sell_date']=self.get_date_index()
            self.trades[self.trade_index]['sell_price']=last_row['close']
            gains =  self.trades[self.trade_index]['sell_price']/self.trades[self.trade_index]['buy_price']
            self.trades[self.trade_index]['gains']= gains
            self.trades[self.trade_index]['profit']=self.trades[self.trade_index]['bet_size']*(self.trades[self.trade_index]['gains']-1.0)
    
            sell_date = self.trades[self.trade_index]['sell_date']
            buy_date = self.trades[self.trade_index]['buy_date']
            
            if isinstance(buy_date,str):
                buy_date = datetime.datetime.strptime(buy_date, '%Y-%m-%d %H:%M:%S')
            if isinstance(sell_date,str):
                sell_date = datetime.datetime.strptime(sell_date, '%Y-%m-%d %H:%M:%S')
            
            print('{} vs {} ... {} vs {}'.format(buy_date,sell_date,buy_date.day,sell_date.day))
            if buy_date.day !=sell_date.day:
                self.trades[self.trade_index]['type']='oc'
    
            pd.set_option('display.max_columns', 500)
            #print(self.trades[self.trade_index])
            trade_df = pd.DataFrame.from_dict(self.trades,orient='index')
            trade_df.set_index('ticker',inplace=True) 
            trade_df.index.name='ticker'
            
            #print(trade_df)
            trades_dbm = db_manager.db_manager(db_name='trades_database') 
            trades_dbm.save_trades(trade_df)
            #self.trade_index = self.trade_index + 1
            self.trades={}
            
            print('sold {} {} at {} for a gains of {}'.format(self.ticker,self.get_date_index(),last_row['close'],gains))
            
        else:
            print('YOU NEED TO HAVE SOMETHING TO SELL!!!')





"""
def main():
    app = QApplication(sys.argv)
    
    ticker='AABB'
    ticker_dict,date_list,previous_date_list = get_example_dict()
    time_index=8
    
    df390 = get_df390_example(ticker,time_index,date_list)
    
    window = ticker_window(ticker,ticker_dict,time_index,date_list,previous_date_list,df390)
    window.show()
    
    window.init_pred_table()
    
    app.exec_()
    
  
if __name__=='__main__':
    main()

"""





       
        
        
        
        
        
        
        
        
        
        
        
        
        
# end        