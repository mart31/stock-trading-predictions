# -*- coding: utf-8 -*-
"""
Created on Tue Jul 20 06:59:41 2021

@author: marti
"""


import features_creator as fc
import queue
import datetime


def compute_features_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size=1,approx=False,approx_ratio=1.0): 
    '''
    Parameters
    ----------
    df : The dataframe that contains the features data.
    last_row : last row of data which is needed for most computation
    live_row : The row that needs to be updated.
    last_date : The index of the previous row
    live_date : The index of live_row in df
    bb_queue : the bollinger band queue needed for the normalize_bb calculation
    candle_size : candle_size
    approx: Wether the computation are an approximation
    approx_ratio:  60/current_second enable us to approximate the average cash. 

    This function computes most of the features needed for live prediction.
    '''
    
    
    live_row['average_cash'] = fc.dict_average_cash(last_row, live_row,approx_ratio)
    live_row['gains'] = fc.dict_gains(last_row, live_row)
    live_row['avg_gain'] = fc.dict_avg_gain(last_row,live_row)
    live_row['avg_loss'] = fc.dict_avg_loss(last_row,live_row)
    live_row['RSI'] = fc.dict_RSI(last_row,live_row)
    live_row['d1_ema3_RSI'] = fc.dict_d1_ema3_RSI(last_row,live_row)
    live_row['d2_ema3_RSI'] = fc.dict_d2_ema3_RSI(last_row,live_row)
    live_row['normalize_bb'] = fc.dict_normalize_bb(last_row,live_row,bb_queue,approx)
    live_row['d1_ema3_bb'] = fc.dict_d1_ema3_bb(last_row,live_row)
    live_row['d2_ema3_bb'] = fc.dict_d2_ema3_bb(last_row,live_row)
    live_row['ema12'] = fc.dict_ema12(last_row,live_row)
    live_row['ema26'] = fc.dict_ema26(last_row,live_row)
    live_row['PPO_line'] = fc.dict_PPO_line(last_row,live_row)
    live_row['d1_ema3_PPO_line'] = fc.dict_d1_ema3_PPO_line(last_row,live_row)
    live_row['d2_ema3_PPO_line'] = fc.dict_d2_ema3_PPO_line(last_row,live_row)
    live_row['signal_line'] = fc.dict_signal_line(last_row,live_row)
    live_row['PPO_histo'] = fc.dict_PPO_histo(last_row,live_row)
    live_row['d1_ema3_PPO_histo'] = fc.dict_d1_ema3_PPO_histo(last_row,live_row)
    live_row['d2_ema3_PPO_histo'] = fc.dict_d2_ema3_PPO_histo(last_row,live_row)
    live_row['ema13_cash'] = fc.dict_ema13_cash(last_row,live_row)
    live_row['d1_ema13_cash'] = fc.dict_d1_ema13_cash(last_row,live_row)
    live_row['d2_ema13_cash'] = fc.dict_d2_ema13_cash(last_row,live_row)
    live_row['ema13_barcount'] = fc.dict_ema13_barcount(last_row,live_row)
    live_row['d1_ema13_barcount'] = fc.dict_d1_ema13_barcount(last_row,live_row)
    live_row['d2_ema13_barcount'] = fc.dict_d2_ema13_barcount(last_row,live_row)
    live_row['cash_ratio'] = fc.dict_cash_ratio(last_row,live_row)
    live_row['barcount_ratio'] = fc.dict_barcount_ratio(last_row,live_row)
    live_row['candle_shape'] = fc.dict_candle_shape(last_row,live_row)
    live_row['candle_range'] = fc.dict_candle_range(last_row,live_row)
    
    # added recently
    live_row['ema13_range'] = fc.dict_ema13_range(last_row,live_row)
    live_row['d1_ema13_range'] = fc.dict_d1_ema13_range(last_row,live_row)
    live_row['d2_ema13_range'] = fc.dict_d2_ema13_range(last_row,live_row)
    
    if candle_size !=390:
        live_row['last_candle'] = fc.last_candle(live_date,candle_size)
        live_row['overnight_candle'] = fc.overnight_candle(live_date,candle_size)
        live_row['morning'] = fc.morning(live_date,candle_size)
        
    live_row['trade_ratio'] = fc.dict_trade_ratio(last_row,live_row)
    live_row['friday'] = fc.friday(live_date)
    
    live_row['vwap'] = fc.dict_vwap(live_row,price_q,volume_q,approx)
    live_row['vwap_ratio'] = fc.dict_vwap_ratio(live_row)
      



def compute_live_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size=1,approx=False,approx_ratio=1.0): 
    '''
    Parameters
    ----------
    df : The dataframe that contains the features data.
    last_row : last row of data which is needed for most computation
    live_row : The row that needs to be updated.
    last_date : The index of the previous row
    live_date : The index of live_row in df
    bb_queue : the bollinger band queue needed for the normalize_bb calculation
    candle_size : candle_size
    approx: Wether the computation are an approximation
    approx_ratio:  60/current_second enable us to approximate the average cash. 

    This function computes most of the features needed for live prediction.
    '''
    
    
    live_row['average_cash'] = fc.dict_average_cash(last_row, live_row,approx_ratio)
    live_row['gains'] = fc.dict_gains(last_row, live_row)
    
    gains3,gains5,gains10,gains20 = fc.dict_gains_x(live_row,bb_queue) #the bb_queue contains the price of the last 20 candles
    live_row['gains3'] = gains3
    live_row['gains5'] = gains5
    live_row['gains10'] = gains10
    live_row['gains20'] = gains20
    
    live_row['avg_gain'] = fc.dict_avg_gain(last_row,live_row)
    live_row['avg_loss'] = fc.dict_avg_loss(last_row,live_row)
    live_row['RSI'] = fc.dict_RSI(last_row,live_row)
    live_row['d1_ema3_RSI'] = fc.dict_d1_ema3_RSI(last_row,live_row)
    live_row['d2_ema3_RSI'] = fc.dict_d2_ema3_RSI(last_row,live_row)
    
    live_row['normalize_bb'] = fc.dict_normalize_bb(last_row,live_row,bb_queue,approx)
    live_row['d1_ema3_bb'] = fc.dict_d1_ema3_bb(last_row,live_row)
    live_row['d2_ema3_bb'] = fc.dict_d2_ema3_bb(last_row,live_row)
    
    live_row['ema12'] = fc.dict_ema12(last_row,live_row)
    live_row['ema26'] = fc.dict_ema26(last_row,live_row)
    live_row['PPO_line'] = fc.dict_PPO_line(last_row,live_row)
    live_row['d1_ema3_PPO_line'] = fc.dict_d1_ema3_PPO_line(last_row,live_row)
    live_row['d2_ema3_PPO_line'] = fc.dict_d2_ema3_PPO_line(last_row,live_row)
    live_row['signal_line'] = fc.dict_signal_line(last_row,live_row)
    live_row['PPO_histo'] = fc.dict_PPO_histo(last_row,live_row)
    live_row['d1_ema3_PPO_histo'] = fc.dict_d1_ema3_PPO_histo(last_row,live_row)
    live_row['d2_ema3_PPO_histo'] = fc.dict_d2_ema3_PPO_histo(last_row,live_row)
    
    live_row['ema13_cash'] = fc.dict_ema13_cash(last_row,live_row)
    live_row['d1_ema13_cash'] = fc.dict_d1_ema13_cash(last_row,live_row)
    live_row['d2_ema13_cash'] = fc.dict_d2_ema13_cash(last_row,live_row)
  
    live_row['candle_shape'] = fc.dict_candle_shape(last_row,live_row)
    live_row['candle_range'] = fc.dict_candle_range(last_row,live_row)
    
    # added recently
    live_row['ema13_range'] = fc.dict_ema13_range(last_row,live_row)
    live_row['d1_ema13_range'] = fc.dict_d1_ema13_range(last_row,live_row)
    live_row['d2_ema13_range'] = fc.dict_d2_ema13_range(last_row,live_row)
    live_row['hl_range20'] = fc.dict_hl_range20(live_row,bb_queue,approx) # the bb_queue contains the price of the last 20 candles
    
   
    live_row['overnight_candle'] = fc.overnight_candle(live_date,candle_size)
    
    live_row['vwap'] = fc.dict_vwap(live_row,price_q,volume_q,approx)
    live_row['vwap_ratio'] = fc.dict_vwap_ratio(live_row)
    
    live_row['no_trade_count'] = fc.no_trade_count(live_row,volume_q,approx)     
        
        
def compute_all_ticker(features_dict,watchlist,previous_date,live_date,bb_dict,vwap_dict,approx,approx_ratio=1.0,min_barcount=0,candle_size=1):
    '''
    Used by live feed to compute the features of all the ticker at once. 
    '''
    
    for ticker in watchlist:
        #ticker = watchlist[key]
        
        barcount = features_dict[live_date][ticker]['barCount']
        
        if barcount >=min_barcount: # we only compute stock that are active. The inactive stock are missing data. 
        
            bb_queue = bb_dict[ticker]
            price_q = vwap_dict[ticker]['price_q']
            volume_q = vwap_dict[ticker]['volume_q']
            last_row = features_dict[previous_date][ticker]
            live_row = features_dict[live_date][ticker]
           
            compute_live_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size,approx,approx_ratio) 
            #compute_features_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size,approx,approx_ratio)
     

def compute_all_intraday_daily(features_dict,daily_dict,watchlist,live_date):
    '''
    Used by live feed to compute the features of all the ticker at once. 
    '''
    
    for ticker in watchlist:
    
        live_row = features_dict[live_date][ticker]
        daily_last_row = daily_dict[ticker]['last_row']
        daily_bb_queue = daily_dict[ticker]['bb_queue']
        compute_live_intraday(daily_last_row,live_row,daily_bb_queue,live_date)
        #compute_intraday(daily_last_row,live_row,daily_bb_queue,live_date)
              
     
        

def compute_remaining_features(features_dict,watchlist,overnight_candle,live_date):
    
    for ticker in watchlist:
        overnight_open = features_dict[overnight_candle][ticker]['open']
        current_close = features_dict[live_date][ticker]['close']
        green_line = current_close/overnight_open
        
        features_dict[live_date][ticker]['green_line']=green_line
        
        

def compute_ticker_remaining_features(ticker,features_dict,overnight_candle,live_date):
    
    overnight_open = features_dict[overnight_candle][ticker]['open']
    current_close = features_dict[live_date][ticker]['close']
    green_line = current_close/overnight_open
    
    oc_gains = features_dict[overnight_candle][ticker]['gains'] 
    
    features_dict[live_date][ticker]['green_line']=green_line
    features_dict[live_date][ticker]['oc_gains']=oc_gains        

 
    
def init_features(features_dict,date_list,candle_size=1): 
    
    
    bb_queue = queue.Queue(maxsize=20)
    
    if candle_size==1:
        price_q = queue.Queue(maxsize=20)  
        volume_q = queue.Queue(maxsize=20) 
    else:
        price_q = queue.Queue(maxsize=20)  
        volume_q = queue.Queue(maxsize=20) 
    
    init_first_features_row(features_dict, date_list,candle_size)
    first = True
    for live_date in date_list:
        
        if live_date in features_dict:
            live_row = features_dict[live_date]
            if first:
                first = False
                first_price = features_dict[live_date]['close']
                bb_queue.put(first_price)
                
                last_row = live_row
            else:
                #compute_features_row(last_row,live_row,live_date,bb_queue,candle_size=1,approx=False)
                compute_features_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size,approx=False)
    
                
            last_row = live_row
    
    
def init_first_features_row(features_dict,date_list,candle_size): 
    
    
    for first_date in date_list:
        if first_date in features_dict:
            
            first_close = features_dict[first_date]['close']
            average = features_dict[first_date]['average']
            volume = features_dict[first_date]['volume']
            
            features_dict[first_date]['average_cash'] = average*volume
            features_dict[first_date]['gains'] = 1.0
    
            features_dict[first_date]['avg_gain'] = first_close/20
            features_dict[first_date]['avg_loss'] = first_close/20
            features_dict[first_date]['RSI'] = 50
            features_dict[first_date]['d1_ema3_RSI'] = 0.0
            features_dict[first_date]['d2_ema3_RSI'] = 0.0
            
            features_dict[first_date]['normalize_bb'] = 0.0
            features_dict[first_date]['d1_ema3_bb'] = 0.0
            features_dict[first_date]['d2_ema3_bb'] = 0.0
            
            features_dict[first_date]['ema12'] = first_close
            features_dict[first_date]['ema26'] = first_close
            features_dict[first_date]['PPO_line'] = 0.0
            features_dict[first_date]['d1_ema3_PPO_line'] = 0.0
            features_dict[first_date]['d2_ema3_PPO_line'] = 0.0
            features_dict[first_date]['signal_line'] = 0.0
            features_dict[first_date]['PPO_histo'] = 0.0
            features_dict[first_date]['d1_ema3_PPO_histo'] = 0.0
            features_dict[first_date]['d2_ema3_PPO_histo'] = 0.0
            
            
            features_dict[first_date]['ema13_cash'] = 100
            features_dict[first_date]['d1_ema13_cash'] = 0.0
            features_dict[first_date]['d2_ema13_cash'] = 0.0
            features_dict[first_date]['ema13_barcount'] = 1
            features_dict[first_date]['d1_ema13_barcount'] = 0.0
            features_dict[first_date]['d2_ema13_barcount'] = 0.0
            features_dict[first_date]['cash_ratio'] = 1.0
            features_dict[first_date]['barcount_ratio'] = 1.0
            
            features_dict[first_date]['candle_shape'] = 0.0
            features_dict[first_date]['candle_range'] = 0.0
            features_dict[first_date]['ema13_range'] = 0.0
            features_dict[first_date]['d1_ema13_range']=0.0
            features_dict[first_date]['d2_ema13_range']=0.0
            
            if candle_size !=390:
                features_dict[first_date]['last_candle'] = int(0)
                features_dict[first_date]['overnight_candle'] = int(0)
                features_dict[first_date]['morning'] = int(0)
                
            features_dict[first_date]['trade_ratio'] = 1
            features_dict[first_date]['friday'] = int(0)   
            
            if candle_size==390:
                features_dict[first_date]['trade_ratio']=1.0
                features_dict[first_date]['friday']=0
                features_dict[first_date]['vwap']=first_close
                features_dict[first_date]['vwap_ratio']=1.0
                
                features_dict[first_date]['ema13_oc_gains'] = 1
                features_dict[first_date]['d1_ema13_oc_gains']=0
                features_dict[first_date]['oc_gains']= 1.0
    
            break
    


def update_features(update_dict,date_list,bb_queue,price_q,volume_q,x=5,candle_size=1):
    
    count = 0
    first = True
    for live_date in date_list:
        count = count + 1
        if count >= x:
            live_row = update_dict[live_date]
            if first:
                first = False
                last_row = update_dict[live_date]
        
            else:
                compute_features_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size,approx=False)
    
            last_row = update_dict[live_date]


def update_30m_features(update_dict,date_list,bb_queue,price_q,volume_q,candle_size=30):
    
    if candle_size !=30:
        raise Exception('wrong candle size')
        
    first = True
    update=False
    for live_date in date_list:
        
        if isinstance(live_date,str):
            temp = datetime.datetime.strptime(live_date, '%Y-%m-%d %H:%M:%S')
            hour = temp.hour
        else:
            hour = live_date.hour
        
        if first:
            first = False
            last_row = update_dict[live_date]
            
            
        if hour==8:
            update=True
            
        
        if update:
            #print('updating {}'.format(live_date))
            live_row = update_dict[live_date]
            compute_features_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size,approx=False)
        
        last_row = update_dict[live_date]
        
     
        
def update_390m_features(update_dict,date_list,bb_queue,price_q,volume_q,candle_size=390):
    
    if candle_size !=390:
        raise Exception('wrong candle size')
        
    first = True
    for live_date in date_list:
        if first:
            first = False
            last_row = update_dict[live_date]
        
        else:
            #print('updating {}'.format(live_date))
            live_row = update_dict[live_date]
            compute_features_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size,approx=False)
        
        last_row = update_dict[live_date]



def approximate_390m_features(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size=390,approx=True):
    
    if candle_size !=390:
        raise Exception('wrong candle size')
        
    compute_features_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size,approx)
        
        




def compute_test_row(last_row,live_row,live_date,bb_queue,price_q,volume_q,candle_size=1,approx=False,approx_ratio=1.0): 
    '''
    Parameters
    ----------
    df : The dataframe that contains the features data.
    last_row : last row of data which is needed for most computation
    live_row : The row that needs to be updated.
    last_date : The index of the previous row
    live_date : The index of live_row in df
    bb_queue : the bollinger band queue needed for the normalize_bb calculation
    candle_size : candle_size
    approx: Wether the computation are an approximation
    approx_ratio:  60/current_second enable us to approximate the average cash. 

    This function computes most of the features needed for live prediction.
    '''
    
    
    live_row['average_cash'] = fc.dict_average_cash(last_row, live_row,approx_ratio)
    live_row['gains'] = fc.dict_gains(last_row, live_row)
    
    gains3,gains5,gains10,gains20 = fc.dict_gains_x(live_row,bb_queue) #the bb_queue contains the price of the last 20 candles
    live_row['gains3'] = gains3
    live_row['gains5'] = gains5
    live_row['gains10'] = gains10
    live_row['gains20'] = gains20
    
    live_row['avg_gain'] = fc.dict_avg_gain(last_row,live_row)
    live_row['avg_loss'] = fc.dict_avg_loss(last_row,live_row)
    live_row['RSI'] = fc.dict_RSI(last_row,live_row)
    live_row['d1_ema3_RSI'] = fc.dict_d1_ema3_RSI(last_row,live_row)
    live_row['d2_ema3_RSI'] = fc.dict_d2_ema3_RSI(last_row,live_row)
    
    live_row['normalize_bb'] = fc.dict_normalize_bb(last_row,live_row,bb_queue,approx)
    live_row['d1_ema3_bb'] = fc.dict_d1_ema3_bb(last_row,live_row)
    live_row['d2_ema3_bb'] = fc.dict_d2_ema3_bb(last_row,live_row)
    
    live_row['ema12'] = fc.dict_ema12(last_row,live_row)
    live_row['ema26'] = fc.dict_ema26(last_row,live_row)
    live_row['PPO_line'] = fc.dict_PPO_line(last_row,live_row)
    live_row['d1_ema3_PPO_line'] = fc.dict_d1_ema3_PPO_line(last_row,live_row)
    live_row['d2_ema3_PPO_line'] = fc.dict_d2_ema3_PPO_line(last_row,live_row)
    live_row['signal_line'] = fc.dict_signal_line(last_row,live_row)
    live_row['PPO_histo'] = fc.dict_PPO_histo(last_row,live_row)
    live_row['d1_ema3_PPO_histo'] = fc.dict_d1_ema3_PPO_histo(last_row,live_row)
    live_row['d2_ema3_PPO_histo'] = fc.dict_d2_ema3_PPO_histo(last_row,live_row)
    
    live_row['ema13_cash'] = fc.dict_ema13_cash(last_row,live_row)
    live_row['d1_ema13_cash'] = fc.dict_d1_ema13_cash(last_row,live_row)
    live_row['d2_ema13_cash'] = fc.dict_d2_ema13_cash(last_row,live_row)
  
    live_row['candle_shape'] = fc.dict_candle_shape(last_row,live_row)
    live_row['candle_range'] = fc.dict_candle_range(last_row,live_row)
    
    # added recently
    live_row['ema13_range'] = fc.dict_ema13_range(last_row,live_row)
    live_row['d1_ema13_range'] = fc.dict_d1_ema13_range(last_row,live_row)
    live_row['d2_ema13_range'] = fc.dict_d2_ema13_range(last_row,live_row)
    live_row['hl_range20'] = fc.dict_hl_range20(live_row,bb_queue,approx) # the bb_queue contains the price of the last 20 candles
    
   
    live_row['overnight_candle'] = fc.overnight_candle(live_date,candle_size)
    
    live_row['vwap'] = fc.dict_vwap(live_row,price_q,volume_q,approx)
    live_row['vwap_ratio'] = fc.dict_vwap_ratio(live_row)
    
    live_row['no_trade_count'] = fc.no_trade_count(live_row,volume_q,approx)





def compute_intraday(last_row,live_row,bb_queue,date_index):
    
    '''
    Last row is the previous day daily row.
    live_row is the actual intraday row.
    bb_queue is the daily bb_queue. This function does not modify the queue.
    date_index is the intraday index used to compute time index.
    '''
    
    live_row['previous_gains'] = last_row['gains']
     
    live_row['avg_gain'] = fc.dict_avg_gain(last_row,live_row)
    live_row['avg_loss'] = fc.dict_avg_loss(last_row,live_row)
    live_row['RSI'] = fc.dict_RSI(last_row,live_row)
    live_row['d1_ema3_RSI'] = fc.dict_d1_ema3_RSI(last_row,live_row)
    live_row['d2_ema3_RSI'] = fc.dict_d2_ema3_RSI(last_row,live_row)
    
    live_row['normalize_bb'] = fc.dict_normalize_bb(last_row,live_row,bb_queue,approx=True)
    live_row['d1_ema3_bb'] = fc.dict_d1_ema3_bb(last_row,live_row)
    live_row['d2_ema3_bb'] = fc.dict_d2_ema3_bb(last_row,live_row)
    
    live_row['ema12'] = fc.dict_ema12(last_row,live_row)
    live_row['ema26'] = fc.dict_ema26(last_row,live_row)
    live_row['PPO_line'] = fc.dict_PPO_line(last_row,live_row)
    live_row['d1_ema3_PPO_line'] = fc.dict_d1_ema3_PPO_line(last_row,live_row)
    live_row['d2_ema3_PPO_line'] = fc.dict_d2_ema3_PPO_line(last_row,live_row)
      
    live_row['signal_line'] = fc.dict_signal_line(last_row,live_row)
    live_row['PPO_histo'] = fc.dict_PPO_histo(last_row,live_row)
    live_row['d1_ema3_PPO_histo'] = fc.dict_d1_ema3_PPO_histo(last_row,live_row)
    live_row['d2_ema3_PPO_histo'] = fc.dict_d2_ema3_PPO_histo(last_row,live_row)
    
    live_row['vwap_ratio'] = live_row['close']/last_row['vwap']
    live_row['time_index'] = fc.find_time_index(date_index)



def compute_live_intraday(last_row,live_row,bb_queue,date_index):
    
    '''
    Last row is the previous day daily row.
    live_row is the actual intraday row.
    bb_queue is the daily bb_queue. This function does not modify the queue.
    date_index is the intraday index used to compute time index.
    '''
    
    live_row['previous_gains'] = last_row['gains']
     
    live_row['daily_avg_gain'] = fc.dict_avg_gain(last_row,live_row)
    live_row['daily_avg_loss'] = fc.dict_avg_loss(last_row,live_row)
    live_row['daily_RSI'] = fc.intraday_daily_dict_RSI(last_row,live_row)
    live_row['daily_d1_ema3_RSI'] = fc.intraday_daily_dict_d1_ema3_RSI(last_row,live_row)
    live_row['daily_d2_ema3_RSI'] = fc.intraday_daily_dict_d2_ema3_RSI(last_row,live_row)
    
    
    live_row['daily_normalize_bb'] = fc.dict_normalize_bb(last_row,live_row,bb_queue,approx=True)
    live_row['daily_d1_ema3_bb'] = fc.intraday_daily_dict_d1_ema3_bb(last_row,live_row)
    live_row['daily_d2_ema3_bb'] = fc.intraday_daily_dict_d2_ema3_bb(last_row,live_row)
    
    live_row['daily_ema12'] = fc.dict_ema12(last_row,live_row)
    live_row['daily_ema26'] = fc.dict_ema26(last_row,live_row)
    live_row['daily_PPO_line'] = fc.intraday_daily_dict_PPO_line(last_row,live_row)
    live_row['daily_d1_ema3_PPO_line'] = fc.intraday_daily_dict_d1_ema3_PPO_line(last_row,live_row)
    live_row['daily_d2_ema3_PPO_line'] = fc.intraday_daily_dict_d2_ema3_PPO_line(last_row,live_row)
      
    live_row['daily_signal_line'] = fc.intraday_daily_dict_signal_line(last_row,live_row)
    live_row['daily_PPO_histo'] = fc.intraday_daily_dict_PPO_histo(last_row,live_row)
    live_row['daily_d1_ema3_PPO_histo'] = fc.intraday_daily_dict_d1_ema3_PPO_histo(last_row,live_row)
    live_row['daily_d2_ema3_PPO_histo'] = fc.intraday_daily_dict_d2_ema3_PPO_histo(last_row,live_row)
    
    live_row['daily_vwap_ratio'] = live_row['close']/last_row['vwap']
    live_row['time_index'] = fc.find_time_index(date_index)





















# end
    
    