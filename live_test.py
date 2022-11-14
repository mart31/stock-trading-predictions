# -*- coding: utf-8 -*-
"""
Created on Tue Jul 20 06:59:42 2021

@author: marti
"""


import features_data
import os
import features_creator as fc
import compute_module as cm

#import iq_database as iqd
#import db_manager
import db_histo_manager as dhm
import date_manager
import live_models

import otc_object
import live_object

from xgboost import XGBRegressor

import datetime
import time as t

import pandas as pd
import math
import queue
import numpy as np
import features_creator as fc
import live_test_object
import american_object


'''
This module test that each of the data point pulled during the live sessions are within
plus or minus 1% of the historical data. 
'''


def get_db_manager(db_name):
    
    if db_name=='otc_database':
        data_shape = otc_object.otc_object()
    elif db_name=='live_database':
        data_shape = live_object.live_object()
    elif db_name=='live_test_database':
        data_shape = live_test_object.live_test_object()
    elif db_name=='american_database':
           data_shape = american_object.american_object()
        
    dbm = dhm.db_manager(db_name,data_shape)
    
    return dbm


def get_test_features():
    
    features = ['ema13_cash','open','close','high','low','volume',
                    'gains','gains3','gains5','gains10','gains20','RSI','normalize_bb',
                    'PPO_line','hl_range20','overnight_candle','vwap_ratio','green_line'] 
    
    
    return features


def get_pred_col():
    pred_col=['pred_high20','pred_hl_ratio20','pred_ng20']
    return pred_col
    

def get_target_dict(candle_size=1):
    if candle_size==1:
        target_dict ={'pred_high20':'highest_ng20',
                       'pred_hl_ratio20':'hl_ratio20',
                       'pred_ng20':'next_gain20'}
        
    return target_dict
        

def get_pred_threshold(candle_size=1):
    if candle_size==1:
        threshold_dict ={'pred_high20':1.06,
                       'pred_hl_ratio20':1.03,
                       'pred_ng20':1.03}
        
    return threshold_dict
    
    
    
      
    
    
def compare_two_df_features(df_live,df_histo,col_list,ratio = 1.01):

    anomalies_dict = {}
    #col_list = get_test_features()
    
    for index,row in df_live.iterrows():
        #print(type(index))
        a_list = []
        for col in col_list:
            first_value = row[col]
            second_value = df_histo.loc[index,col]
            #print(second_value)
            if second_value==0:
                first_value = first_value + 1
                second_value = second_value+1
                test_ratio = first_value/second_value
                
                if test_ratio>ratio:
                    a_list.append(col)
                elif test_ratio < (1/ratio):
                    a_list.append(col)
            else:
                test_ratio = first_value/second_value
                
                if test_ratio>ratio:
                    a_list.append(col)
                elif test_ratio < (1/ratio):
                    a_list.append(col)
                    
        if len(a_list)>0:
            anomalies_dict[index]=a_list
            
    if len(anomalies_dict)>0:
        volume = df_live['volume'].sum()
        average_price = df_live['close'].mean()  
        anomalies_dict['volume_sum']=volume 
        anomalies_dict['average_price']= average_price
        anomalies_dict['total_cash']=average_price*volume
        
        zero_trade = df_live.loc[df_live['volume']==0]
        activity = (len(df_live)-len(zero_trade))/len(df_live)
        anomalies_dict['activity'] = activity              
    
    return anomalies_dict


def compare_two_df_prices(df1,df2,ratio = 1.01):

    anomalies_dict = {}
    #col_list = ['open','high','low','close','volume']
    col_list = ['open','high','low','close']
    
    for index,row in df1.iterrows():
        
        a_list = []
        for col in col_list:
            first_value = row[col]
            second_value = df2.loc[index,col]
            
            if second_value==0:
                first_value = first_value + 1
                second_value = second_value + 1
                test_ratio = first_value/second_value
                
                if test_ratio>ratio:
                    a_list.append(col)
                elif test_ratio < (1/ratio):
                    a_list.append(col)
            else:
                test_ratio = first_value/second_value
                
                if test_ratio>ratio:
                    a_list.append(col)
                elif test_ratio < (1/ratio):
                    a_list.append(col)
                    
        if len(a_list)>0:
            anomalies_dict[index]=a_list
            
    if len(anomalies_dict)>0:
        volume= df1['volume'].sum()
        average_price = df1['close'].mean()
        zero_trade = df1.loc[df1['volume']==0]
        activity = (len(df1)-len(zero_trade))/len(df1)
        
        anomalies_dict['volume_sum']=volume 
        anomalies_dict['average_price']= average_price
        anomalies_dict['total_cash']=average_price*volume
        anomalies_dict['activity'] = activity              
    
    return anomalies_dict



def get_test_data(last_date,db_name):
    
    dh  = date_manager.date_handler()
    if last_date=='':
        last_date = dh.get_last_market_date().to_pydatetime()
        
    #live_dbm = db_manager.db_manager('live_database')
    #iq_dbm = db_manager.db_manager('iq_database')
    
    live_dbm = get_db_manager('live_database')
    iq_dbm = get_db_manager(db_name)
    
    #live_day = live_dbm.download_specific_day(last_date)
    #histo_day = iq_dbm.download_specific_day(last_date)
    
    table='FEATURES_1M'
    live_day = live_dbm.download_specific_day(last_date,table,cond_list=[],data_struct='df')
    
    histo_day = iq_dbm.download_specific_day(last_date,table,cond_list=[],data_struct='df')
    #histo_day = iq_dbm.download_combined_specific_day(last_date)
    
    ticker_list = live_day['ticker'].unique()
    
    lm = live_models.live_models() 
    lm.predict_1m(histo_day)
    
    return live_day,histo_day,ticker_list


def get_anomalies(live_day,histo_day,ticker_list):
    anomalies_dict = {}
    
    col_list = get_test_features()
    for ticker in ticker_list:
        
        live_df = live_day.loc[live_day['ticker']==ticker]
        histo_df = histo_day.loc[histo_day['ticker']==ticker]
        
        live_sum_vol = live_df['volume'].sum()
        histo_sum_vol = histo_df['volume'].sum()
        
        live_df.set_index('date',inplace=True)
        histo_df.set_index('date',inplace=True)
        
        if live_sum_vol>0:
            try:
                anomalies = compare_two_df_features(live_df, histo_df,col_list)
                if len(anomalies)>0:
                    anomalies_dict[ticker]=anomalies
                
                
            except Exception as e:
            
                print('e: {},ticker: {},live_trade: {}, histo_trade: {}'.format(e,ticker,live_sum_vol,histo_sum_vol))
                
                break
            
    anomalies_df = {}
    
    col_list = get_test_features()
    col=['date']
    
    for c in col_list:
        col.append(c)
    
    if len(anomalies_dict)>0:
        
        for ticker in anomalies_dict:
            live_df = live_day.loc[live_day['ticker']==ticker]
            histo_df = histo_day.loc[histo_day['ticker']==ticker]
            
            live_df = live_df[col]
            histo_df = histo_df[col]
            
            live_df.set_index('date',inplace=True)
            histo_df.set_index('date',inplace=True)
            
            anomalies_df[ticker]={}
            anomalies_df[ticker]['live_df'] = live_df
            anomalies_df[ticker]['histo_df'] = histo_df
            #anomalies_df[ticker]['live_df'] = live_day.loc[live_day['ticker']==ticker]
            #anomalies_df[ticker]['histo_df'] = histo_day.loc[histo_day['ticker']==ticker]
            
    return anomalies_dict,anomalies_df



def get_price_anomalies(live_day,histo_day,ticker_list):
    anomalies_dict = {}
    
    col_list = get_test_features()
    for ticker in ticker_list:
        
        live_df = live_day.loc[live_day['ticker']==ticker]
        histo_df = histo_day.loc[histo_day['ticker']==ticker]
        
        live_sum_vol = live_df['volume'].sum()
        histo_sum_vol = histo_df['volume'].sum()
        
        live_df.set_index('date',inplace=True)
        histo_df.set_index('date',inplace=True)
        
        if live_sum_vol>0:
            try:
                
                anomalies = compare_two_df_prices(live_df,histo_df)
                if len(anomalies)>0:
                    anomalies_dict[ticker]=anomalies
                
                
            except Exception as e:
            
                print('e: {},ticker: {},live_trade: {}, histo_trade: {}'.format(e,ticker,live_sum_vol,histo_sum_vol))
                
                break
            
    anomalies_df = {}
    
    col_list = get_test_features()
    col=['date']
    
    for c in col_list:
        col.append(c)
    
    if len(anomalies_dict)>0:
        
        for ticker in anomalies_dict:
            live_df = live_day.loc[live_day['ticker']==ticker]
            histo_df = histo_day.loc[histo_day['ticker']==ticker]
            
            live_df = live_df[col]
            histo_df = histo_df[col]
            
            live_df.set_index('date',inplace=True)
            histo_df.set_index('date',inplace=True)
            
            anomalies_df[ticker]={}
            anomalies_df[ticker]['live_df'] = live_df
            anomalies_df[ticker]['histo_df'] = histo_df
            #anomalies_df[ticker]['live_df'] = live_day.loc[live_day['ticker']==ticker]
            #anomalies_df[ticker]['histo_df'] = histo_day.loc[histo_day['ticker']==ticker]
            
    return anomalies_dict,anomalies_df



def get_pred_anomalies(live_day,histo_day,ticker_list):
    pred_anomalies_dict = {}
    
    ratio = 1.005 # this is the anomalies ratio the ratio is low as to capture potential anomalies. 
    
    col_list = get_pred_col()
    for ticker in ticker_list:
        
        live_df = live_day.loc[live_day['ticker']==ticker]
        histo_df = histo_day.loc[histo_day['ticker']==ticker]
        
        live_sum_vol = live_df['volume'].sum()
        histo_sum_vol = histo_df['volume'].sum()
        
        live_df.set_index('date',inplace=True)
        histo_df.set_index('date',inplace=True)
        
        if live_sum_vol>0:
            try:
                anomalies = compare_two_df_features(live_df, histo_df,col_list,ratio)
                if len(anomalies)>0:
                    pred_anomalies_dict[ticker]=anomalies
                
                
            except Exception as e:
            
                print('e: {},ticker: {},live_trade: {}, histo_trade: {}'.format(e,ticker,live_sum_vol,histo_sum_vol))
                
                break
            
    pred_anomalies_df = {}
    if len(pred_anomalies_dict)>0:
        
        for ticker in pred_anomalies_dict:
            
            live_df = live_day.loc[live_day['ticker']==ticker]
            histo_df = histo_day.loc[histo_day['ticker']==ticker]
            
            live_df.set_index('date',inplace=True)
            histo_df.set_index('date',inplace=True)
            
            pred_anomalies_df[ticker]={}
            pred_anomalies_df[ticker]['live_df'] = live_df
            pred_anomalies_df[ticker]['histo_df'] = histo_df
        
    return pred_anomalies_dict,pred_anomalies_df



def print_predictions_time(last_date,live_day):
       
    min_volume = 1
    min_ema_cash = 10000
    min_hl_ratio = 1.03
    
    pred_df = live_day.loc[(live_day['volume']>=min_volume)]
    pred_df = pred_df.loc[(pred_df['ema13_cash']>=min_ema_cash)]
    pred_df = pred_df.loc[(pred_df['pred_hl_ratio20']>=min_hl_ratio)]
    
    dh  = date_manager.date_handler()
    if last_date=='':
        last_date = dh.get_last_market_date().to_pydatetime()
    
    noon_date = last_date.replace(hour=12,minute=0)
    afternoon_date = noon_date.replace(hour=15)
    
    morning = 0
    afternoon = 0
    eod = 0
    
    for index,row in pred_df.iterrows():
        date_str = row['date']
        candle_date = datetime.datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        
        if candle_date<noon_date:
            morning = morning+1
        elif candle_date<afternoon_date:
            afternoon = afternoon + 1
        else:
            eod = eod + 1
        
    if len(pred_df)>0:
        length = len(pred_df)
        morning_percent = round((morning/length)*100,2)
        afternoon_percent = round((afternoon/length)*100,2)
        eod_percent = round((eod/length)*100,2)
        
        print('')
        print('stocks spotted by the algorithm: {}'.format(len(pred_df)))
        print('morning: {}% of the picks, {} stocks'.format(morning_percent,morning))
        print('afternoon: {}% of the picks, {} stocks'.format(afternoon_percent,afternoon))
        print('eod: {}% of the picks, {} stocks'.format(eod_percent,eod))
        print('')
    else:
        print('there was no prediction on {}'.format(last_date))



def get_pred_dict(last_date,min_avg_cash=5000):
    
    #live_dbm = db_manager.db_manager('live_database')
    live_dbm = get_db_manager('live_database')

    table='FEATURES_1M'
    live_dict = live_dbm.download_specific_day(last_date,table,cond_list=[],data_struct='dict')
    #live_dict = live_dbm.download_specific_day(last_date,data_struct='dict',candle_size=1)
    #data = live_dbm.download_specific_day(last_date,data_struct='data',candle_size=1)
        
    
    for ticker in live_dict:
        ticker_dict = live_dict[ticker]
         
        fc.dict_add_next_gain_20(ticker_dict)
        
        fc.init_highest_ng(ticker_dict,ng=20)
        fc.init_lowest_ng(ticker_dict,ng=20)
        fc.init_hl_ratio(ticker_dict,ng=20) 
    
    
    #min_avg_cash = 7000
    pred_dict={}
    
    pred_col = get_pred_col()
    
    for pred in pred_col:
        pred_dict[pred]={}
    
    pred_threshold = get_pred_threshold()
    
    
    for pred in pred_dict:
        for ticker in live_dict:
            ticker_dict = live_dict[ticker]
            for d in ticker_dict:
                candle = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
                
                pred_time = False
                if candle.hour<15:
                    pred_time = True
                elif candle.hour==15 and candle.minute<40:
                    pred_time = True
                    
                if pred_time:
                    row = ticker_dict[d]
                    prediction = row[pred]
                    threshold = pred_threshold[pred]
                    ema_cash = row['ema13_cash']
                    
                    if (prediction is not None) and ema_cash>=min_avg_cash:
                        
                        if prediction >=threshold:
                            if ticker not in pred_dict[pred]:
                                pred_dict[pred][ticker]={}
                            
                            pred_dict[pred][ticker][d]=row
                            
    return pred_dict



def print_predictions_evaluation(pred_dict):
    target_dict = get_target_dict()
    
    for pred_col in pred_dict:
        target = target_dict[pred_col]
        
        pred_avg = 0
        true_avg = 0
        count = 0
        
        total_profit=0
        
        for ticker in pred_dict[pred_col]:
            for d in pred_dict[pred_col][ticker]:
                row = pred_dict[pred_col][ticker][d]
                
                true_value = row[target]
                true_avg = true_avg + true_value
                
                pred_value = row[pred_col]
                pred_avg = pred_avg + pred_value
                
                avg_cash = pred_dict[pred_col][ticker][d]['ema13_cash']
                profit = (avg_cash/5)*(true_value-1.0)
                total_profit = profit + total_profit
                
                count = count + 1
         
        if count>0:    
            true_avg = true_avg/count
            pred_avg = pred_avg/count
                
        true_avg = round(true_avg,4)
        pred_avg = round(pred_avg,4)
        total_profit = round(total_profit,0)
                
        print('')
        print('{}: True mean: {}, Pred mean: {}, nbr stocks: {}, profit: {}'.format(target,true_avg,pred_avg,count,total_profit))
        print('')


def count_pred_caught(histo_day,live_day,min_cash=10000):
    
    '''
    We verify that we do catch the majority of the potential trade in a day.
    '''
    
    test_col = ['date','ticker','close','ema13_cash','pred_high20','pred_ng20','pred_hl_ratio20']
    
    test_histo = histo_day[test_col]  
    test_live = live_day[test_col]
    #min_cash=7000
    df_histo = test_histo.loc[test_histo['ema13_cash']>=min_cash]  
    df_live = test_live.loc[test_live['ema13_cash']>=min_cash] 
    
    bet=1.03
     
    pred_histo = df_histo.loc[df_histo['pred_hl_ratio20']>=bet] 
    pred_live = df_live.loc[df_live['pred_hl_ratio20']>=bet] 
    
    length_histo = len(pred_histo)
    length_live = len(pred_live)
    
    if length_histo>0:
        percent_caught = round(100*length_live/length_histo,2)
    else:
        percent_caught=0
    
    print('')
    print('Trade caught: {} out of {} which is {}%'.format(length_live,length_histo,percent_caught))
    print('')
    
    return percent_caught


def remove_illiquid(live_day,ticker_list,min_cash=300000):

    new_list = []
    for ticker in ticker_list:
        live_df = live_day.loc[live_day['ticker']==ticker]
        
        total_vol = live_df['volume'].sum()
        
        avg_price = live_df['close'].mean()
    
        daily_cash = total_vol*avg_price
        
        if daily_cash>=min_cash:
            new_list.append(ticker)
    
    return new_list


def test_combined_date(histo_day):
    for index,row in histo_day.iterrows():
        date = row['date']
        intraday_daily_date = row['INTRADAY_DAILY_date']
        
        if date!=intraday_daily_date:
            print('oh my god NOOOOOOO!!!!')
            print(date)


def save_stock_list(db_name):
    dbm = get_db_manager(db_name)
    
    stock_list = dbm.download_stock_list()
    name = 'stock_list '+db_name+'.csv'
    
    stock_list.to_csv(name)
    print('Saved {} stocks in a csv file'.format(len(stock_list)))
    
    
def save_this_week_prediction(db_name):
    
    '''
    This method do not verify if the data has already been saved for this week. 
    '''
    
    dbm = get_db_manager(db_name)
    live_test_dbm= get_db_manager('live_test_database')
    features = live_test_dbm.data_shape.features
    
    dh  = date_manager.date_handler()
    date_list = dh.this_week_date_list()
    
    a =['close','>=','0.002']
    b =['ema13_cash','>=','10000']
    c =['average_cash','>=','10000']
    
    cond_list = [a,b,c]
    table='FEATURES_1M'
    
    for d in date_list:
        date_morning = d.to_pydatetime()
        df = dbm.download_combined_specific_day(date_morning,cond_list=cond_list)
        lm = live_models.live_models() 
        lm.predict_1m(df)
        
        data_df = df[features]
        data_df.set_index('date',inplace=True)
        
        live_test_dbm.save_multiple_ticker(data_df,table)
        
        
def get_this_week_lm_data(data_struct='df'):
                       
    live_test_dbm = get_db_manager('live_database')
    dh  = date_manager.date_handler()
    date_list = dh.this_week_date_list()            
    
    data_dict = {}
    
    table='FEATURES_1M'
    for d in date_list:
        date_morning = d.to_pydatetime()
        df = live_test_dbm.download_specific_day(date_morning, table,data_struct=data_struct)
        data_dict[d]=df
        
    return data_dict


def get_this_week_caught_data(data_struct='df'):
    
    a =['close','>=','0.002']
    b =['ema13_cash','>=','10000']
    c =['average_cash','>=','10000']
    
    cond_list = [a,b,c]
                       
    live_dbm = get_db_manager('live_database')
    dh  = date_manager.date_handler()
    date_list = dh.this_week_date_list()            
    
    data_dict = {}
    
    table='FEATURES_1M'
    for d in date_list:
        date_morning = d.to_pydatetime()
        df = live_dbm.download_specific_day(date_morning, table,cond_list=cond_list,data_struct=data_struct)
        
        if data_struct=='df':
            df.dropna(inplace=True)
        data_dict[d]=df
        
    return data_dict




def compute_daily_analysis(daily_dict,caught_daily_dict,target,pred_col,bet,bet_size):
    
    all_pred =0
    all_true=0
    all_count=0
    
    caught_pred=0
    caught_true=0
    caught_count=0
    
    max_profit=0
    all_profit=0
    caught_profit =0
    
    for ticker in daily_dict:
        for d in daily_dict[ticker]:
            all_row = daily_dict[ticker][d]
            if all_row[target] is not None:
                all_prediction = all_row[pred_col]
                if all_prediction>=bet:
                    all_count = all_count+1
                    all_pred = all_pred+all_prediction
                    
                    true_value = all_row[target]
                    
                    #print(ticker)
                    #print(d)
                    #print(all_prediction)
                    
                    all_true = all_true+true_value
                    
                    ema_cash = all_row['ema13_cash']
                    
                    max_profit = max_profit +((ema_cash/5)*(true_value-1.0))
                    
                    all_profit = all_profit + (bet_size*(true_value-1.0))
                    
                    if ticker in caught_daily_dict:
                        if d in caught_daily_dict[ticker]:
                            caught_row = caught_daily_dict[ticker][d]
                            caught_prediction = caught_row[pred_col]
                            
                            if caught_prediction is not None:
                                if caught_prediction>=bet:
                                    caught_count = caught_count+1
                                    
                                    caught_pred = caught_pred + caught_prediction
                                    caught_true = caught_true + true_value
                                    
                                    caught_profit = caught_profit + (bet_size*(true_value-1.0))
       
    if all_count>0:
        all_pred = all_pred/all_count
        all_true = all_true/all_count
    
    if caught_count>0:
        caught_pred = caught_pred/caught_count
        caught_true = caught_true/caught_count 
    
    return all_pred,all_true,all_count,caught_pred,caught_true,caught_count,max_profit,all_profit,caught_profit




def create_weekly_analysis(bet=1.03,bet_size=2000,save=True):          

    all_data_dict = get_this_week_lm_data(data_struct='dict')
    caught_dict = get_this_week_caught_data(data_struct='dict')
    
    #p = prediction_mean, t = true_mean, a=all_data, c=caught_data
    col = ['date','ap_hl20','at_hl20','a_hl20_count','cp_hl20','ct_hl20','c_hl20_count',
           'ap_ng20','at_ng20','a_ng20_count','cp_ng20','ct_ng20','c_ng20_count','max_profit_hl20','all_profit_hl20','caught_profit_hl20',
           'max_profit_ng20','all_profit_ng20','caught_profit_ng20','percent_caught']
    
    df = pd.DataFrame(columns=col)
    for day in all_data_dict:
        daily_dict = all_data_dict[day]
        caught_daily_dict=caught_dict[day]
    
        target='hl_ratio20'
        pred_col='pred_hl_ratio20'
        
        ap_hl20,at_hl20,a_hl20_count,cp_hl20,ct_hl20,c_hl20_count,max_profit_hl20,all_profit_hl20,caught_profit_hl20= compute_daily_analysis(daily_dict,caught_daily_dict,target,pred_col,bet,bet_size)
        
        target='next_gain20'
        pred_col='pred_ng20'
        
        ap_ng20,at_ng20,a_ng20_count,cp_ng20,ct_ng20,c_ng20_count,max_profit_ng20,all_profit_ng20,caught_profit_ng20= compute_daily_analysis(daily_dict,caught_daily_dict,target,pred_col,bet,bet_size)
        
        
        if a_hl20_count>0:
            percent_caught=c_hl20_count/a_hl20_count
        else:
            percent_caught = 1.0
            
        row = [day,ap_hl20,at_hl20,a_hl20_count,cp_hl20,ct_hl20,c_hl20_count,ap_ng20,at_ng20,a_ng20_count,cp_ng20,ct_ng20,c_ng20_count,
               max_profit_hl20,all_profit_hl20,caught_profit_hl20,max_profit_ng20,all_profit_ng20,caught_profit_ng20,percent_caught]
        
        df.loc[day]=row
        
    if save:
        d = day.date()
        path = 'weekly_analysis/Prediction_analysis/'
        name = '{}_weekly_analysis.csv'.format(d)
        full_path = path+name
        
        df.to_csv(full_path)
        print('Saved {} weekly analysis'.format(day))
        
        
    return df



def print_weekly_result(weekly_df):

    col_list = weekly_df.columns
    
    for index,row in weekly_df.iterrows():
        print('')
        
        for col in col_list:
            print('{} : {}'.format(col,row[col]))



def get_live_bet_df(db_name,pred_col,ema_cash=7000,last_date=''):

    dh  = date_manager.date_handler()
    if last_date=='':
        last_date = dh.get_last_market_date().to_pydatetime()
    
    
    
    # data needed to verify the live data
    live_day,histo_day,ticker_list = get_test_data(last_date,db_name)
    
    target_dict = get_target_dict()
    target = target_dict[pred_col]
    pred_threshold = get_pred_threshold()
    bet = pred_threshold[pred_col]
    
    test_col = get_test_features()
    test_col = [pred_col]+test_col
    
    live_bet = live_day.loc[live_day[pred_col]>=bet]
    live_bet = live_bet.loc[live_bet['ema13_cash']>=ema_cash]
    
    bet_dict = {}
    i = 0
    for index,row in live_bet.iterrows():
        d = row['date']
        ticker=row['ticker']
        
        histo_ticker = histo_day.loc[(histo_day['date']==d)]
        histo_row = histo_ticker.loc[histo_ticker['ticker']==ticker]
        histo_row = histo_row.iloc[0]
        
        bet_dict[i] = {}
        bet_dict[i]['date']=d
        bet_dict[i]['ticker']=ticker
        bet_dict[i]['m_score']=0.0
        bet_dict[i]['target']=target
        bet_dict[i]['true']=histo_row[target]
        #bet_dict[i]['live_pred']=row[pred_col]
        #bet_dict[i]['histo_pred']= histo_row[pred_col]
        
        m_score=0
        for col in test_col:
            live_feature = row[col]
            histo_feature = histo_row[col]
            
            histo_col = 'h_'+col
            bet_dict[i][col]= live_feature
            bet_dict[i][histo_col]=histo_feature
            
            mismatch_col = 'm_'+col
            
            live_f = abs(live_feature)
            histo_f = abs(histo_feature)
            
            if live_f>= 0.0001:
                mismatch = 100*((live_f-histo_f)/live_f)
            else:
                mismatch = 0
                
            bet_dict[i][mismatch_col]=mismatch
            m_score = m_score+mismatch
            
        bet_dict[i]['m_score']=m_score
                
        i = i + 1
        
    
    bet_df = pd.DataFrame.from_dict(bet_dict,orient='index')
    
    if len(bet_df)>0:
        true_gain = bet_df['true'].mean()
        print('{}: True gains for {} is {}'.format(last_date,target,true_gain))
    else:
        print('There was no pick on {}'.format(last_date))

    return bet_df

"""
def get_all_bet_df(db_name,ema_cash=7000):

    pred_col = get_pred_col()
    bet_dict = {}
    
    for col in pred_col:
        bet_df = get_live_bet_df(db_name, col,ema_cash=ema_cash)
        bet_dict[col]=bet_df

    return bet_dict
"""
   
 
def get_all_bet_df(db_name,ema_cash=7000,num_days=5):   
    dh  = date_manager.date_handler()
    calendar = dh.get_market_calendar()
    date_list = calendar.index.to_list()
    date_list = date_list[-num_days:]
    
    pred_col = get_pred_col()
    bet_dict = {}
    
    for d in date_list:
        bet_dict[d]={}
        date_index = d.to_pydatetime()
        for col in pred_col:
            bet_df = get_live_bet_df(db_name, col,ema_cash=ema_cash,last_date=date_index)
            bet_dict[d][col]=bet_df
            
    return bet_dict



def exchange_ratio(exchange_dict,ticker_list,print_info=True):
 
    ratio = {}
    total = len(ticker_list)
    
    for ticker in ticker_list:
        exchange = exchange_dict[ticker]
        
        if exchange in ratio:
            count = ratio[exchange]
            ratio[exchange] = count + 1
            
        else:
            ratio[exchange]=1
            
    
    for exchange in ratio:
        total_count = ratio[exchange]
        r = total_count/total
        r = round(r,3)
        ratio[exchange]=r
    
        if print_info:
            print('Ratio from exchange {} is:{}'.format(exchange,r))


def add_exchange(exchange_dict,df):
    
    df['exchange']=0
    
    for index,row in df.iterrows():
        ticker=row['ticker']
        exchange=exchange_dict[ticker]
        
        df.loc[index,'exchange']=exchange


def evaluate_predictions(db_name,last_date='',ema_cash=7000):

    #last_date=''
    dh  = date_manager.date_handler()
    if last_date=='':
        last_date = dh.get_last_market_date().to_pydatetime()
    
    temps = t.time()
        
    # data needed to verify the live data
    live_day,histo_day,ticker_list = get_test_data(last_date,db_name)
    
    ticker_list = remove_illiquid(live_day,ticker_list)
    
    # feature anomalies
    anomalies_dict,anomalies_df = get_anomalies(live_day, histo_day, ticker_list)
    # prediction anomalies
    
    anomalies_price_dict,anomalies_price_df = get_price_anomalies(live_day,histo_day,ticker_list)
    
    
    pred_anomalies_dict,pred_anomalies_df = get_pred_anomalies(live_day,histo_day,ticker_list)
    # printing the number of prediction and time of day of these predictions.
    print_predictions_time(last_date,live_day)
    
    # get all the prediction above a certain threshold
    pred_dict = get_pred_dict(last_date)
    # print the prediction average relative to the true average for each type of prediction
    print_predictions_evaluation(pred_dict)
    
    # verify if our algorithm catches most trade during the live session
    # note refill price dict does not add predictions. 
    percent_caught = count_pred_caught(histo_day,live_day)
    
    bet_dict = get_all_bet_df(db_name,ema_cash,num_days=10)
    
    temps = t.time()-temps
    
    print('')
    print('Testing the live data tooks {} seconds'.format(temps))
    print('')
    
    summary = {}
    
    summary['live_day']=live_day
    summary['histo_day']=histo_day
    summary['ticker_list']= ticker_list
    summary['anomalies_dict']=anomalies_dict
    summary['anomalies_df']=anomalies_df
    summary['anomalies_price_dict']= anomalies_price_dict
    summary['anomalies_price_df'] = anomalies_price_df
    summary['pred_anomalies_dict']=pred_anomalies_dict
    summary['pred_anomalies_df']=pred_anomalies_df
    summary['pred_dict']=pred_dict
    summary['percent_caught'] = percent_caught
    summary['bet_dict']=bet_dict
    
    return summary





     






# end