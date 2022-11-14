# -*- coding: utf-8 -*-
"""
Created on Wed Dec  8 16:27:21 2021

@author: marti
"""



#import time as t
from xgboost import XGBRegressor


class live_models():
    
    '''
    The sole purpose of this class is to make the live predictions.
    '''
    
    
    def __init__(self):
        self.features_1m = self.get_features(candle_size=1)
        self.features_390m = self.get_features(candle_size=390)
        
        self.target_dict_1m =self.get_target_dict(candle_size=1) 
        self.target_dict_390m =self.get_target_dict(candle_size=390) 
        
        self.model_dict_1m =self.load_models(candle_size=1)
        self.model_dict_390m =self.load_models(candle_size=390)  
        
        
    def get_features(self,candle_size):
        
        if candle_size==1:
            
            features = ['gains','gains3','gains5','gains10','gains20','RSI','d1_ema3_RSI','d2_ema3_RSI','normalize_bb','d1_ema3_bb','d2_ema3_bb',
                        'PPO_line','d1_ema3_PPO_line','d2_ema3_PPO_line','ema13_cash','d1_ema13_cash','d2_ema13_cash','candle_shape',
                        'candle_range','ema13_range','d1_ema13_range','d2_ema13_range','hl_range20','overnight_candle','vwap_ratio',
                        'no_trade_count','green_line']    
            
        elif candle_size==390:
            features = ['average','gains','RSI','d1_ema3_RSI','d2_ema3_RSI','normalize_bb','d1_ema3_bb','d2_ema3_bb',
                           'PPO_line','d1_ema3_PPO_line','d2_ema3_PPO_line','PPO_histo','d1_ema3_PPO_histo',
                           'd2_ema3_PPO_histo','ema13_cash','d1_ema13_cash','d2_ema13_cash',
                           'candle_shape','candle_range','ema13_range','d1_ema13_range','d2_ema13_range',
                           'friday','vwap_ratio','ema13_oc_gains','d1_ema13_oc_gains','oc_gains'] 
            
        return features


    def get_target_dict(self,candle_size):
        if candle_size==1:
            target_dict ={'highest_ng20':'pred_high20',
                           'lowest_ng20':'pred_low20',
                           'hl_ratio20':'pred_hl_ratio20',
                           'next_gain20':'pred_ng20'}
             
        elif candle_size==390:
             target_dict = {'next_gain':'pred_next_gain',
                            'next_oc':'pred_next_oc',
                           'next_high':'pred_next_high',
                           'next_low':'pred_next_low',
                           'hl_ratio':'pred_hl_ratio'}
             
        return target_dict


    def compute_hl_ratio(self,live_df):
        live_df['hl_ratio'] = live_df['pred_high20']+live_df['pred_low20']-1.0
        
       
        
    def load_models(self,candle_size):

        model_dict = {}
        target_dict = self.get_target_dict(candle_size)
        for model_name in target_dict:
            model = XGBRegressor(n_jobs=6)
            model.load_model(model_name)
            model_dict[model_name] = model
            #print(model_name)
            
        return model_dict


    def predict_1m(self,live_df):
        #temps = t.time()
        
        for target in self.target_dict_1m:
            pred_col = self.target_dict_1m[target]
            model = self.model_dict_1m[target]
            live_df[pred_col] = model.predict(live_df[self.features_1m])
                
        self.compute_hl_ratio(live_df)  
        
        #temps = t.time()-temps
        #print('predict 1 min tooks {} seconds'.format(temps))
          
                
    def predict_390m(self,daily_df):
        #temps = t.time()
        
        for target in self.target_dict_390m:
            pred_col = self.target_dict_390m[target]
            model = self.model_dict_390m[target]
            daily_df[pred_col] = model.predict(daily_df[self.features_390m])
                
        #self.compute_daily_hl_ratio(daily_df) 
        
        #temps = t.time()-temps
        #print('predict 390 min tooks {} seconds'.format(temps))
                
           
 
        
        
        
        
        
        
        
#end        
        
        
        
        