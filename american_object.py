# -*- coding: utf-8 -*-
"""
Created on Wed Jul 20 10:19:44 2022

@author: marti
"""


class american_object():
    
    def __init__(self):
        
        self.features = self.create_features_1m()
        self.features_390m = self.create_features_390m()
        self.intraday_daily = self.create_intraday_daily()
        self.stock_info_col = self.create_stock_info_col()
        
        
    def create_stock_info_col(self):
        feat = ['ticker','name','exchange','sec_type','common_share',
                'institutionalPercent','shortInterest']
        
        return feat
        
    
    def create_intraday_daily(self):
        feat = []
        
        feat.append('date')
        feat.append('ticker')
        feat.append('previous_gains')
        
        feat.append('daily_RSI')
        feat.append('daily_d1_ema3_RSI')
        feat.append('daily_d2_ema3_RSI')
        
        feat.append('daily_normalize_bb')
        feat.append('daily_d1_ema3_bb')
        feat.append('daily_d2_ema3_bb')
        
        feat.append('daily_PPO_line')
        feat.append('daily_d1_ema3_PPO_line')
        feat.append('daily_d2_ema3_PPO_line')
        
        feat.append('daily_PPO_histo')
        feat.append('daily_d1_ema3_PPO_histo')
        feat.append('daily_d2_ema3_PPO_histo')
        
        feat.append('daily_vwap_ratio')
        feat.append('time_index')
        
        return feat
        
        
        
    def create_features_1m(self):
        feat = ['date','open','high','low','close','volume','barCount','average','ticker','average_cash','gains']
    
        feat.append('gains3')
        feat.append('gains5')
        feat.append('gains10')
        feat.append('gains20')
        
        feat.append('avg_gain')
        feat.append('avg_loss')
        feat.append('RSI')
        feat.append('d1_ema3_RSI')
        feat.append('d2_ema3_RSI')
        
        feat.append('normalize_bb')
        feat.append('d1_ema3_bb')
        feat.append('d2_ema3_bb')
       
        feat.append('ema12')
        feat.append('ema26')
        feat.append('PPO_line')
        feat.append('d1_ema3_PPO_line')
        feat.append('d2_ema3_PPO_line')
        feat.append('signal_line')
        feat.append('PPO_histo')
        feat.append('d1_ema3_PPO_histo')
        feat.append('d2_ema3_PPO_histo')
       
        feat.append('ema13_cash')
        feat.append('d1_ema13_cash')
        feat.append('d2_ema13_cash')
        
        feat.append('candle_shape')
        feat.append('candle_range')
        
        feat.append('ema13_range')
        feat.append('d1_ema13_range')
        feat.append('d2_ema13_range') # 41 features up to that point
        feat.append('hl_range20')
        
      
        feat.append('overnight_candle')
        feat.append('vwap')
        feat.append('vwap_ratio')
        feat.append('no_trade_count')
        feat.append('green_line')
        #feat.append('volume_ratio20')
        #feat.append('d1_ema13_volume_ratio20')
        #feat.append('d2_ema13_volume_ratio20')

        for x in range(20):
            ng = 'next_gain'+str(x+1)
            feat.append(ng)
            
        
        feat.append('highest_ng20')
        feat.append('lowest_ng20')
        feat.append('hl_ratio20')
        
        return feat
        
        # note date is the index.
        
       
    def create_features_390m(self):
        feat = ['date','open','high','low','close','volume','barCount','average','ticker','average_cash','gains']
    
        feat.append('avg_gain')
        feat.append('avg_loss')
        feat.append('RSI')
        feat.append('d1_ema3_RSI')
        feat.append('d2_ema3_RSI')
        
        feat.append('normalize_bb')
        feat.append('d1_ema3_bb')
        feat.append('d2_ema3_bb')
       
        feat.append('ema12')
        feat.append('ema26')
        feat.append('PPO_line')
        feat.append('d1_ema3_PPO_line')
        feat.append('d2_ema3_PPO_line')
        feat.append('signal_line')
        feat.append('PPO_histo')
        feat.append('d1_ema3_PPO_histo')
        feat.append('d2_ema3_PPO_histo')
       
        feat.append('ema13_cash')
        feat.append('d1_ema13_cash')
        feat.append('d2_ema13_cash')
        feat.append('ema13_barcount')
        feat.append('d1_ema13_barcount')
        feat.append('d2_ema13_barcount')
        feat.append('cash_ratio')
        feat.append('barcount_ratio')
        
        feat.append('candle_shape')
        feat.append('candle_range')
        feat.append('ema13_range')
        feat.append('d1_ema13_range')
        feat.append('d2_ema13_range') # 41 features up to that point
        
        feat.append('trade_ratio')
        feat.append('friday')
        feat.append('vwap')
        feat.append('vwap_ratio')
        feat.append('ema13_oc_gains')
        feat.append('d1_ema13_oc_gains')
        feat.append('oc_gains')
        #feat.append('volume_ratio')
        #feat.append('d1_ema13_volume_ratio')
        #feat.append('d2_ema13_volume_ratio')
        
        feat.append('next_gain')
        feat.append('next_oc')
        feat.append('next_high')
        feat.append('next_low')
        feat.append('next_average')
        
        return feat
# 135 lines of code