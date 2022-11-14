# -*- coding: utf-8 -*-
"""
Created on Tue Dec  7 09:07:17 2021

@author: marti
"""

import numpy as np
import time as t
from xgboost import XGBRegressor
import pandas as pd


import date_manager
#import db_manager
import otc_object
import american_object
import db_histo_manager as dhm



class train_live_model():
    
    def __init__(self,db_name):
        
        #self.dbm = db_manager.db_manager(db_name='iq_database')
        self.db_name = db_name
        self.dbm = self.get_db_manager()
        
        self.dh = date_manager.date_handler()
        self.bet=1.01
        
        
    def get_db_manager(self):
    
        #if self.db_name=='otc_database':
        #    data_shape = otc_object.otc_object() 
        if self.db_name=='american_database':
            data_shape = american_object.american_object()
 
        dbm = dhm.db_manager(self.db_name,data_shape)
        
        return dbm
    
    
    def get_1m_data(self):
   
        
        a =['close','>=','0.002']
        b =['ema13_cash','>=','7000']
        #c =['average_cash','>=','10000']
        d =['gains','<=','1.5']
        cond_list = [a,b,d]
        
        dh = date_manager.date_handler()
        date_list = dh.get_intraday_date_list(keep_2021=True,candle_size=1)
        stock_dict = self.dbm.split_data_by_minute(date_list,cond_list=cond_list)
         
        #stock_dict = self.dbm.download_day_to_day_training_data(print_date=False)
        
        return stock_dict
    
    
    def add_hl_ratio(self,stock_dict):
    
        for day in stock_dict:
            df = stock_dict[day]
            df['hl_ratio'] = df['next_high']+df['next_low']-1.0
    
    
    def get_390m_data(self):
        
        a =['close','>=','0.002']
        b =['ema13_cash','>=','400000']
        c =['average_cash','>=','1000000']
        d =['gains','<=','10']
        cond_list = [a,b,c,d]  
        
        dh = date_manager.date_handler()
        calendar = dh.get_market_calendar(keep_2021=True)
        date_list = calendar.index.to_list()
        
        stock_dict = self.dbm.split_data_by_days(date_list,cond_list=cond_list)
        self.add_hl_ratio(stock_dict)
        
        return stock_dict

    
    def get_model(self,target):
        
        '''
        The next_gain20 and hl_ratio20 models were optimized using the new data split.
        '''
        
        if target=='next_gain20':
            # 20 minute model
            model = XGBRegressor(max_depth = 9,learning_rate=0.19525,n_estimators = 189,gamma=0.0811,reg_lambda=100,objective = 'reg:squaredlogerror',n_jobs=6)

        elif target=='hl_ratio20':
            # 20 minute model
            model = XGBRegressor(max_depth = 17,learning_rate=0.2475,n_estimators = 45,gamma=0.0933,reg_lambda=64,objective = 'reg:squaredlogerror',n_jobs=6)
        elif target=='next_oc':
            # daily model
            model = XGBRegressor(max_depth = 20,learning_rate=0.269135,n_estimators = 433,gamma=0.03,reg_lambda=1,objective = 'reg:squaredlogerror',n_jobs=6)
        elif target=='hl_ratio':
            # daily model
            model = XGBRegressor(max_depth = 7,learning_rate=0.214,n_estimators = 459,gamma=0.0617,reg_lambda=100,objective = 'reg:squaredlogerror',n_jobs=6)

        elif target=='next_high':
            # daily model
            model = XGBRegressor(max_depth = 7,learning_rate=0.214,n_estimators = 459,gamma=0.0617,reg_lambda=100,objective = 'reg:squaredlogerror',n_jobs=6)

        elif target=='next_low':
            # daily model
            model = XGBRegressor(max_depth = 7,learning_rate=0.214,n_estimators = 459,gamma=0.0617,reg_lambda=100,objective = 'reg:squaredlogerror',n_jobs=6)

        elif target=='next_gain':
            # daily model
            model = XGBRegressor(max_depth = 2,learning_rate=0.5465,n_estimators = 30,gamma=0.04265,reg_lambda=45,objective = 'reg:squaredlogerror',n_jobs=6)
       
        else:
            
            '''
            Ironically the most performant highest_ng20 model is the same as the hl_ratio20. Which is
            not that surprising. 
            
            Lowest_ng20 wasnt optimized but is expected to follow the same pattern. 
            '''
            model = XGBRegressor(max_depth = 17,learning_rate=0.2475,n_estimators = 45,gamma=0.0933,reg_lambda=64,objective = 'reg:squaredlogerror',n_jobs=6)
        
        return model
    
    
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
    
    
    
    def add_predictions_using_features(self,model,date_dict,features,prediction):
        
        '''
        Model: the fitted model
        date_dict: the dictionary containing the date which need the prediction.
        drop_col: column that need to be removed before using the model.
        '''
        
        for key in date_dict:
            test_data = date_dict[key]
            x_test = test_data[features]
            
            pred = model.predict(x_test)
            date_dict[key][prediction] = pred
            
            
            
    def get_pred_list(self,candle_size):
        if candle_size==1:
            pred_list = ['pred_high20','pred_low20','pred_hl_ratio20','pred_ng20']
            
        elif candle_size==390:
            pred_list = ['pred_next_gain','pred_next_oc','pred_next_high','pred_next_low','pred_hl_ratio']
            
        return pred_list
    
    
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
                           'hl_ratio':'pred_hl_ratio'
                           }
                          
        return target_dict
    
    
    def add_pred_cols(self,df,candle_size):
        pred_list = self.get_pred_list(candle_size)
        for p in pred_list:
            df[p]=np.nan
            
    def add_stock_dict_pred_col(self,stock_dict,candle_size):
        for key in stock_dict:
            df = stock_dict[key]
            self.add_pred_cols(df,candle_size)
           
            
    def get_models_dict(self,candle_size):
        target_dict = self.get_target_dict(candle_size)
        model_dict = {}
        
        for target in target_dict:
            model_dict[target] = self.get_model(target)
            
        return model_dict

        
         
    def get_models_features_importance(self,model):
        fi_dict = {}
        fi_dict['fi_gain_high'] = model.get_booster().get_score(importance_type="gain")
        fi_dict['fi_weight_high'] = model.get_booster().get_score(importance_type="weight")
        fi_dict['fi_cover_high'] = model.get_booster().get_score(importance_type="cover")  
        
        return fi_dict
    
    
    def get_models_fi(self,model_dict):
        model_fi = {}
    
        for target in model_dict:
            model_fi[target]= self.get_models_features_importance(model_dict[target])
            
        return model_fi
    
     
    def save_models(self,model_dict):
        for target in model_dict:
            model_name = target
            model_dict[target].save_model(model_name)
            
            
    def save_features_importance(self,model_dict,candle_size):
        features_importance = self.get_models_fi(model_dict)
        
        col = ['target','metrics','feature','value']
        fi_df = pd.DataFrame(columns=col)
        
        index=0
        for target in features_importance:
            
            for metric in features_importance[target]:
                
                for feature in features_importance[target][metric]:
                   
                    value = features_importance[target][metric][feature]
                    row = [target,metric,feature,value]
                    fi_df.loc[index]=row
                    index+=1
        
        path = 'features_importance/'
        name = str(candle_size)+' min fi.csv'
        fi_df.to_csv(path+name)
        
        
    def load_models(self,candle_size):

        model_dict = {}
        target_dict = self.get_target_dict(candle_size)
        for model_name in target_dict:
            model = XGBRegressor(n_jobs=-1)
            model.load_model(model_name)
            model_dict[model_name] = model
            #print(model_name)
            
        return model_dict
        
  

    def evaluate_loaded_model(self,test_df,candle_size=1): 
        
        target_dict = self.get_target_dict(candle_size)
        
        init_pred_col(test_df,target_dict)         
            
        model_dict = self.load_models(candle_size)
        target_dict = self.get_target_dict(candle_size)
        features = self.get_features(candle_size)
        
        
        temps = t.time()
        for target in model_dict:
            
            pred_col = target_dict[target]
            print('adding the prediction for {}'.format(target))
            
            model = model_dict[target]
            
            x_test = test_df[features]
        
            pred_col = target_dict[target]
            test_df[pred_col] = model.predict(x_test)
        
        
        self.print_the_score(test_df,candle_size)
        
        temps = t.time()-temps
        print('testing the loaded models tooks {} seconds'.format(temps)) 
        
        
    def print_the_score(self,test_df,candle_size):
        target_dict = self.get_target_dict(candle_size)
        
        for target in target_dict:
            pred_col = target_dict[target]
            compute_pi_error(test_df,target,pred_col,bet=self.bet,iteration=3)
        
    
    
    def prepare_1m_models(self):

        candle_size=1
        
        '''
        The minimum liquidity to make it to the training set. 
        '''
    
        split_n = 7 
        
        '''
        downloading the data take 40 minutes
        '''
        
        stock_dict = self.get_1m_data()
        
        
        date_list = self.dh.create_date_list_from_dict(stock_dict)
        #split_dict = self.dh.split_cv_date(date_list,n=split_n)
        split_dict = self.dh.split_last_x_day(self.db_name,date_list,num_days=6)
        add_split_index(date_list,split_dict,stock_dict)
        
        data_df = create_data_df(stock_dict,candle_size)
        
        features = self.get_features(candle_size)
        target_dict = self.get_target_dict(candle_size)
        model_dict = self.get_models_dict(candle_size)
        
        split_index = 1
        
        test_df = data_df.loc[data_df['split_index']==split_index].copy()
        train_df = data_df.loc[data_df['split_index']!=split_index]
        
        
        init_pred_col(test_df,target_dict)
        
        print('')
        print('round {}'.format(split_index))
        print('length train: {}, length test: {}'.format(len(train_df),len(test_df)))
        print('')
        
        X = train_df[features]
        
        temps = t.time()
        for target in model_dict:
        
            y = train_df[target]
            
            #print(features)
            print('training {} model'.format(target))
            model = model_dict[target]
            model.fit(X,y)
            
            x_test = test_df[features]
        
            pred_col = target_dict[target]
            test_df[pred_col] = model.predict(x_test)
            
            print('{}/{} prediction were added for {}'.format(len(test_df),len(data_df),target))
            
            
            #pred_list.append(test_df)
            
            print('')
            print('testing {} model'.format(target))
            compute_pi_error(test_df,target,pred_col,bet=self.bet,iteration=3)
        
        temps = t.time()-temps
        print('Training the live models tooks {} seconds'.format(temps))
        
        
        '''
        Save features importance
        '''
        self.save_features_importance(model_dict, candle_size)
        
        '''
        Saving the models
        '''
        self.save_models(model_dict)
        
        '''
        Load the models
        '''
        
        self.evaluate_loaded_model(test_df)


            


    
    def prepare_390m_models(self):
    

        '''
        downloading the data take 40 minutes
        '''
        
        candle_size=390
        stock_dict = self.get_390m_data()
           
        split_n = 6
        
        date_list = self.dh.create_date_list_from_dict(stock_dict)
        split_dict = self.dh.split_cv_date(date_list,n=split_n)
        
        add_split_index(date_list,split_dict,stock_dict)
        
        data_df = create_data_df(stock_dict,candle_size)
        
        
        features = self.get_features(candle_size)
        target_dict = self.get_target_dict(candle_size)
        model_dict = self.get_models_dict(candle_size)
        
        # save one of the split for the validation
        split_index=2
            
        test_df = data_df.loc[data_df['split_index']==split_index].copy()
        train_df = data_df.loc[data_df['split_index']!=split_index]
        
        init_pred_col(test_df,target_dict)
        
        X = train_df[features]
        
        temps = t.time()
        for target in model_dict:
        
            y = train_df[target]
            
            #print(features)
            model = model_dict[target]
            model.fit(X,y)
            
            x_test = test_df[features]
        
            pred_col = target_dict[target]
            test_df[pred_col] = model.predict(x_test)
            
            print('{}/{} prediction were added for {}'.format(len(test_df),len(data_df),target))
            
            compute_pi_error(test_df,target,pred_col,bet=self.bet,iteration=4,candle_size=1)
        
        
        
        temps = t.time()-temps
        print('training the {} minute models tooks {} seconds'.format(candle_size,temps))
        
        
        '''
        Save features importance
        '''
        self.save_features_importance(model_dict, candle_size)
        
        '''
        Saving the models
        '''
        self.save_models(model_dict)
        
        '''
        Load the models
        '''
        
        self.evaluate_loaded_model(test_df,candle_size=390)






def add_split_index(date_list,split_dict,train_dict):
    
    temps = t.time()
    for d in date_list:
       train_dict[d]['split_index']= split_dict[d] 
    
    temps = t.time()-temps
    print('Adding the split index took {} seconds'.format(temps))  


def compute_pi_error(pred_df,target,pred_col,bet=1.01,iteration=3,candle_size=1,ema_cash=7000):
    
    '''
    From experience most of the worthy pick have a hl ratio between 3 and 6.
    Therefore this function optimize the precision for the hl ratio between
    3 and 6.
    '''
  
    
    print('evaluating {}'.format(target))
    
    bet_df = pred_df.loc[pred_df[pred_col]>=bet]
    bet_df = bet_df.loc[bet_df['ema13_cash']>=ema_cash]
    
    testing_file_name = 'testing_live_model_'+target +'.csv'
    bet_df.to_csv(testing_file_name)
    
    nbr_bet = len(bet_df)
    
    total_error = 0
    for x in range(iteration):
        lb = bet + x*0.01
        ub = lb + 0.01
        bound_df = pred_df.loc[pred_df[pred_col]>=lb]
        bound_df = bound_df.loc[bound_df[pred_col]<=ub]
        
        if len(bound_df)>=10:
            true_mean = bound_df[target].mean()
            pred_mean = bound_df[pred_col].mean()
            
            print('lower bound: {}, true: {}, pred: {}, nbr: {}'.format(lb,round(true_mean,4),round(pred_mean,4),len(bound_df)))
        
            error = abs(true_mean-pred_mean)
            total_error = error + total_error
        else:
            total_error = total_error + 0.1 # penalty for being way too conservative. 
        
    '''
    Adding a penalty if the hl ration above 6 does not predict
    an hl ratio of at least above 6 on average
    '''
    
    ub = ub+0.01
    
    bound_df = pred_df.loc[pred_df[pred_col]>=ub]
    true_mean = bound_df[target].mean()
    pred_mean = bound_df[pred_col].mean()
    
    print('Above: {}, true: {}, pred: {}, nbr: {}'.format(ub,round(true_mean,4),round(pred_mean,4),len(bound_df)))

    
    if len(bound_df)>=10:
        if true_mean < ub:
            error = abs(true_mean-pred_mean)
            total_error = error + total_error
    else:
        total_error = total_error + 0.1 # penalty for being way too conservative. 
        
    # interating value to see while optimizing  
    true_mean = bet_df[target].mean()
    pred_mean = bet_df[pred_col].mean()
    
    """
    if candle_size==1:
        ng_mean = bet_df['next_gain20'].mean()
    else:
        ng_mean=0
    """
    
    #return true_mean,pred_mean,ng_mean,total_error,nbr_bet


def create_data_df(train_dict,candle_size):
    data_df = pd.concat(train_dict.values()) 
    data_df = data_df.sample(frac=1).reset_index(drop=True)
    
    if candle_size==1:
        data_df = data_df.loc[data_df['hl_ratio20']<=3]
    elif candle_size==390:
        data_df = data_df.loc[data_df['hl_ratio']<=3]
    
    return data_df 



def init_pred_col(df,target_dict):

    for target in target_dict:
        pred_col = target_dict[target]
        df[pred_col]=np.nan    



def train_all_models():

    tlm = train_live_model(db_name='american_database')
    tlm.prepare_390m_models()
    tlm.prepare_1m_models()



#train_all_models()








  
    
    
    
    
    
    
#end