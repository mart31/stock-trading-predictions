# -*- coding: utf-8 -*-
"""
Created on Sat Jan 15 10:09:47 2022

@author: marti
"""


import db_manager
import date_manager

import pandas as pd
import numpy as np
from skopt.space import Real, Integer, Categorical
from xgboost import XGBRegressor
import time as t
from skopt.utils import use_named_args
import math
from skopt import gp_minimize
from datetime import date

import otc_object
import american_object
import db_histo_manager as dhm

import warnings
warnings.filterwarnings(action='ignore', category=UserWarning)
warnings.filterwarnings(action='ignore', category=FutureWarning)


'''
This is the testing module where I tried to find the optimal hyperparameter for the live model.
'''




class model_optimizer():
    
    '''
    This class optimize a given model.
    '''
    
    def __init__(self,train_dict,features,model,param_grid,target='hl_ratio20',cost='pi_score',cv_split=3,min_ema_cash=7000,min_cash=7000,bet=1.03,max_gain=3,num_round=10,bias=0,error_iteration=5):
        
        
        self.train_dict = train_dict
        self.features = features
        self.model = model
        self.param_grid = param_grid
        self.target = target
        self.cost = cost
        self.cv_split = cv_split
        self.min_ema_cash=min_ema_cash
        self.min_cash = min_cash
        self.bet = bet
        self.max_gain = max_gain
        self.num_round = num_round
        self.iteration = 0
        self.error_iteration=error_iteration
        self.metrics = {}
        self.bias = bias
        
        self.dh = date_manager.date_handler()
        #self.dbm = self.get_db_manager(db_name='otc_database')
        #self.dbm = db_manager.db_manager(db_name='test_database')
        
        self.date_list = self.dh.create_date_list_from_dict(self.train_dict)
        self.split_dict = self.dh.split_cv_date(self.date_list,n=cv_split)
        self.add_split_index()
        
        self.data_df = self.create_data_df()
        print('')
        print('the data df contains {} rows'.format(len(self.data_df)))
        print('')
        
        self.add_pred_col()
        
        
        
    def get_db_manager(self,db_name):
    
        if db_name=='otc_database':
            data_shape = otc_object.otc_object() 

            
        dbm = dhm.db_manager(db_name,data_shape)
        
        return dbm
    

        
    
    def get_metrics(self):
        return self.metrics
        
    def get_pred_df(self):
        return self.pred_df
    
        
    def compute_pred_df(self):
        pred_list=[]
        for x in range(self.cv_split):
        
            split_index = x + 1
            print('training split number {} for {}'.format(split_index,self.target))
            
            test_df = self.data_df.loc[self.data_df['split_index']==split_index].copy()
            train_df = self.data_df.loc[self.data_df['split_index']!=split_index] 
            
            X = train_df[self.features]
            y = train_df[self.target]
            
            #print(features)
            self.model.fit(X,y)
            
            x_test = test_df[self.features]
            
            test_df['prediction'] = self.model.predict(x_test)
            pred_list.append(test_df)
        
        pred_df = pd.concat(pred_list)
        self.pred_df = pred_df
        
        #return pred_df
     
    
    def create_data_df(self):
        data_df = pd.concat(self.train_dict.values()) 
        data_df = data_df.sample(frac=1).reset_index(drop=True)
        
        return data_df
        
      
    def add_split_index(self):
        
        temps = t.time()
        for d in self.date_list:
           self.train_dict[d]['split_index']= self.split_dict[d] 
        
        temps = t.time()-temps
        print('Adding the split index took {} seconds'.format(temps))
        
        
        
    def get_train_dict(self):
        return self.train_dict
    
    
    def get_data_df(self):
        return self.data_df
        
        
    def train_model(self,cv_list,split):
        pass
    
    
    def add_prediction(self,cv_list,split):
        pass
        
        
    def add_pred_col(self):
        '''
        Simply add the prediction col. As we are only testing 1 target.
        '''
        
        self.data_df['prediction']=np.nan
        
        
    
    
    def get_models_features_importance(self):
        fi_dict = {}
        fi_dict['fi_gain_high'] = self.model.get_booster().get_score(importance_type="gain")
        fi_dict['fi_weight_high'] = self.model.get_booster().get_score(importance_type="weight")
        fi_dict['fi_cover_high'] = self.model.get_booster().get_score(importance_type="cover")  
        
        return fi_dict
    
    
    def add_all_predictions(self):
        
        for x in range(self.split_cv):
            pass
        
        
    def evaluate_predictions(self):
        
        if self.cost=='positive_bias_score':
            score = self.compute_positive_bias_score()
        elif self.cost=='true_mean':
            score = self.compute_true_score()
        elif self.cost=='combined':
            score = self.compute_combined_score()
        elif self.cost=='short_true_mean':
            true_mean,pred_mean,ng_mean,score,nbr_bet = self.compute_short_true_error()
        else:
            score = self.pred_metrics()
        
        '''
        The bayesian algorithm will find the lower point so we need to return the -value
        '''
        
        
        if math.isnan(score):
            score=0
        elif score<0:
            score=0
            
        print('score: {}'.format(score))
        
        return -score
    
    
    def compute_combined_score(self):
        true_mean,pred_mean,ng_mean,bias_score,nbr_bet = self.compute_positive_bias_error()
        
        p = (true_mean-1.0)*100
        
        score = nbr_bet*bias_score*p
        
        return score
    
    
    def compute_positive_bias_score(self):
        true_mean,pred_mean,ng_mean,bias_score,nbr_bet = self.compute_positive_bias_error()
        
        return nbr_bet*bias_score
    
    
    def compute_true_score(self):
        true_mean,pred_mean,ng_mean,bias_score,nbr_bet = self.compute_positive_bias_error()
        
        p = (true_mean-self.bet+0.01)*100 #added a small bias to prevent underfitting.
        score = nbr_bet*p
        
        return score
    
    
    
    def get_bias_score(self,true_mean,pred_mean):
        e = 2.7183
                
        value = (true_mean-pred_mean)*100
        print('value: {}, true_mean: {}, pred_mean: {}'.format(value,true_mean,pred_mean))
 
        if value<=-1:
            score=0
        elif value<0:
            
            x = e+(value*1.7)
            score = math.log(x)
        elif value<1:
            x = e+(value*0.5)
            score = math.log(x)
        elif value<2:
            value = value -1
            x = e+0.5+(value*0.25)
            score = math.log(x)
        else:
            score = 1.25
            
        
        return score
            
    
    
    
    def compute_positive_bias_error(self,print_result=True):
        
        '''
        From experience most of the worthy pick have a hl ratio between 3 and 6.
        Therefore this function optimize the precision for the hl ratio between
        3 and 6.
        '''
        pred_df = self.get_pred_df()
        bet_df = pred_df.loc[pred_df['prediction']>=self.bet]
        
        nbr_bet = len(bet_df)
        
        bias_score = 0
        for x in range(self.error_iteration):
            lb = self.bet + x*0.01
            ub = lb + 0.01
            bound_df = pred_df.loc[pred_df['prediction']>=lb]
            bound_df = bound_df.loc[bound_df['prediction']<=ub]
            
            if len(bound_df)>=10:
                true_mean = bound_df[self.target].mean()
                pred_mean = bound_df['prediction'].mean()
                
                if print_result:
                    #print('lower bound: {}, true: {}, pred: {}'.format(lb,true_mean,pred_mean))
                    print('lower bound: {}, true: {}, pred: {}, nbr: {}'.format(lb,round(true_mean,4),round(pred_mean,4),len(bound_df)))
            
            
                score= self.get_bias_score(true_mean,pred_mean)
                bias_score = score+ bias_score
            else:
                bias_score = bias_score-1 # penalty for being way too conservative. 
            
        '''
        Adding a penalty if the hl ration above 6 does not predict
        an hl ratio of at least above 6 on average
        '''
        
        #ub = 1.06
        ub = self.bet + (self.error_iteration*0.01)
        
        bound_df = pred_df.loc[pred_df['prediction']>=ub]
        true_mean = bound_df[self.target].mean()
        pred_mean = bound_df['prediction'].mean()
        
        if print_result:
            #print('Above: {}, true: {}, pred: {}'.format(ub,true_mean,pred_mean))
            print('Above: {}, true: {}, pred: {}, nbr: {}'.format(ub,round(true_mean,4),round(pred_mean,4),len(bound_df)))
    
        
        value = (true_mean-pred_mean)*100
        if len(bound_df)>=10:
            if value<-1:
                bias_score = bias_score# penalty for having a negative bias
            elif value<0:
                bias_score = bias_score+0.2 
            elif value<1:
                bias_score = bias_score+0.4
            elif value<2:
                bias_score = bias_score+0.6
            elif value<3:
                bias_score = bias_score+0.8
            else:
                bias_score = bias_score+1
                
        else:
            bias_score = bias_score-1 # penalty for being way too conservative. 
            
        # interating value to see while optimizing  
        true_mean = bet_df[self.target].mean()
        pred_mean = bet_df['prediction'].mean()
        ng_mean = bet_df['next_gain20'].mean()
        
        
        return true_mean,pred_mean,ng_mean,bias_score,nbr_bet
    
    
    
    
    def compute_short_true_error(self,print_result=True):
        
     
        pred_df = self.get_pred_df()
        bet_df = pred_df.loc[pred_df['prediction']<=self.bet]
        
        nbr_bet = len(bet_df)
      
        # interating value to see while optimizing  
        true_mean = 1-bet_df[self.target].mean()
        pred_mean = 1-bet_df['prediction'].mean()
        ng_mean = 1-bet_df['next_gain20'].mean()
        
        bet_percent = 1-self.bet # Add a small bias to prevent underfitting.
        #bet_percent=0.02 #the average cost bias of the transaction.
        
        score = (true_mean-bet_percent)*nbr_bet
        
        if true_mean>pred_mean:
            score = score*1.1  # add a positive bias for when the modele is more precise. 
            
        if true_mean>(pred_mean*1.1):
            score = score*1.1  # add a extra bias of the true mean is more then 10% above the pred mean.
        
        if print_result:
            
            print('Above: {}, true: {}, pred: {}, nbr: {}'.format(self.bet,round(true_mean,4),round(pred_mean,4),len(bet_df)))
        
        
        return true_mean,pred_mean,ng_mean,score,nbr_bet
    
    
        
        
    def pred_metrics(self):
        '''
        This compute the average above the bet for the prediction and its
        correspondant true average. Using the error and the number of bet
        its computes the score. Note the score is always a positive value.
        '''
        
        true_mean,pred_mean,ng_mean,error,nbr_bet = self.compute_bet_metrics()
        
        print('evaluating the model for {}'.format(self.target))
        print('true mean: {}'.format(true_mean))
        print('pred_mean: {}'.format(pred_mean))
        print('next_gain20: {}'.format(ng_mean))
        print('number of bet: {}'.format(nbr_bet))
        
        #the 0.01 (1%) is there in case the error get really close to 0. 
        score = abs(nbr_bet*(1/(error+0.01)))
        
        return score
    
    
    
    
    def compute_bet_metrics(self,print_result=True):
        if self.cost=='pred_mean_bet':
            true_mean,pred_mean,ng_mean,error,nbr_bet = self.compute_mean_bet_metrics()
            return true_mean,pred_mean,ng_mean,error,nbr_bet 
        elif self.cost=='pi_score':
            true_mean,pred_mean,ng_mean,error,nbr_bet = self.compute_pi_error(print_result)
            return true_mean,pred_mean,ng_mean,error,nbr_bet 
        elif self.cost=='positive_bias_score':
            true_mean,pred_mean,ng_mean,error,nbr_bet = self.compute_positive_bias_error(print_result)
            return true_mean,pred_mean,ng_mean,error,nbr_bet 
        elif self.cost=='true_mean':
            true_mean,pred_mean,ng_mean,error,nbr_bet = self.compute_positive_bias_error(print_result)
            return true_mean,pred_mean,ng_mean,error,nbr_bet
        elif self.cost=='combined':
            true_mean,pred_mean,ng_mean,error,nbr_bet = self.compute_positive_bias_error(print_result)
            return true_mean,pred_mean,ng_mean,error,nbr_bet
        elif self.cost=='short_true_mean':
            true_mean,pred_mean,ng_mean,score,nbr_bet = self.compute_short_true_error()
            return true_mean,pred_mean,ng_mean,score,nbr_bet
            
    
    
    
    def compute_mean_bet_metrics(self):
        
        pred_df = self.get_pred_df()
        bet_df = pred_df.loc[pred_df['prediction']>=self.bet]
        
        true_mean = bet_df[self.target].mean()
        pred_mean = bet_df['prediction'].mean()
        ng_mean = bet_df['next_gain20'].mean()
        
        error = abs(true_mean-pred_mean)
        nbr_bet = len(bet_df)
        
        return true_mean,pred_mean,ng_mean,error,nbr_bet
    
    
    def compute_pi_error(self,print_result):
        
        '''
        From experience most of the worthy pick have a hl ratio between 3 and 6.
        Therefore this function optimize the precision for the hl ratio between
        3 and 6.
        '''
        pred_df = self.get_pred_df()
        bet_df = pred_df.loc[pred_df['prediction']>=self.bet]
        
        nbr_bet = len(bet_df)
        
        total_error = 0
        for x in range(self.error_iteration):
            lb = self.bet + x*0.01
            ub = lb + 0.01
            bound_df = pred_df.loc[pred_df['prediction']>=lb]
            bound_df = bound_df.loc[bound_df['prediction']<=ub]
            
            if len(bound_df)>=10:
                true_mean = bound_df[self.target].mean()
                pred_mean = bound_df['prediction'].mean()
                
                if print_result:
                    #print('lower bound: {}, true: {}, pred: {}'.format(lb,true_mean,pred_mean))
                    print('lower bound: {}, true: {}, pred: {}, nbr: {}'.format(lb,round(true_mean,4),round(pred_mean,4),len(bound_df)))
            
                error = abs(true_mean-pred_mean)
                total_error = error + total_error
            else:
                total_error = total_error + 0.1 # penalty for being way too conservative. 
            
        '''
        Adding a penalty if the hl ration above 6 does not predict
        an hl ratio of at least above 6 on average
        '''
        
        ub = self.bet+self.error_iteration*0.01
        
        bound_df = pred_df.loc[pred_df['prediction']>=ub]
        true_mean = bound_df[self.target].mean()
        pred_mean = bound_df['prediction'].mean()
        
        if print_result:
            #print('Above: {}, true: {}, pred: {}'.format(ub,true_mean,pred_mean))
            print('Above: {}, true: {}, pred: {}, nbr: {}'.format(ub,round(true_mean,4),round(pred_mean,4),len(bound_df)))
    
        
        if len(bound_df)>=10:
            if true_mean < ub:
                error = abs(true_mean-pred_mean)
                total_error = error + total_error
        else:
            total_error = total_error + 0.1 # penalty for being way too conservative. 
            
        # interating value to see while optimizing  
        true_mean = bet_df[self.target].mean()
        pred_mean = bet_df['prediction'].mean()
        ng_mean = bet_df['next_gain20'].mean()
        
        
        return true_mean,pred_mean,ng_mean,total_error,nbr_bet
    
    
    def save_optimisation_details(self):
        '''
        Saving the optimisation details
        '''
        
        metrics_df = pd.DataFrame.from_dict(self.metrics,orient='index')
        
        file_name = 'optimisation_details_'+self.target+'_'+self.cost+'.csv'
        metrics_df.to_csv(file_name,index=False)
        
        
    def add_optimisation_details(self):
        '''
        Once an optimisation round has been completed it add the result to the 
        previous results using the same cost function. 
        '''
        file_name = self.target+'_'+ str(self.cost)+'_optimisation_details.csv'
        metrics_df = pd.DataFrame.from_dict(self.metrics,orient='index')
        metrics_df.to_csv('optimisation_details.csv',index=False)
        
        try:
            df = pd.read_csv(file_name)
            metrics_df = df.append(metrics_df)
            #metrics_df = metrics_df.sample(frac=1).reset_index(drop=True)
            
        except Exception as e:
            msg = 'initializing '+file_name
            print(msg)
        
        metrics_df.to_csv(file_name,index=False)
       
    
    def start_optimisation(self):
        self.features_bayesian_optimisation()
        
    
    def features_bayesian_optimisation(self):

        '''
        This function optimise the percentage of stock that our best prediction beats.
        
        model: base model that will be optimised.
        train_dict: a dictionary of candle date.
        target: the target for the prediction.
        candle_size: The time length being considered
        bet: The minimum value of the prediction before we consider making a bet.
        nbr_stock: The max number of stock to beat on at the same time.
        param_grid: The parameter grid that we are looking for an optimal solution. 
        cv_split: how many cross validation split.
        num_round: number of optimisation round
        '''
        
        # the decorator allows our objective function to receive the parameters as
        # keyword arguments. This is a requirement of Scikit-Optimize.
        @use_named_args(param_grid)
        def objective(**params):
            
            round_time = t.time()
            # model with new parameters
            self.model.set_params(**params)
            
            self.compute_pred_df()
        
            print(params)
            score = self.evaluate_predictions()
        
            
            '''
            The metrics depend on which cost function was chosen. This might need to be
            modified in order to make it more flexible
            '''
            
                
            self.iteration = self.iteration+1
            
            self.metrics[self.iteration]={}
            
            true_mean,pred_mean,ng_mean,error,nbr_bet = self.compute_bet_metrics(print_result=False)
            n_estimators = params['n_estimators']
            max_depth = params['max_depth']
            learning_rate = params['learning_rate']
            gamma = params['gamma']
            objective = params['objective']
            reg_lambda = params['reg_lambda']
            
            self.metrics[self.iteration]['true_mean']=true_mean
            self.metrics[self.iteration]['pred_mean']=pred_mean
            self.metrics[self.iteration]['ng_mean']=ng_mean
            
            if self.cost=='positive_bias_score':
                self.metrics[self.iteration]['bias_score']=error
            else:
                self.metrics[self.iteration]['error']=error
                
            self.metrics[self.iteration]['nbr_bet']=nbr_bet
            self.metrics[self.iteration]['cost']=self.cost
            self.metrics[self.iteration]['n_estimators']=n_estimators
            self.metrics[self.iteration]['max_depth']=max_depth
            self.metrics[self.iteration]['learning_rate']=learning_rate
            self.metrics[self.iteration]['gamma']=gamma
            self.metrics[self.iteration]['objective']=objective
            self.metrics[self.iteration]['reg_lambda']=reg_lambda
            self.metrics[self.iteration]['score']=score
            
            self.save_optimisation_details()
            
            round_time = t.time()-round_time
            
            print('')
            print('score: {}'.format(score))
            print('round {} tooks {} seconds'.format(self.iteration,round_time))
            print('')
            
            return score # is already negative
        
        
        temps = t.time()
        # gp_minimize performs by default GP Optimization 
        # using a Marten Kernel
        
        init_round = math.floor(self.num_round/5)
        
        gp_ = gp_minimize(
            objective, # the objective function to minimize
            self.param_grid, # the hyperparameter space
            n_initial_points=init_round, # the number of points to evaluate f(x) to start of
            acq_func='EI', # the acquisition function
            n_calls=self.num_round, # the number of subsequent evaluations of f(x)
            random_state=0, 
        )
        
        # all together in one dataframe, so we can investigate further
        dim_names = ['n_estimators', 
                     'max_depth',
                     'learning_rate',
                     #'booster',
                     'gamma',
                     'objective',
                     #'subsample',
                     #'colsample_bytree',
                     #'colsample_bylevel',
                     #'colsample_bynode',
                     'reg_lambda',
                    ]
        
        tmp = pd.concat([
            pd.DataFrame(gp_.x_iters),
            pd.Series(gp_.func_vals),
        ], axis=1)
          
        tmp.columns = dim_names + ['score']
        tmp.sort_values(by='score', ascending=True, inplace=True)
        
        today = date.today()
        tmp.to_csv('{} {} bayesian optimisation.csv'.format(today,self.target))
        
        final_time = t.time()-temps
        
        print('it tooks {} seconds to perform the optimisation'.format(final_time))
        
        self.add_optimisation_details()
        
        return gp_,tmp,final_time
        
   
    
    
    
    
def get_db_manager(db_name):

    if db_name=='otc_database':
        data_shape = otc_object.otc_object() 
    if db_name=='american_database':
        data_shape = american_object.american_object()

        
    dbm = dhm.db_manager(db_name,data_shape)
    
    return dbm


def get_combined_data(dbm,cond_list,keep_2021=True):
    '''
    Last try was with cash above 30 000$ Which had bigger variability.
    
    '''
     
    dh = date_manager.date_handler()
    date_list = dh.get_intraday_date_list(keep_2021=keep_2021,candle_size=1)
    
    #date_list = date_list[-3910:]
    stock_dict = dbm.split_combined_by_minute(date_list,cond_list=cond_list)

    return stock_dict  

    

def get_1m_data(dbm,num_days=150):
   
     
     a =['close','>=','0.002']
     b =['ema13_cash','>=','7000']
     #c =['average_cash','>=','10000']
     d =['gains','<=','1.5']
     cond_list = [a,b,d]
     
     dh = date_manager.date_handler()
     date_list = dh.get_intraday_date_list(keep_2021=True,candle_size=1,num_days=num_days)
     stock_dict = dbm.split_data_by_minute(date_list,cond_list=cond_list)
      
     #stock_dict = self.dbm.download_day_to_day_training_data(print_date=False)
     
     return stock_dict





'''
Use this section to test the class.
'''

cv_split = 2

param_grid = [
        Integer(30, 200, name='n_estimators'),
        Integer(2, 20, name='max_depth'),
        Real(0.01, 0.9, name='learning_rate'),
        #Categorical(['gbtree', 'dart'], name='booster'),
        Real(0.01, 0.9, name='gamma'),
        Categorical(['reg:squaredlogerror','reg:pseudohubererror'],name='objective'),
        #Categorical(['reg:squarederror','reg:squaredlogerror','reg:pseudohubererror'],name='objective'),
        #Real(1.0, 1.0, name='subsample'),
        #Real(1.0, 1.0, name='colsample_bytree'),
        #Real(1.0, 1.0, name='colsample_bylevel'),
        #Real(1.0, 1.0, name='colsample_bynode'),
        Integer(1, 100, name='reg_lambda'),]
    
#model = XGBRegressor(n_jobs=-1)
model = XGBRegressor(n_jobs=4)
 
     

features = ['gains','gains3','gains5','gains10','gains20','RSI','d1_ema3_RSI','d2_ema3_RSI','normalize_bb','d1_ema3_bb','d2_ema3_bb',
                        'PPO_line','d1_ema3_PPO_line','d2_ema3_PPO_line','ema13_cash','d1_ema13_cash','d2_ema13_cash','candle_shape',
                        'candle_range','ema13_range','d1_ema13_range','d2_ema13_range','hl_range20','overnight_candle','vwap_ratio',
                        'no_trade_count','green_line'] 



'''
downloading the data take 40 minutes
'''

dbm = get_db_manager('american_database')
stock_dict = get_1m_data(dbm)


#target = 'hl_ratio20' 
target='next_gain20'
#target='highest_ng20'
    
#cost='pi_score'
#cost='positive_bias_score'
#cost='true_mean'
#cost = 'combined' 
cost = 'short_true_mean'
 
#bet = 1.03
bet=0.97  

optimizer = model_optimizer(stock_dict,features,model,param_grid,target,cost=cost,cv_split=cv_split,bet=bet,num_round=500,error_iteration=3)
optimizer.start_optimisation()


#dh = date_manager.date_handler()
#self.dbm = self.get_db_manager(db_name='otc_database')
#self.dbm = db_manager.db_manager(db_name='test_database')

#date_list = dh.create_date_list_from_dict(stock_dict)
#cv_split=3
#split_dict = dh.split_cv_date(date_list,n=cv_split)
#optimizer.add_split_index()

#data_df = optimizer.create_data_df()
#print(len(data_df))

#ztest = data_df[0:10000]


"""
#data_df = optimizer.create_data_df()
#ztest = data_df[0:10000]
#ztest_range = data_df.loc[data_df['hl_range20']>=1.04]

metrics = optimizer.get_metrics()
metrics_df = pd.DataFrame.from_dict(metrics,orient='index')
#metrics_df.to_csv('optimisation_details.csv')

#pi_score_optimisation_df = pd.read_csv('pi_score_optimisation_details.csv')
optimisation_df = pd.read_csv('optimisation_details.csv')

#zdf = pd.concat(stock_dict.values()) 
#ztest = zdf.loc[zdf['prediction']>=1.03]
"""
   



































# end