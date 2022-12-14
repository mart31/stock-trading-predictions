# Stock Trading Predictions
This project is an end-to-end system (15 057 lines of code) used to forecast the price of stocks 20 minutes in advance. This section is broken into 4 parts:

1) System Summary
2) Model Performance
3) Live Trading
4) Technical Overview


1. System Summary

The system architecture at the component level is shown below.

![system dataflow](https://user-images.githubusercontent.com/16655278/201698488-6dfbbc70-b290-4b4d-aece-1be117cb8406.png)

Note the features_creator module has been purposely omitted from the uploaded files. 

The entire application can be split into two parts: the online and offline systems. 

**The offline systems** comprise everything needed to train, optimize and validate the live model. Which includes the data feed, the database manager, the features creator and the training modules for the different models.

**The online systems** include the tick data and level 2 data feed, which pulls the real-time data needed to make live predictions. The freedom_app module uses live data and combines it with historical data to create the features used by the model. The resulting predictions are saved in the live database. The graphical user interface then pulls and displays the data in real-time.


# Technical overview
This section explains at a high level the functions of the different modules. 


**The feed modules**

The feed modules pull the required data from IQFEED (a subscription-based stock data provider). Five different types of data are needed: news data, stock summary data, historical data, live tick data and live depth data. Note the five feed modules rely heavily on the pyiqfeed package, which can be found here: https://github.com/akapur/pyiqfeed


**The database manager modules**

The raw data provided by the feed modules need to be properly indexed and modified to be of any use. The db_updater pulls the data from the different data feed, indexes the data using the date_manager, creates the features using features_creator and then sends the output to db_manager to be saved for later use. 

All the data are saved in specific SQLite files. The module db_histo_manager contains the class db_manager, which comprises all the functions required to save, download and filter the data from the different SQLite databases. 


**The training modules**

I decided to use XGboost as the predictive model. The train_live_model module uses historical data (using 22 893 737 data rows) to train the models  used for the live predictions. The last three weeks of data are used as a validation set. Four main models are being used, each with a different target. They all predict attributes from the price, as a ratio from the current price, 20 minutes in the future. The four targets are:    

**highest_ng20,** which is the highest price ratio that will be reached in the next 20 minutes   
**lowest_ng20,** which is the lowest price ratio of the next 20 minutes   
**hl_ratio20,** which is the ratio between the high point and the low point.     
**next_gain20,** which is the price ratio precisely 20 minutes after the prediction has been made.     

The optimise_model module is used to find the optimal hyperparameter for the model. The goal is to find hyper-parameters that will minimize overtraining and maximize the amount of daily profit. In order to achieve this goal, the data is first split into days, and the last 20 minutes of each day are removed. It is essential not to have rows from the same day simultaneously in the training and testing set, as this dramatically increases overfitting. The model will be trained using cross-validation, and a performance score will be calculated for a given set of hyperparameters. A bayesian optimization algorithm determines which set of hyperparameters should be tested next. This process is repeated until a satisfactory solution is found. 


**The live data modules**

The live data modules comprise live_feed, features_creator,freedom_app, and the live database. Live_feed pull the live data, create the features using features_creator and send it to freedom_app. The freedom_app module saves the data to the live_database at precisely the right time. 


**The graphical user interfaces**

Scanner_gui pulls and sorts the data row from the live database and displays it to the scanner user interface, as seen in the image below.

![scanner_gui](https://user-images.githubusercontent.com/16655278/201759394-b093d240-e1d4-4268-8cab-a840ee4f074d.png)

The scanner interface includes four tables.      
**The top table** sorts the row according to their predicted range. These are the stocks, sorted from top to bottom, where potential picks are predicted to occur. The values are updated every minute.              
**The second table** sorts the row according to the selected target. The values are updated every minute.    
**The third table** shows the selected target's approximated predictions for the next minute. The values are updated every second      
**The fourth table** shows the latest headlines from the watchlist. The headlines are updated every minute.   


In the top three tables, multiple features are displayed. From left to right:   
**ticker:** Display the ticker and is also a button which opens the specific ticker GUI.    
**ema_cash:** The exponential moving average cash from the last 13 minutes.   
**avg_cash:** The total cash traded during the last minute.   
**price:** The closing price of the last minute.   
**range:** The predicted difference from the high and low price point, as a ratio, of the next 20 minutes.   
**p_high20:** the predicted high price as a ratio to the actual price in the next 20 minutes.   
**hl_ratio:** The predicted tendency, as a ratio, of the next 20 minutes.   
**p_hl20:** Same as hl_ratio but used a specific model to calculate that number.  
**ng20:** Prediction of the price in 20 minutes again as a ratio.  

The ticker graphical user interface is shown below.

![ticker gui](https://user-images.githubusercontent.com/16655278/201955465-ef825ea7-7bdc-43b9-b967-6d7c5077edc6.png)

The ticker GUI adds some important information that is not included in the scanner GUI, specific to the requested ticker. Notably, it enables us to visualize the candlestick graph, the level 2 bid and ask tables, the level 2 summary and the latest trades in near real-time. 


**The depth modules**    
The live system monitors the tick data of 490 stocks in real time. However, my current computer can only handle the live depth data of at most 30 stocks. Since the depth data is only displayed in the ticker GUI, the scanner GUI continuously updates which ticker requires the live depth data through depth_watchlist. The module depth_feed updates every 4 seconds, which ticker it should be watching and then send the data to depth_data, which prepare the depth data to be used by the GUI. 
