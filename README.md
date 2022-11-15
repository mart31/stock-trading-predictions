# stock-trading-predictions
This project is an end-to-end system (15057 lines of code) used to forecast the price of stocks 20 minutes in advance. 

# Summary

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

I decided to use XGboost as the predictive model. The train_live_model module uses historical data (more than 10 million data rows) to train the model that will be used for the live predictions. The last three weeks of data are used as a validation set. 

The optimise_model module is used to find the optimal hyperparameter for the model. The goal is to find hyper-parameters that will minimize overtraining and maximize the amount of daily profit. Remember that the goal is to predict the stock price 20 minutes in the future. In order to achieve this goal, the data is first split into days, and the last 20 minutes of each day is removed. It is essential not to have rows from the same day simultaneously in the training and testing set, as this dramatically increases overfitting. The model will be trained using cross-validation, and a performance score will be calculated for a given set of hyperparameters. A bayesian optimization algorithm determines which set of hyperparameters should be tested next. This process is repeated until a satisfactory solution is found. 


**The live data modules**

The live data modules comprise live_feed, features_creator,freedom_app, and the live database. Live_feed pull the live data, create the features using features_creator and send it to freedom_app. The freedom_app module saves the data to the live_database at precisely the right time. 


**The graphical user interfaces**

Scanner_gui pulls and sorts the data row from the live database and displays it to the scanner user interface, as seen in the image below.

![scanner_gui](https://user-images.githubusercontent.com/16655278/201759394-b093d240-e1d4-4268-8cab-a840ee4f074d.png)

Multiple features are displayed. From left to right:    
**ticker:** Display the ticker and is also a button which opens the specific ticker GUI.   
**ema_cash:** The exponential moving average cash from the last 13 minutes.   
**avg_cash:** The total cash traded during the last minute.    
**price:** The closing price of the last minute.    
**range:** The predicted difference from the high and low price point, in percent, of the next 20 minutes.     
**p_high20:** the predicted high price in percent from the actual price in the next 20 minutes.    
**hl_ratio:** The predicted tendency, in percent, of the next 20 minutes.   
**p_hl20:** Same as hl_ratio but used a specific model to calculate that number.    
**ng20:** Prediction of the price in 20 minutes again in percent.

The ticker graphical user interface is shown below.

![ticker gui](https://user-images.githubusercontent.com/16655278/201955465-ef825ea7-7bdc-43b9-b967-6d7c5077edc6.png)


