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

Scanner_gui pull the data row from the live database and sort them in an easy-to-read manner. 
https://github.com/mart31/stock-trading-predictions/blob/main/scanner_gui.png
