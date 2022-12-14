# Stock Trading Predictions
This project is an end-to-end system (15 057 lines of code) used to forecast the price of stocks 20 minutes in advance. This section is broken into 4 parts:

1) System Summary
2) Model Performance
3) Live Trading
4) Technical Overview



# 1. System Summary

The system architecture at the component level is shown below.

![system dataflow](https://user-images.githubusercontent.com/16655278/201698488-6dfbbc70-b290-4b4d-aece-1be117cb8406.png)

Note the features_creator module has been purposely omitted from the uploaded files. 

The entire application can be split into two parts: the online and offline systems. 

**The offline systems** comprise everything needed to train, optimize and validate the live model. Which includes the data feed, the database manager, the features creator and the training modules for the different models.

**The online systems** include the tick data and level 2 data feed, which pulls the real-time data needed to make live predictions. The freedom_app module uses live data and combines it with historical data to create the features used by the model. The resulting predictions are saved in the live database. The graphical user interface then pulls and displays the data in real-time.

# 2. Model Performance

The system was operational from January 2022. Each week, all the models would be retrained to include the new data. The historical performance (cross-validated) and the live performance of the models were practically identical. This is mainly because the predictions are made only for the subsequent 20 minutes. The main target used for the live system was the high-low ratio (the trend) of the next 20 minutes. The table below highlights the accuracy of the high-low ratio model for different prediction ranges.  

![Predictions table](https://user-images.githubusercontent.com/16655278/207610995-fc07b6b1-c6e9-4405-b52e-be5d249f9862.png)

The overall results are excellent. However, the performance was not stable over time. The following graph highlights the situation: 

![otc_monthly_change](https://user-images.githubusercontent.com/16655278/207611157-b6f13559-8843-4374-ae37-6d08cd4f14c6.png)

The green line shows the average gains per potential trade flagged by the model. The orange line is the average number of stocks flagged daily by the model. The blue line corresponds to the amount of capital traded by the 100 most active stocks on the OTC market. 

The OTC market stocks are purely speculative, which causes the movement of these stocks to be very sensitive to the overall market sentiment. The Russian invasion of Ukraine in February 2022, and the talk of recession that followed, destroyed confidence in the market. The number of stocks flagged and the gains of these stocks were affected negatively. 

# 3. Live Trading 

When launching the live system, it became evident that most of the stocks flagged by the algorithm were not tradable for three reasons. 

First, most of the false positives consisted of completely illiquid stocks that had one very big trade. Two classic examples are shown below.  

![illiquid_green_graph](https://user-images.githubusercontent.com/16655278/207611358-2551b8d3-dbe9-449e-b2f5-defd7adf009f.png)

![illiquid_red_graph](https://user-images.githubusercontent.com/16655278/207611410-16b61ad7-1a3e-479e-ae24-ae1585e00915.png)

Although the model was very precise with these stocks, the lack of liquidity meant that it was not possible to buy at the targeted price. 

Second, the model training data could not have access to the spread (the difference in price between the highest bidder and lowest seller). Therefore, the model would often flag a stock predicting it would go up, but the actual spread was too large to buy at the targeted price and make a profitable trade.

Third, some patterns flagged by the model would happen so fast that it was impossible to place a trade manually in time. 

Based on my live trade analysis, I was able to catch roughly 25% of the trade flagged by my model. Starting in June 2022, the amount of money in the market and the number of potential trades were so low that I decided to shut down the system. 





# 4. Technical Overview
This section explains at a high level the functions of the different modules. 

**The feed modules**

The feed modules pull the required data from IQFEED (a subscription-based stock data provider). Five different types of data are needed: news data, stock summary data, historical data, live tick data and live depth data. Note that the five feed modules rely heavily on the pyiqfeed package, which can be found here: https://github.com/akapur/pyiqfeed



**The database manager modules**

In order to be useful, the raw data provided by the feed modules need to be properly indexed and modified. The db_updater pulls the data from the different data feeds, indexes the data using the date_manager, creates the features using features_creator and then sends the output to db_manager to be saved for later use. 

All the data are saved in specific SQLite files. The module db_histo_manager contains the class db_manager, which comprises all the functions required to save, download and filter the data from the different SQLite databases. 


**The training modules**

I decided to use XGboost as the predictive model. The train_live_model module uses historical data (using 22 893 737 data rows) to train the models used for the live predictions. The last three weeks of data are used as a validation set. Four main models were used, each with a different target. They all predict attributes from the price, as a ratio from the current price, 20 minutes into the future. The four targets are:    

**highest_ng20,** which is the highest price ratio that will be reached in the next 20 minutes.   
**lowest_ng20,** which is the lowest price ratio of the next 20 minutes.   
**hl_ratio20,** which is the ratio between the high point and the low point.     
**next_gain20,** which is the price ratio precisely 20 minutes after the prediction has been made.     

The optimise_model module is used to find the optimal hyper-parameter for the model. The goal is to find hyper-parameters that will minimize overtraining and maximize the amount of daily profit. In order to achieve this goal, the data are first split into days, and the last 20 minutes of each day are removed. It is essential not to have rows from the same day simultaneously in the training and testing set, as this dramatically increases overfitting. The model is then trained using cross-validation, and a performance score is calculated for a given set of hyper-parameters. A bayesian optimization algorithm determines which set of hyper-parameters should be tested next. This process is repeated until a satisfactory solution is found. 



**The live data modules**

The live data modules comprise live_feed, features_creator, freedom_app, and the live database. Live_feed pulls the live data, creates the features using features_creator and sends it to freedom_app. The freedom_app module saves the data to the live_database at precisely the right time. 


**The graphical user interfaces**

Scanner_gui pulls and sorts the data row from the live database and displays it to the scanner user interface, as seen in the image below.

![scanner_gui](https://user-images.githubusercontent.com/16655278/201759394-b093d240-e1d4-4268-8cab-a840ee4f074d.png)

The scanner interface includes four tables:
      
**The top table** sorts the row according to their predicted range. These are the stocks, sorted from top to bottom, where potential picks are predicted to occur. The values are updated every minute.              
**The second table** sorts the row according to the selected target. The values are updated every minute.    
**The third table** shows the selected target’s approximate predictions for the next minute. The values are updated every second.      
**The fourth table** shows the latest headlines from the watchlist. The headlines are updated every minute.   


In the top three tables, multiple features are displayed. From left to right:   
**ticker:** Displays the ticker and is also a button which opens the specific ticker GUI.    
**ema_cash:** The exponential moving average cash from the last 13 minutes.   
**avg_cash:** The total cash traded during the last minute.   
**price:** The closing price of the last minute.   
**range:** The predicted difference between the high and low price point, as a ratio, of the next 20 minutes.   
**p_high20:** The predicted high price as a ratio to the actual price in the next 20 minutes.   
**hl_ratio:** The predicted tendency, as a ratio, of the next 20 minutes.   
**p_hl20:** Same as hl_ratio but uses a specific model to calculate the value.  
**ng20:** Prediction of the price in 20 minutes as a ratio.  

The ticker graphical user interface is shown below.


![ticker gui](https://user-images.githubusercontent.com/16655278/201955465-ef825ea7-7bdc-43b9-b967-6d7c5077edc6.png)

The ticker GUI adds some important information that is not included in the scanner GUI, specific to the requested ticker. Notably, it enables us to visualize the candlestick graph, the level 2 bid and ask tables, the level 2 summary and the latest trades in near real-time. 


**The depth modules**   
 
The live system monitors the tick data of 490 stocks in real-time. However, my current computer can only handle the live depth data of, at most, 30 stocks. Since the depth data is only displayed in the ticker GUI, the scanner GUI continuously updates which ticker requires the live depth data through depth_watchlist. The module depth_feed updates every 4 seconds, identifying which ticker it should be watching, and then sends the data to depth_data, which prepares the depth data to be used by the GUI. 

