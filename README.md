# stock-trading-predictions
This project is an end-to-end system (15057 lines of code) used to forecast the price of stocks 20 minutes in advance. 

# Summary

The system architecture at the component level is shown below.
![system dataflow](https://user-images.githubusercontent.com/16655278/201698488-6dfbbc70-b290-4b4d-aece-1be117cb8406.png)

Note the features_creator module has been purposely omitted from the uploaded files. 

The entire application can be split into two parts: the online and offline systems. 

**The offline system** is comprised of everything needed to train, optimize and validate the live model. Which includes the data feed, the database manager, the features creator and the training modules for the different models.

**The online system** includes the tick data and level 2 data feed, which pulls the real-time data needed to make live predictions. The freedom_app module uses live data and combines it with historical data to create the features used by the model. The resulting predictions are saved in the live database. The graphical user interface then pulls and displays the data in real-time.
