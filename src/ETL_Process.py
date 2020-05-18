# -*- coding: utf-8 -*-
"""
Created on Sun Dec  8 15:46:29 2019

@author: Ninad
"""


try:
    import pymongo
    from pymongo import MongoClient
except ImportError:
    raise ImportError('PyMongo is not installed')
import pandas.io.sql as sqlio
import sql
import psycopg2

import csv,json,wbpy,sys, getopt, pprint,requests
import pandas as pd
from pprint import pprint
import numpy as np
import matplotlib.pyplot as plt

import plotly.offline as py
py.init_notebook_mode(connected=True)
import plotly.graph_objs as go
import plotly.tools as tls
import seaborn as sns
import time


def insertTempInMongoFromAPI() :
    
    df1 = pd.read_csv("K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\all.csv")
    df1.head(2)
    df1 = df1.drop(['alpha-2'],axis =1)
    df3 = df1.iloc[:,0]
    df4 = df1.iloc[:,1]
    country_list = []
    iso_code = []
    for i in df4:
        iso_code.append(i)
    print(iso_code)
    
    for j in df3:
       country_list.append(j)
    print(country_list)
    dictionary = dict(zip(iso_code, country_list))
    print(dictionary)
        
    c_api = wbpy.ClimateAPI()
    c_api.ARG_DEFINITIONS["instrumental_intervals"]
    iso_and_basin_codes = iso_code
    dataset = c_api.get_instrumental(data_type="tas", interval = "year",locations=iso_and_basin_codes)
    pprint(dataset.as_dict())
    data1= dataset.as_dict()
   
    """
    tempData2019= c_api.get_instrumental(data_type="tas", interval = "month",locations=iso_and_basin_codes)
    pprint(tempData2019.as_dict())

    newDict1={}
    for kk,vv in tempData2019.as_dict().items():
        try :
            newDict1.update({kk:{2019:(sum(tempData2019.as_dict()[kk])/len(tempData2019.as_dict()[kk]))}})
        except : 
            continue
    
    
    for key1,value1 in data1.items():
        for key2,value2 in tempData2019.as_dict().items():
            if key1==key2 :
                try:
                    data1[key1].update(newDict1[key1])    
                except KeyError:
                    continue
    """
    
    
    finalDict ={}
    for key1,value1 in data1.items():
        for key2,value2 in dictionary.items():
            if key1==key2 :
                finalDict.update({value2:value1})
    pprint(finalDict)
    connection = pymongo.MongoClient('localhost',27017)
    database= connection['DAPDatabase']
    collection=database['temperatureCollection']
    for k,v in finalDict.items():
        data = {k : v}
        collection.insert_one(data)
    connection.close()

def insertHumidityInMongoFromAPI() :
    
df1 = pd.read_csv("K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\all.csv")
    df1.head(2)
    df1 = df1.drop(['alpha-2'],axis =1)
    df3 = df1.iloc[:,0]
    df4 = df1.iloc[:,1]
    country_list = []
    iso_code = []
    for i in df4:
        iso_code.append(i)
    print(iso_code)
    
    for j in df3:
       country_list.append(j)
    print(country_list)
    dictionary = dict(zip(iso_code, country_list))
    print(dictionary)
        
    c_api = wbpy.ClimateAPI()
    c_api.ARG_DEFINITIONS["instrumental_intervals"]
    iso_and_basin_codes = iso_code
    dataset = c_api.get_instrumental(data_type="pr", interval = "year",locations=iso_and_basin_codes)
    pprint(dataset.as_dict())
    data1= dataset.as_dict()
   
    """
    tempData2019= c_api.get_instrumental(data_type="pr", interval = "month",locations=iso_and_basin_codes)
    pprint(tempData2019.as_dict())

    newDict1={}
    for kk,vv in tempData2019.as_dict().items():
        try :
            newDict1.update({kk:{2019:(sum(tempData2019.as_dict()[kk])/len(tempData2019.as_dict()[kk]))}})
        except : 
            continue
    
    
    for key1,value1 in data1.items():
        for key2,value2 in tempData2019.as_dict().items():
            if key1==key2 :
                try:
                    data1[key1].update(newDict1[key1])    
                except KeyError:
                    continue 
    
  """  
    finalDict ={}
    for key1,value1 in data1.items():
        for key2,value2 in dictionary.items():
            if key1==key2 :
                finalDict.update({value2:value1})
    pprint(finalDict)
    connection = pymongo.MongoClient('localhost',27017)
    database= connection['DAPDatabase']
    collection=database['precipitationCollection']
    for k,v in finalDict.items():
        data = {k : v}
        collection.insert_one(data)
    connection.close()
    
    

def insertInMongoFromFile():
    #CSV to JSON Conversion
    csvfile = open('C://test//final-current.csv', 'r')
    reader = csv.DictReader( csvfile )
    mongo_client=MongoClient() 
    db=mongo_client.october_mug_talk
    db.segment.drop()
    header= [ "S No", "Instrument Name", "Buy Price", "Buy Quantity", "Sell Price", "Sell Quantity", "Last Traded Price", "Total Traded Quantity", "Average Traded Price", "Open Price", "High Price", "Low Price", "Close Price", "V" ,"Time"]
    
    for each in reader:
        row={}
        for field in header:
            row[field]=each[field]
            
            db.segment.insert(row)


def insertInMongoDBFromJSON():

    mng_client = pymongo.MongoClient('localhost', 27017)
    mng_db = mng_client['DAPDatabase'] # Replace mongo db name
    collection_name = 'seaLevelCollection' # Replace mongo db collection name
    db_cm = mng_db[collection_name]
    
    # Get the data from JSON file
    with open(r'K:\NCI\Study\Sem 1\Datasets\DAP\SeaLevelInfo.json', 'r') as data_file:
        data_json = json.load(data_file)
        
        #Insert Data
    db_cm.remove()
    db_cm.insert(data_json)
    mng_client.close()
    
 
# Query data
#db_cm.UNS_Collection2.find().pretty()


def insertInMongoFromCSV():

    #CSV to JSON Conversion
    csvfile = open(r'K:\NCI\Study\Sem 1\Datasets\DAP\GlobalTemperatures.csv', 'r')
    reader = csv.DictReader( csvfile )
    mongo_client=MongoClient('localhost', 27017) 
    db=mongo_client['DAPDatabase']
    db.GlobalTemperaturesCollection.drop()
    header= [ "dt","LandAverageTemperature","LandAverageTemperatureUncertainty","LandMaxTemperature","LandMaxTemperatureUncertainty","LandMinTemperature","LandMinTemperatureUncertainty","LandAndOceanAverageTemperature","LandAndOceanAverageTemperatureUncertainty"]

    for each in reader:
        row={}
        for field in header:
            row[field]=each[field]

        db.GlobalTemperaturesCollection.insert_one(row)
    mongo_client.close()

def insertCountryInMongoFromCSV():

    #CSV to JSON Conversion
    csvfile = open(r'K:\NCI\Study\Sem 1\Datasets\DAP\GlobalLandTemperaturesByCountry.csv', 'r')
    reader = csv.DictReader( csvfile )
    mongo_client=MongoClient('localhost', 27017) 
    db=mongo_client['DAPDatabase']
    db.GlobalTemperaturesCountryCollection.drop()
    header= ["dt","AverageTemperature","AverageTemperatureUncertainty","Country"]

    for each in reader:
        row={}
        for field in header:
            row[field]=each[field]

        db.GlobalTemperaturesCountryCollection.insert_one(row)
    mongo_client.close()


def readDataFromMongoSeaLevel():
    connection = pymongo.MongoClient('localhost',27017)
    database= connection['DAPDatabase']
    collection=database['seaLevelCollection']
    data = collection.find()
    connection.close()
    listValues = list(data)
    df = pd.DataFrame(listValues) 
    df.head()
    df = df[['GMSL','Time']]
    df.columns = ['GMSL','Date']
    df = df[["Date","GMSL"]] 
    """
    for listIter in listValues:
        for k in listIter:
            #data2=listIter.get(k)
            if k != "_id" and k!= "GMSL uncertainty":
                print(k)
    
    """
    return df

def readDataFromMongoCountryTemp():
    connection = pymongo.MongoClient('localhost',27017)
    database= connection['DAPDatabase']
    collection=database['GlobalTemperaturesCountryCollection']
    data = collection.find()
    connection.close()
    listValuesCountryTemp = list(data)
    dfCountryTemp = pd.DataFrame(listValuesCountryTemp) 
    dfCountryTemp.head()
    dfCountryTemp = dfCountryTemp[["dt","AverageTemperature","Country"]]
    dfCountryTemp.columns = ['Date',"AverageTemperature","Country"]
    return dfCountryTemp

def readDataFromMongoGlobalTemp():
    connection = pymongo.MongoClient('localhost',27017)
    database= connection['DAPDatabase']
    collection=database['GlobalTemperaturesCollection']
    data = collection.find()
    connection.close()
    listValuesGlobalTemp = list(data)
    dfGlobalTemp = pd.DataFrame(listValuesGlobalTemp) 
    dfGlobalTemp.head()
    dfGlobalTemp.dtypes
    dfGlobalTemp = dfGlobalTemp[["dt","LandAverageTemperature","LandMaxTemperature","LandMinTemperature","LandAndOceanAverageTemperature"]]
    dfGlobalTemp.columns = ["Date","LandAverageTemperature","LandMaxTemperature","LandMinTemperature","LandAndOceanAverageTemperature"]
    dfGlobalTemp = dfGlobalTemp[["Date","GMSL"]] 
    return dfGlobalTemp

def readDataFromMongoAPITemp():
    connection = pymongo.MongoClient('localhost',27017)
    database= connection['DAPDatabase']
    collection=database['temperatureCollection']
    data = collection.find()
    connection.close()
    listValuesAPITemp = list(data)
    listValuesAPITemp[0:2]
    x =[]
    a= []
    for i in listValuesAPITemp:
        for k in i:
            if k != "_id":
                x.append(i.get(k))
                a.append(k)
    randomList=[]  
    randomList1=[]   
    randomList2=[]    
    randomList3=[]
    listx = []
    for e in a:
        randomList1.append(e)
        for  z in x:
            if a.index(e) == x.index(z):
                for q in z:
                   
                # randomList.append(randomList1[-1])
                    randomList2.append(q)
                    randomList2.append(z.get(q))
                #randomList.append(randomList1)
                    randomList.append([randomList1[-1],randomList2[-1],randomList2[-2]])
                        #randomList.append(randomList2[-2])
    randomList[]
              
               # randomLt.append(z.get(q))
     
    dfAPITemp = pd.DataFrame(randomList)  
    dfAPITemp.columns = ['Country',"AverageTemperature","Year"]



def readDataFromMongoAPIPrecip():
    connection = pymongo.MongoClient('localhost',27017)
    database= connection['DAPDatabase']
    collection=database['precipitationCollection']
    data = collection.find()
    connection.close()
    listValuesAPIPrec = list(data)
    listValuesAPIPrec[0:2]
    xPrec =[]
    aPrec= []
    for i in listValuesAPIPrec:
        for k in i:
            if k != "_id":
                xPrec.append(i.get(k))
                aPrec.append(k)
    randomListPrec=[]  
    randomList1Prec=[]   
    randomList2Prec=[]    
    randomList3Prec=[]
    listx = []
    for e in aPrec:
        randomList1Prec.append(e)
        for  z in xPrec:
            if aPrec.index(e) == xPrec.index(z):
                for q in z:
                   
                # randomList.append(randomList1[-1])
                    randomList2Prec.append(q)
                    randomList2Prec.append(z.get(q))
                #randomList.append(randomList1)
                    randomListPrec.append([randomList1Prec[-1],randomList2Prec[-1],randomList2Prec[-2]])
                        #randomList.append(randomList2[-2])
    randomListPrec[0:10]
              
               # randomLt.append(z.get(q))
     
    dfAPIPrec = pd.DataFrame(randomListPrec)  
    dfAPIPrec.columns = ['Country',"AveragePrecipitation","Year"]
        



def cleanData():
    dfCountryTemp.head()
    dfCountryTemp[["AverageTemperature"]].head()
    dfCountryTemp['AverageTemperature'] = dfCountryTemp['AverageTemperature'].str.strip()
    dfCountryTemp['AverageTemperature'] = dfCountryTemp['AverageTemperature'].replace("","xyz")
    dfCountryTemp['AverageTemperature'] = dfCountryTemp['AverageTemperature'].replace("xyz",None)
    dfCountryTemp['AverageTemperature']=dfCountryTemp[~dfCountryTemp['AverageTemperature'].str.contains("xyz")]
    dfCountryTemp['AverageTemperature'].isnull().sum()
    dfCountryTemp = dfCountryTemp.dropna(subset=['AverageTemperature'])
    
    global_temp_country_clear = dfCountryTemp[~dfCountryTemp['Country'].isin(['Denmark', 'Antarctica', 'France', 'Europe', 'Netherlands','United Kingdom', 'Africa', 'South America'])]
    global_temp_country_clear = global_temp_country_clear.replace(['Denmark (Europe)', 'France (Europe)', 'Netherlands (Europe)', 'United Kingdom (Europe)'],['Denmark', 'France', 'Netherlands', 'United Kingdom'])
    countries = np.unique(global_temp_country_clear['Country'])
    mean_temp = []
    
    global_temp_country_clear.dtypes
    global_temp_country_clear['AverageTemperature']=global_temp_country_clear['AverageTemperature'].astype(float)
    
    for country in countries:
        mean_temp.append(global_temp_country_clear[global_temp_country_clear['Country'] == country]['AverageTemperature'].mean())
        data = [ dict(type = 'choropleth',locations = countries,z = mean_temp,locationmode = 'country names',text = countries,marker = dict( line = dict(color = 'rgb(0,0,0)', width = 1)), colorbar = dict(autotick = True, tickprefix = '', title = '# Average\nTemperature,\n°C'))]
        layout = dict(title = 'Average land temperature in countries', geo = dict(showframe = False, showocean = True,oceancolor = 'rgb(0,255,255)',projection = dict(type = 'orthographic',rotation = dict(lon = 60, lat = 10),), lonaxis =  dict(
                showgrid = True,
                gridcolor = 'rgb(102, 102, 102)'
            ),
        lataxis = dict(
                showgrid = True,
                gridcolor = 'rgb(102, 102, 102)'
                )
            ),
        )

fig = dict(data=data, layout=layout)
py.iplot(fig, validate=False, filename='worldmap')



def visualisation2():
    years = np.unique(dfGlobalTemp['dt'].apply(lambda x: x[:4]))
    mean_temp_world = []
    mean_temp_world_uncertainty = []
    dfGlobalTemp['LandAverageTemperature'].isnull().sum()
    dfGlobalTemp['LandAverageTemperature'] = dfGlobalTemp['LandAverageTemperature'].replace("","xyz")
    dfGlobalTemp['LandAverageTemperature'] = dfGlobalTemp['LandAverageTemperature'].replace("xyz",None)
    dfGlobalTemp['LandAverageTemperature']=dfGlobalTemp['LandAverageTemperature'].astype(float)

    dfGlobalTemp['LandAverageTemperatureUncertainty'].isnull().sum()    
    dfGlobalTemp['LandAverageTemperatureUncertainty'] = dfGlobalTemp['LandAverageTemperatureUncertainty'].replace("","xyz")
    dfGlobalTemp['LandAverageTemperatureUncertainty'] = dfGlobalTemp['LandAverageTemperatureUncertainty'].replace("xyz",None)
    dfGlobalTemp['LandAverageTemperatureUncertainty']=dfGlobalTemp['LandAverageTemperatureUncertainty'].astype(float)
    
    dfGlobalTemp = dfGlobalTemp.dropna(subset=['LandAverageTemperature'])
    dfGlobalTemp = dfGlobalTemp.dropna(subset=['LandAverageTemperatureUncertainty'])
    dfGlobalTemp.dtypes


    for year in years:
        mean_temp_world.append(dfGlobalTemp[dfGlobalTemp['dt'].apply(
                lambda x: x[:4]) == year]['LandAverageTemperature'].mean())
        mean_temp_world_uncertainty.append(dfGlobalTemp[dfGlobalTemp['dt'].apply(
                lambda x: x[:4]) == year]['LandAverageTemperatureUncertainty'].mean())
    
    trace0 = go.Scatter(
            x = years, 
            y = np.array(mean_temp_world) + np.array(mean_temp_world_uncertainty),
            fill= None,
            mode='lines',
            name='Uncertainty top',
            line=dict(
                    color='rgb(0, 255, 255)',
                    )
            )
    trace1 = go.Scatter(
            x = years, 
            y = np.array(mean_temp_world) - np.array(mean_temp_world_uncertainty),
            fill='tonexty',
            mode='lines',
            name='Uncertainty bot',
            line=dict(
                    color='rgb(0, 255, 255)',
                            )
                    )

    trace2 = go.Scatter(
            x = years, 
            y = mean_temp_world,
            name='Average Temperature',
            line=dict(
                    color='rgb(199, 121, 093)',
                    )
            )
    data = [trace0, trace1, trace2]

    layout = go.Layout(
            xaxis=dict(title='year'),
            yaxis=dict(title='Average Temperature, °C'),
            title='Average land temperature in world',
            showlegend = False)

    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)


def visualization3():
    df['GMSL'].isnull().sum()
    df['Date'].isnull().sum()
    yearsFromDate = np.unique(df['Date'].apply(lambda x: x[:4]))
    mean_sea_level= []
    for year in yearsFromDate:
        mean_sea_level.append(df[df['Date'].apply(
                lambda x: x[:4]) == year]['GMSL'].mean())
     
    
    trace = go.Scatter(
        x = yearsFromDate, 
        y = mean_sea_level,
        name='Average Sea Level',
        line=dict(
                color='rgb(199, 121, 093)',
                )
        )
    data = [trace]
    layout = go.Layout(
            xaxis=dict(title='year'),
            yaxis=dict(title='Average Temperature, °C'),
            title='Average land temperature in world',
            showlegend = False)

    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)


def visualisation4():    
    dfAPIPrec['AveragePrecipitation'].isnull().sum()
    dfAPITemp['AverageTemperature'].isnull().sum()
    yearsFromDateTempAPI = np.unique(dfAPITemp['Year'])
    yearsFromDatePrecAPI = np.unique(dfAPIPrec['Year'])
    dfAPIPrec.dtypes
    dfAPITemp.dtypes
    DiffTempCount=[]
    DiffPrecCount=[]
    AvgTempCount=[]
    AvgPrecCount=[]
    countriesUnique = np.unique(dfAPITemp['Country'])
  
    for country in countriesUnique:
        DiffTempCount.append(dfAPITemp[dfAPITemp['Country'] == country]['AverageTemperature'].max()-dfAPITemp[dfAPITemp['Country'] == country]['AverageTemperature'].min())
        DiffPrecCount.append(dfAPIPrec[dfAPIPrec['Country'] == country]['AveragePrecipitation'].max()-dfAPIPrec[dfAPIPrec['Country'] == country]['AveragePrecipitation'].min())
        AvgTempCount.append(dfAPITemp[dfAPITemp['Country'] == country]['AverageTemperature'].mean())
        AvgPrecCount.append(dfAPIPrec[dfAPIPrec['Country'] == country]['AveragePrecipitation'].mean())
        
        
        
    trace0 = go.Scatter(
        x = yearsFromDateTempAPI, 
        y = dfAPITemp[dfAPITemp['Country']=='Australia']['AverageTemperature'],
        mode='lines',
        name='Australia',
        line=dict(
                color='rgb(199, 121, 093)',
                )
        )
        
    trace1 = go.Scatter(
        x = yearsFromDateTempAPI, 
        y = dfAPITemp[dfAPITemp['Country']=='India']['AverageTemperature'],
        mode='lines',
        name='India',
        line=dict(
                color='rgb(0, 255, 255)',
                        )
                )
    trace2 = go.Scatter(
        x = yearsFromDateTempAPI, 
        y = dfAPITemp[dfAPITemp['Country']=='Ireland']['AverageTemperature'],
        mode='lines',
        name='Ireland',
        line=dict(
                color='rgb(35, 190, 148)',
                        )
                )
    trace3 = go.Scatter(
        x = yearsFromDateTempAPI, 
        y = dfAPITemp[dfAPITemp['Country']=='United States of America']['AverageTemperature'],
        mode='lines',
        name='United States of America',
        line=dict(
                color='rgb(140, 80, 190)',
                        )
                )
    trace4 = go.Scatter(
        x = yearsFromDateTempAPI, 
        y = dfAPITemp[dfAPITemp['Country']=='Greece']['AverageTemperature'],
        mode='lines',
        name='Greece',
        line=dict(
                color='rgb(220, 130, 50)',
                        )
                )
    
        
    data = [trace0,trace1,trace2,trace3,trace4]
    layout = go.Layout(
            xaxis=dict(title='year'),
            yaxis=dict(title='Average Temperature'),
            title='Average temperature of random 5 countries in world'
            )

    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)
    
    
def visualisation5():
    dfAPIPrec['AveragePrecipitation'].isnull().sum()
    dfAPITemp['AverageTemperature'].isnull().sum()
    yearsFromDateTempAPI = np.unique(dfAPITemp['Year'])
    yearsFromDatePrecAPI = np.unique(dfAPIPrec['Year'])
    dfAPIPrec.dtypes
    dfAPITemp.dtypes
    DiffTempCount=[]
    DiffPrecCount=[]
    AvgTempCount=[]
    AvgPrecCount=[]
    countriesUnique = np.unique(dfAPITemp['Country'])
  
    for country in countriesUnique:
        DiffTempCount.append(dfAPITemp[dfAPITemp['Country'] == country]['AverageTemperature'].max()-dfAPITemp[dfAPITemp['Country'] == country]['AverageTemperature'].min())
        DiffPrecCount.append(dfAPIPrec[dfAPIPrec['Country'] == country]['AveragePrecipitation'].max()-dfAPIPrec[dfAPIPrec['Country'] == country]['AveragePrecipitation'].min())
        AvgTempCount.append(dfAPITemp[dfAPITemp['Country'] == country]['AverageTemperature'].mean())
        AvgPrecCount.append(dfAPIPrec[dfAPIPrec['Country'] == country]['AveragePrecipitation'].mean())
        
        
        
    trace0 = go.Scatter(
        x = yearsFromDatePrecAPI, 
        y = dfAPIPrec[dfAPIPrec['Country']=='Australia']['AveragePrecipitation'],
        mode='lines',
        name='Australia',
        line=dict(
                color='rgb(199, 121, 093)',
                )
        )
        
    trace1 = go.Scatter(
        x = yearsFromDatePrecAPI, 
        y = dfAPIPrec[dfAPIPrec['Country']=='India']['AveragePrecipitation'],
        mode='lines',
        name='India',
        line=dict(
                color='rgb(220, 130, 50)',
                        )
                )
    trace2 = go.Scatter(
        x = yearsFromDatePrecAPI, 
        y = dfAPIPrec[dfAPIPrec['Country']=='Ireland']['AveragePrecipitation'],
        mode='lines',
        name='Ireland',
        line=dict(
                color='rgb(35, 190, 50)',
                        )
                )
    trace3 = go.Scatter(
        x = yearsFromDatePrecAPI, 
        y = dfAPIPrec[dfAPIPrec['Country']=='United States of America']['AveragePrecipitation'],
        mode='lines',
        name='United States of America',
        line=dict(
                color='rgb(140, 80, 120)',
                        )
                )
    trace4 = go.Scatter(
        x = yearsFromDatePrecAPI, 
        y = dfAPIPrec[dfAPIPrec['Country']=='Greece']['AveragePrecipitation'],
        mode='lines',
        name='Greece',
        line=dict(
                color='rgb(0, 255, 255)',
                        )
                )
    
        
    data = [trace0,trace1,trace2,trace3,trace4]
    layout = go.Layout(
            xaxis=dict(title='year'),
            yaxis=dict(title='Average Precipitation'),
            title='Average precipitation of random 5 countries in world'
            )

    fig = go.Figure(data=data, layout=layout)
    py.iplot(fig)
    
    
    
def createDBInPostGre():

    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "postgres")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute('CREATE DATABASE DAPData1;')
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()
    
def PushSeaLevelIntoPostGre():  
#creating table  seaLevel   
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("""
    CREATE TABLE seaLevelData(
    dateOfEvent varchar(15),
    GMSL varchar(10)
    );
        """)
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()
        
#pushing values 
#write df to csv
    df.to_csv(r'K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\Datasets\\seaLevelCleaned.csv',index=False)    
        
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
       
        with open('K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\Datasets\\seaLevelCleaned.csv', 'r') as seaLevelValues:
            reader = csv.reader(seaLevelValues)
            next(reader) # skip the header
            for row in reader:
                dbCursor.execute("INSERT INTO seaLevelData VALUES (%s,%s)",row)
        dbConnection.commit()
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        if(dbConnection): dbConnection.close()
        
#retrieving data back to use it in visualization      
    sql = "SELECT * from seaLevelData;"
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        seaLevel_dataframe = sqlio.read_sql_query(sql, dbConnection)
    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        if(dbConnection): dbConnection.close()
    

def PushTemperatureAPIIntoPostGre():  
#creating table  TempAPI   
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("""
    CREATE TABLE TempAPIData(
    countryName varchar(100),
    AverageTemp varchar(100),
    YearOcc varchar(10)
    );
        """)
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()
        
#pushing values 
#write df to csv
    dfAPITemp .to_csv(r'K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\Datasets\\APITempCleaned.csv',index=False)    
        
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
       
        with open('K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\Datasets\\APITempCleaned.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader) # skip the header
            for row in reader:
                dbCursor.execute("INSERT INTO TempAPIData VALUES (%s,%s,%s)",row)
        dbConnection.commit()
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        if(dbConnection): dbConnection.close()
        
#retrieving data back to use it in visualization      
    sql = "SELECT * from TempAPIData;"
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        APITemp_dataframe = sqlio.read_sql_query(sql, dbConnection)
    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        if(dbConnection): dbConnection.close()    

def PushPrecAPIIntoPostGre():  
#creating table  PrecAPI   
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("""
    CREATE TABLE PrecAPIData(
    countryName varchar(100),
    AveragePrec varchar(100),
    YearOcc varchar(10)
    );
        """)
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()
        
#pushing values 
#write df to csv
    dfAPITemp .to_csv(r'K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\Datasets\\APIPrecCleaned.csv',index=False)    
        
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
       
        with open('K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\Datasets\\APITempCleaned.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader) # skip the header
            for row in reader:
                dbCursor.execute("INSERT INTO PrecAPIData VALUES (%s,%s,%s)",row)
        dbConnection.commit()
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        if(dbConnection): dbConnection.close()
        
#retrieving data back to use it in visualization      
    sql = "SELECT * from PrecAPIData;"
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        APIPrec_dataframe = sqlio.read_sql_query(sql, dbConnection)
    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        if(dbConnection): dbConnection.close()    

def PushGlobalTempIntoPostGre():  
#creating table GLobalTemp   
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute("""
    CREATE TABLE GlobalTempData(
    DateOcc varchar(20),
    LandAverageTemperature varchar(20),
    LandAverageTemperatureUncertainty varchar(100)
    );
        """)
        dbCursor.close()

    except (Exception , psycopg2.Error) as dbError :
        print ("Error while connecting to PostgreSQL", dbError)
    finally:
        if(dbConnection): dbConnection.close()
        
#pushing values 
#write df to csv
    dfGlobalTempCleaned = dfGlobalTemp[['dt','LandAverageTemperature','LandAverageTemperatureUncertainty']]
    dfGlobalTempCleaned .to_csv(r'K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\Datasets\\GlobalTempCleaned.csv',index=False)    
        
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        dbConnection.set_isolation_level(0) # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
       
        with open('K:\\NCI\\Study\\Sem 1\\Datasets\\DAP\\Datasets\\GlobalTempCleaned.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader) # skip the header
            for row in reader:
                dbCursor.execute("INSERT INTO GlobalTempData VALUES (%s,%s,%s)",row)
        dbConnection.commit()
        dbCursor.close()
    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        if(dbConnection): dbConnection.close()
        
#retrieving data back to use it in visualization      
    sql = "SELECT * from GlobalTempData;"
    try:
        dbConnection = psycopg2.connect(
            user = "dap",
            password = "dap",
            host = "192.168.56.30",
            port = "5432",
            database = "dapdata1")
        GlobalTemp_dataframe = sqlio.read_sql_query(sql, dbConnection)
    except (Exception , psycopg2.Error) as dbError :
        print ("Error:", dbError)
    finally:
        if(dbConnection): dbConnection.close()    


class MongoDB(object):
    def __init__(self, host='localhost', port=27017, database_name=None, collection_name=None):
        try:
            self._connection = MongoClient(host=host, port=port, maxPoolSize=200)
        except Exception as error:
            raise Exception(error)
        self._database = None
        self._collection = None
        if database_name:
            self._database = self._connection[database_name]
        if collection_name:
            self._collection = self._database[collection_name]

    def insert(self, post):
        # add/append/new single record
        post_id = self._collection.insert_one(post).inserted_id
        return post_id


url = 'https://api.waqi.info/feed/beijing/?token=3ab4e36913fdace838b17cf42a48ee06ab97d744'
response = requests.get(url)
data = response.text
#data = response.json()
if response.status_code != 200:
    print('Failed to get data:', response.status_code)
else:
    print('First 100 characters of data are')
    print(data[:100])

print('[*] Parsing response text')
data = data.split('\n')
data_list = list()
for value in data:
    if 'year,data' not in value:
        if value:
            value = value.split(',')
            data_list.append({'year': char(value[1]), 'data': float(value[1])})

print(data_list)

print('[*] Pushing data to MongoDB ')
mongo_db = MongoDB(database_name='Climate_DB', collection_name='climate_data')

for collection in data_list:
    print('[!] Inserting - ', collection)
    mongo_db.insert(collection)
    







