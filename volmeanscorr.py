import requests
import json
import pandas as pd
import mysql.connector
from utils import key,contrasenaSQL
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from itertools import groupby
import statistics
import scipy.stats
from scipy.stats import pearsonr

def get_ticker(contrasenaSQL):
    exchange= "US"
    conexion = mysql.connector.connect(
    user="root",
    password=contrasenaSQL,
    host="localhost",
    database="us_tickers",
    port="3306"
    )
    cursor = conexion.cursor()

    insert_query = "SELECT Tiker FROM tickers"
    cursor.execute(insert_query)

    lista_tickers = cursor.fetchall()
    lista_tickers_withoutcoma = [tupla[0] for tupla in lista_tickers] 
    return lista_tickers_withoutcoma
    cursor.close()
    conexion.close()
get_tickers=get_ticker(contrasenaSQL)

"""
nombre_objetivo = "DIISY"
indice_objetivo = get_tickers.index(nombre_objetivo)
print(indice_objetivo)
"""

def get_mean(values_for_month):
    if not values_for_month:
        return 0 
    return round(sum(values_for_month)/len(values_for_month), 2)
def get_std(values_for_month):
    if not values_for_month:
        return 0 
    else:
        try: 
            data = statistics.stdev(values_for_month)  

            return round(data, 3)
        except:
            pass

tiker_list= get_tickers[16000:17000]        
insert_failed_list = []        
for tiker in tiker_list:       
    
    def get_data_price(tiker):
        endpoint = f"https://eodhistoricaldata.com/api/eod/"
        endpoint+= f"{tiker}.US?from=2010-12-30&to=2022-12-31&period=d&fmt=json&&api_token={key}"
        response = requests.get(endpoint)
        if response.status_code == 200:
            json_data = response.json()
            
        else:
            pass
            # Return an empty dictionary or None to indicate failure
            
            


        def get_prices_json(json_data):
            price_list = []
            for data in json_data:
                price_date_json_filtered= data["date"]
                price_value_json_filtered = data["close"]
                price_list.append((price_date_json_filtered,price_value_json_filtered))
            
            return price_list
        
        
        price_list = get_prices_json(json_data)
        def get_month_mean(price_list) :
            common_moth_meaned_values_price = []
            std_values_month= []
            month_list=[]
            grouped_data = groupby(price_list, key=lambda x: x[0][:7])
            print(grouped_data)
            
            # Si la fecha coincide, agregar el valor a la lista de resultados
            
            for month, data_group in grouped_data:
                # Extract the values for the month
                values_for_month = [value for _, value in data_group]      
                # Calculate the mean for the month and round to 2 decimal places
                mean = get_mean(values_for_month)
                standar = get_std(values_for_month)
                std_values_month.append(standar)
                


                
                common_moth_meaned_values_price.append(mean)
                month_list.append(month)
        
            
            return month_list, common_moth_meaned_values_price, std_values_month
        month, month_mean, standar_values = (get_month_mean(price_list))
        
        return month, month_mean, price_list, standar_values
    month, month_mean, price_list, standar_values = get_data_price(tiker) 

    def get_profitability(month_mean):
        profitability = []
        try:
            for i in range(1, len(month_mean)):
                price = month_mean[i]
                previous_price = month_mean[i - 1]
                profit = ((price - previous_price) / previous_price) * 100
                profit=round(profit,3)
                profitability.append(profit)
        except:
            None
        return profitability
        
    profitability = get_profitability(month_mean)

    standar_values_adjusted = standar_values[1:]
    month_adjusted= month[1:]
    
  


    
    try:
        dataframe = {
                "ticker": tiker,
                "date": month_adjusted,
                "month_std_deviation" :  standar_values_adjusted,   
                "month_profitability" : profitability
            }
        df=pd.DataFrame(dataframe)
        
        coef_corr, p_value = pearsonr(standar_values_adjusted, profitability)

        df["correlation"]= coef_corr
        df["p_value"]= p_value
        
        
        
    #----------------------------
        def insert_df_to_mysql(df):

            table_name = 'correlations'

            conexion = create_engine(f'mysql+mysqlconnector://root:{contrasenaSQL}@localhost:3306/correlations')


            
            try:
                df.to_sql(name=table_name, con=conexion, if_exists='append', index=False)

                print("Inserción exitosa.")
            except mysql.connector.Error as err:
                print(f"Error al insertar datos: {err}")
            finally:
                if 'conexion' in locals() and conexion is not None:
                    conexion.dispose()
        instert_df= insert_df_to_mysql(df)

        print(instert_df)
    except:
        print("SQL insert failed ")
            
        insert_failed_list.append(tiker)
print(len(insert_failed_list))
 