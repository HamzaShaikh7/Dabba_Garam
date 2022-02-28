import pandas as pd
import numpy as np
from glob import glob
import psycopg2
import os
import time
import getpass




# declaring variable...
__host = "database1.csioilzjaaf3.ap-south-1.rds.amazonaws.com"
__dbname = "database1"
__user = "admin"
__port = 5432
__password = str()
__All_Files = str()

Files_location = str(input("Enter folder location : ")).replace('\\','\\\\')

# Login detail Function...
def login_detail():
    print(f"Host     : {__host}")
    time.sleep(0.7)
    print(f"Database : {__dbname}")
    time.sleep(0.2)
    print(f"Port     : {__port}")
    time.sleep(0.2)
    print(f"User     : {__user}")
    time.sleep(0.2)
    __password_temp = getpass.getpass(prompt='Password : ', stream=None) 
    return __password_temp

#-------------------------------------------------------------------------------------------------------------------------------------------

def import_data():
        df_order_summary = pd.DataFrame()
        if len(os.listdir(Files_location) ) == 0:
                print('File not present on given location either folder is empty...')
                df = pd.DataFrame()
                return df
        else:
                stock_files = sorted(glob(Files_location+'\\\\*.csv'))
                for file in stock_files:
                        df = pd.read_csv(file)
                        outlet = str(str(list(str(list(df.iloc[0,1:2]))[2:-2].split("("))[1:]))
                        outlet = outlet[2:].strip()[:outlet.find(')')]
                        outlet = outlet[:outlet.find(')')]
                        df = df[4:]
                        df = add_outlet(df, outlet)
                        df_order_summary = pd.concat([df_order_summary,df], ignore_index = True)
                return df_order_summary

#-----------------------------------------------------------------------------------------------------------------------------------

def add_outlet(df,outlet):
        df['outlet'] = outlet
        return df

#-----------------------------------------------------------------------------------------------------------------------------------

def cleaning_data(df):

        df = df

        ls = ['Order No.',
                'Order Type',
                'Payment Type',
                'Order Date',
                'Item Name',
                'Qty',
                'Total Selling Price',
                'RAWMATERIAL',
                'Qty',
                'Unit',
                'Total avg.',
                'purchase price',
                'Unnamed: 11',
                'Unnamed: 12',
                'outlet']
        
        df.columns = ls

        df.dropna(subset = ["Order No."], inplace=True)

        df = df[df['Order No.'].str.contains('[Cancelled]') == False]


        df.columns.values[5] = 'Item Qty' 
        df.columns.values[8] = 'RM Qty'

        df = df[['Order No.','outlet','Order Type','Payment Type','Order Date','Item Name','Item Qty','Total Selling Price','RAWMATERIAL','RM Qty','Unit']]


        df['Total Selling Price'] = df['Total Selling Price'].replace("-",0)
        df['Total Selling Price'] = df['Total Selling Price'].replace(np.nan,0)

        
        return df

#------------------------------------------------------------------------------------------------------------------------------------------------

def load_postgreSQL(df):

    df = df
    conn = None
    cur = None


    try:
        conn = psycopg2.connect(
            host = __host,
            dbname = __dbname,
            user = __user,
            password = __password,
            port = __port
            )

        cur = conn.cursor()


        insert_script = '''INSERT INTO order_wise_cunsumption VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


        record_number = 1
        for record in df.values.tolist():
            cur.execute(insert_script,record)

            print(record_number,'[ "'+str(record[0])+'" , "'+str(record[1])+'" , "'+str(record[-1]+'" ]'))
            record_number = record_number + 1

        conn.commit()
        remove_columns()
        info(df)
        time.sleep(20)

    except Exception as error:
        print(error)
    except NameError as error:
        print("Error : Entered month not present in DataBase, please enter a correct month.")
    except psycopg2.OperationalError as error_pass:
        print('\nError : Password incorrect.')

    finally:
        if cur is not None:
            cur.close()

        if conn is not None:
            conn.close()

#-----------------------------------------------------------------------------------------------------------------------------------

def remove_columns():
    stock_files = sorted(glob(Files_location+'\\\\*.csv'))
    for i in stock_files:
        os.remove(i)

#----------------------------------------------------------------------------------------------------------------------------------------

def info(df):
    
    df = df

    print("Which date of data recorded")
    for i in list(df['Order Date'].unique()):
        print(i)
    print()

    outlets_count = 1
    print("Which outlets of data recorded")
    for i in list(df['outlet'].unique()):
        lenght = len(df[df['outlet'] == i] )
        print(f"{outlets_count} : {i} with count : {lenght} ")
        outlets_count = outlets_count + 1

    print()

    print(f"Total number of data recorded  :  {len(df)}.")
    print("Thank You")

#----------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    __password = login_detail()
    dfi = import_data()
    if len(dfi) == 0:
        print("Application terminating...")
        time.sleep(7)
    else:
        df = cleaning_data(dfi)
        load_postgreSQL(df)