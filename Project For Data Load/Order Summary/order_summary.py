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
                'Client OrderID',
                'Order Type',
                'Sub Order Type',
                'Customer Name',
                'Customer Phone',
                'GSTIN',
                'Customer Address',
                'Delivery Boy',
                'Delivery Boy Number',
                'Items',
                'My Amount',
                'Total Discount',
                'Delivery Charge',
                'Container Charge',
                'Total Tax',
                'Round Off',
                'Grand Total',
                'Payment Type',
                'Payment Description',
                'Status',
                'Created',
                'Sequence Name',
                'outlet']

        df.columns = ls
        
        df['Sub Order Type'] = df['Sub Order Type'].replace(np.nan, 'Autiler')
        temp_list = []
        for i in df['Sub Order Type']:
                if 'Zomato' in i:
                        temp_list.append("Zomato")
                elif 'Swiggy' in i:
                        temp_list.append("Swiggy")
                else:
                        temp_list.append("Autiler")
        df['Sub Order Type'] = temp_list

        temp_02 = df['Created'].str.split(" " , n = 4 , expand = True)
        df['Date'] = temp_02[0]+'/'+temp_02[1]+'/'+temp_02[2]

        df = df[['Order No.',
                'Client OrderID',
                'Order Type',
                'Sub Order Type',
                'Customer Name',
                'Customer Address',
                'My Amount',
                'Total Discount',
                'Delivery Charge',
                'Container Charge',
                'Status',
                'Date',
                'Created',
                'outlet']]

        return df

#---------------------------------------------------------------------------------------------------------------------------------

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


        insert_script = '''INSERT INTO order_summary VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''


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
    for i in list(df['Date'].unique()):
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

#-------------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    __password = login_detail()
    dfi = import_data()
    if len(dfi) == 0:
        print("Application terminating...")
        time.sleep(7)
    else:
        df = cleaning_data(dfi)
        load_postgreSQL(df)