import pandas as pd
import numpy as np
from glob import glob
import psycopg2
import os
import time
import getpass
from datetime import datetime




# declaring variable...
__host = "database1.csioilzjaaf3.ap-south-1.rds.amazonaws.com"
__dbname = "database1"
__user = "admin"
__port = 5432
__password = str()
__All_Files = str()


Files_location = str(input("Enter folder location : ")).replace('\\','\\\\')


#---------------------------------------------------------------------------------------------------------------------


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


#---------------------------------------------------------------------------------------------------------------------


def import_data():

    if len(os.listdir(Files_location) ) == 0:
        print('File not present on given location either folder is empty...')
        df = pd.DataFrame()
        return df
    else:
        stock_files = sorted(glob(Files_location+'\\\\*.csv'))
        df = pd.concat((pd.read_csv(file).assign(filename = file) for file in stock_files), ignore_index = True)
        return df

#---------------------------------------------------------------------------------------------------------------------


def creating_outlets(df):
    df = df.copy()
    df['outlet'] = df['outlet'].astype(int)
    
    # create a list of our conditions
    conditions = [
        (df['outlet'] == 21305),
        (df['outlet'] == 21125),
        (df['outlet'] == 23268),
        (df['outlet'] == 23501),
        (df['outlet'] == 23729),
        (df['outlet'] == 24382),
        (df['outlet'] == 34633),
        (df['outlet'] == 46303),
        (df['outlet'] == 46304),
        (df['outlet'] == 52354),
        (df['outlet'] == 54615),
        (df['outlet'] == 59264)
        ]

    # create a list of the values we want to assign for each condition
    values = ['Kasarvadavli','Brahmand','Vrindavan','charai','Tulsidham','Airoli','Powai','Vashi','Kandivali','Andheri West','Chembur','Mira/Bhayandar']

    # create a new column and use np.select to assign values to it using our lists as arguments
    df['Outlet'] = np.select(conditions, values)

    return df


#---------------------------------------------------------------------------------------------------------------------


def cleaning_data(dfi):

    df = dfi
    df = df.copy()

    df.columns = ['Raw Material','Existing Stock','Unit','Current Stock','Comments','filename']


    temp = df['filename'].str.split("_" , n = 2, expand = True)
    df['outlet'] = temp[1].str.split("-", n=4 , expand = True)[0]


    df['Existing Stock'] = df['Existing Stock'].replace('-',np.nan)
    df['Existing Stock'] = df['Existing Stock'].astype(float)
    df['Existing Stock'] = df['Existing Stock'].replace(0,np.nan)


    df.dropna(subset = ['Existing Stock'], inplace = True)

    df = creating_outlets(df)

    df['Time'] = datetime.now().strftime('%d-%m-%Y %H:%M')

    df = df[['Raw Material','Existing Stock','Time','Outlet']]

    return df

#---------------------------------------------------------------------------------------------------------------------


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


        insert_script = '''INSERT INTO stock_data VALUES (%s,%s,%s,%s)'''


        record_number = 1
        for record in df.values.tolist():
            cur.execute(insert_script,record)

            print(record_number,record)
            record_number = record_number + 1

        conn.commit()
        remove_columns()
        info(df)
        time.sleep(20)

    except Exception as error:
        print(error)
        time.sleep(20)
    except NameError as error:
        print("Error : Entered month not present in DataBase, please enter a correct month.")
        time.sleep(20)
    except psycopg2.OperationalError as error_pass:
        print('\nError : Password incorrect.')
        time.sleep(20)

    finally:
        if cur is not None:
            cur.close()

        if conn is not None:
            conn.close()


#---------------------------------------------------------------------------------------------------------------------


def remove_columns():
    stock_files = sorted(glob(Files_location+'\\\\*.csv'))
    for i in stock_files:
        os.remove(i)


#---------------------------------------------------------------------------------------------------------------------


def info(df):
    
    df = df

    print("Which date of data recorded")
    for i in list(df['Time'].unique()):
        print(i)
    print()

    outlets_count = 1
    print("Which outlets of data recorded")
    for i in list(df['Outlet'].unique()):
        lenght = len(df[df['Outlet'] == i] )
        print(f"{outlets_count} : {i} with count : {lenght} ")
        outlets_count = outlets_count + 1

    print()

    print(f"Total number of data recorded  :  {len(df)}.")
    print("Thank You")

if __name__ == "__main__":

    __password = login_detail()
    dfi = import_data()
    if len(dfi) == 0:
        print("Application terminating...")
        time.sleep(7)
    else:
        df = cleaning_data(dfi)
        load_postgreSQL(df)