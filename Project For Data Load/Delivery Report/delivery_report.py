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


def cleaning_data(df):
    df = df

    temp_df_01 = df["Delivered at"].str.split("-", n = 3, expand = True)
    df["Year"]= temp_df_01[0]
    df["Months"]= temp_df_01[1]
    df['Date_tmp'] = temp_df_01[2]

    temp_df_02 = df["Date_tmp"].str.split(" ", n = 2, expand = True)
    df['Date'] = temp_df_02[0].astype(int)
    df = df.drop(columns = df[['Date_tmp']])

    df = creating_months(df)



    #                                   Tiem Difference between Slot End time to Deliver
    TD_slo_del_tmp_list = []

    for i,j in zip(df['Delivered at'],df['Delivery slot end']):
      if (str(pd.to_datetime(i) - pd.to_datetime(j)).startswith('-')):
        value01 = f"-{str(pd.to_datetime(j) - pd.to_datetime(i))[-8:-3]}"
        TD_slo_del_tmp_list.append(value01.replace(":","."))
      else:
        value02 = str(pd.to_datetime(i) - pd.to_datetime(j))[-8:-3]
        TD_slo_del_tmp_list.append(value02.replace(":","."))

    TD_slo_del_tmp_Series = pd.Series(TD_slo_del_tmp_list)
    df['TD_slo_del'] = pd.concat({"TD_slo_del":TD_slo_del_tmp_Series}, axis=1)



    #                                   Time Difference between Dispatched to delivered at
    TD_dip_del_tmp_list = []

    for i,j in zip(df['Delivered at'],df['Dispatched at']):
      if (str(pd.to_datetime(i) - pd.to_datetime(j)).startswith('0')):
        value01 = str(pd.to_datetime(i) - pd.to_datetime(j))[-8:-3]
        TD_dip_del_tmp_list.append(value01.replace(":","."))
      else:
        TD_dip_del_tmp_list.append(np.nan)


    TD_dis_del_tmp_Series = pd.Series(TD_dip_del_tmp_list)
    df['TD_dip_del'] = pd.concat({"TD_dip_del":TD_dis_del_tmp_Series}, axis=1)



    #                                 Time Difference between Dispatched to Delivery Slot End Time
    TD_dip_slo_tmp_list = []

    for i,j in zip(df['Dispatched at'],df['Delivery slot end']):
      if (str(pd.to_datetime(i) - pd.to_datetime(j)).startswith('-')):
        value01 = f"-{str(pd.to_datetime(j) - pd.to_datetime(i))[-8:-3]}"
        TD_dip_slo_tmp_list.append(value01.replace(":","."))
        
      elif (str(pd.to_datetime(i) - pd.to_datetime(j)).startswith('0')):
        value02 = f"{str(pd.to_datetime(i) - pd.to_datetime(j))[-8:-3]}"
        TD_dip_slo_tmp_list.append(value02.replace(":","."))
      else:
        TD_dip_slo_tmp_list.append(np.nan)


    TD_dip_slo_tmp_Series = pd.Series(TD_dip_slo_tmp_list)
    df['TD_dip_slo'] = pd.concat({"TD_dip_slo":TD_dip_slo_tmp_Series}, axis=1)



    #                                 Time Difference between Rider Pick to  Dispatched Time
    TD_Rpick_dip_tmp_list = []

    for i,j in zip(df['Ready for pickup at'],df['Dispatched at']):
      if (str(pd.to_datetime(i) - pd.to_datetime(j)).startswith('-')):
        value01 = f"-{str(pd.to_datetime(j) - pd.to_datetime(i))[-8:-3]}"
        TD_Rpick_dip_tmp_list.append(value01.replace(":","."))
        
      elif (str(pd.to_datetime(i) - pd.to_datetime(j)).startswith('0')):
        value02 = f"{str(pd.to_datetime(i) - pd.to_datetime(j))[-8:-3]}"
        TD_Rpick_dip_tmp_list.append(value02.replace(":","."))
      else:
        TD_Rpick_dip_tmp_list.append(np.nan)


    TD_Rpick_dip_tmp_Series = pd.Series(TD_Rpick_dip_tmp_list)
    df['TD_Rpick_dip'] = pd.concat({"TD_Rpick_dip":TD_Rpick_dip_tmp_Series}, axis=1)
    df['TD_slo_del'] = df['TD_slo_del'].astype(float,errors = 'ignore')
    df['TD_dip_del'] = df['TD_dip_del'].astype(float,errors = 'ignore')
    df['TD_dip_slo'] = df['TD_dip_slo'].astype(float,errors = 'ignore')
    df['TD_Rpick_dip'] = df['TD_Rpick_dip'].astype(float,errors = 'ignore')

    # create a list of our conditions
    conditions = [
        (df['TD_slo_del'] >= 0.01),
        (df['TD_slo_del'] <= 0)
        ]

    # create a list of the values we want to assign for each condition
    values = ['Delay','Deliver on time']

    # create a new column and use np.select to assign values to it using our lists as arguments
    df['Status'] = np.select(conditions, values)

    df = df[['Order id',
            'Quantity',
            'Customer name',
            'Customer mobile',
            'Customer email',
            'Store name',
            'Currency',
            'Subtotal',
            'Total',
            'Discount',
            'Distance(m)',
            'Delivery charge',
            'Packing charge',
            'Coupon',
            'Payment mode',
            'Service mode',
            'Status',
            'Platform',
            'App version',
            'Delivery slot start',
            'Delivery slot end',
            'Cancel reason',
            'Created at',
            'Accepted at',
            'Cancelled at',
            'Started preapring at',
            'Expected prepared at',
            'Prepared at',
            'Ready for pickup at',
            'Dispatched at',
            'Delivered at',
            'Delivery Agent',
            'Year',
            'Months',
            'Date',
            'TD_slo_del',
            'TD_dip_del',
            'TD_dip_slo',
            'TD_Rpick_dip']]

    return df

#---------------------------------------------------------------------------------------------------------------------

def creating_months(df):
  # create a list of our conditions
    conditions = [
        (df['Months'] == 1),
        (df['Months'] == 2),
        (df['Months'] == 3),
        (df['Months'] == 4),
        (df['Months'] == 5),
        (df['Months'] == 6),
        (df['Months'] == 7),
        (df['Months'] == 8),
        (df['Months'] == 9),
        (df['Months'] == 10),
        (df['Months'] == 11),
        (df['Months'] == 12),
        ]

    # create a list of the values we want to assign for each condition...
    values = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Agu','Sep','Oct','Nov','Dec']

    # create a new column and use np.select to assign values to it using our lists as arguments...
    df['Months'] = np.select(conditions, values)

    return df

#--------------------------------------------------------------------------------------------------------------------------------


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

        insert_script = '''INSERT INTO delivery_report VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        record_number = 1
        for record in df.values.tolist():
            cur.execute(insert_script,record)

            print(record_number,record[:5])
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



#--------------------------------------------------------------------------------------------------------------------------------

def remove_columns():
    stock_files = sorted(glob(Files_location+'\\\\*.csv'))
    for i in stock_files:
        os.remove(i)

#--------------------------------------------------------------------------------------------------------------------------------


def info(df):
    
    df = df

    print("Which date of data recorded")
    for i in list(df['Date'].unique()):
        print(i)
    print()

    outlets_count = 1
    print("Which outlets of data recorded")
    for i in list(df['Store name'].unique()):
        lenght = len(df[df['Store name'] == i] )
        print(f"{outlets_count} : {i} with count : {lenght} ")
        outlets_count = outlets_count + 1

    print()
    print(f"Total number of data recorded  :  {len(df)}.")
    print("Thank You")

#--------------------------------------------------------------------------------------------------------------------------------

if __name__ == "__main__":

    __password = login_detail()
    dfi = import_data()
    if len(dfi) == 0:
        print("Application terminating...")
        time.sleep(7)
    else:
        df = cleaning_data(dfi)
        load_postgreSQL(df)