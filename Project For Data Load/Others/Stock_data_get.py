import psycopg2
import time
import getpass
import pandas as pd


# declaring variable...
__host = "database1.csioilzjaaf3.ap-south-1.rds.amazonaws.com"
__dbname = "database1"
__user = "admin"
__port = 5432
__password = str()



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



def postgresql_check():

    global value
    start_month = str(input("Start Month : "))
    start_date = str(input("Start date : "))
    Start_time = f"2022-{start_date}-{start_month} 00:00"
    End_month = str(input("End Month : "))
    End_date = str(input("End date : "))
    End_time = f"2022-{End_date}-{End_month} 23:59"
    conn = None
    cur = None

    try:
        conn = psycopg2.connect(
            host=__host,
            dbname=__dbname,
            user=__user,
            password = __password,
            port= __port
            )
        cur = conn.cursor()


        select_outlet = '''SELECT 
                                stock_data.rawmaterial AS "Raw Materail",
                                sum(stock_data.existing_stock) AS "Existing Stock",
                                stock_data.outlet AS "Outlet",
                                rm_category.category
                            FROM 
                                stock_data
                            JOIN 
                                rm_category
                                ON
                                rm_category.name = stock_data.rawmaterial
                            WHERE
                                stock_data.time >= %s AND stock_data.time <= %s
                            group by
                                stock_data.rawmaterial,
                                stock_data.outlet,
                                rm_category.category
                            ORDER BY
                                stock_data.outlet,stock_data.rawmaterial;'''


        cur.execute(select_outlet,[Start_time , End_time])
        Result=cur.fetchall()
        df = pd.DataFrame(Result)

        df.columns = ['Raw Materail', "Existing Stock", "Outlet", "Category"]

        df.to_csv(f"C:\\Users\\a\\Downloads\\stock_data_2022_{start_month}_{start_date}.csv" , index = False)

        conn.commit()

    except NameError as error:
        print("Error : Entered month not present in DataBase, please enter a correct month.")
        time.sleep(10)
    except psycopg2.OperationalError as error_pass:
        print('\nError : Password incorrect.')
        time.sleep(10)


    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()



if __name__ == '__main__':
    __password = login_detail()
    postgresql_check()
    time.sleep(20)
