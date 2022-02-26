import psycopg2
import time
import getpass


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

        select_date = '''SELECT DISTINCT(date) FROM order_summary ORDER BY date desc LIMIT 1;'''

        cur.execute(select_date)
        Result=cur.fetchall()

        print(f'Last Updated date ::== {str(Result)[25:-4]}')

        select_outlet = '''SELECT DISTINCT(outlet) FROM order_summary WHERE date = %s ORDER BY outlet'''

        cur.execute(select_outlet, Result)
        Result=cur.fetchall()
        print()
        print()
        counting = 1
        print("Order Recorded by outlets...")
        for x in Result:
            temp_outlet = str(x)[2:]
            temp_outlet = temp_outlet[:-3]
            print(f'{counting} {temp_outlet}')
            counting = counting + 1

        conn.commit()

    except NameError as error:
        print("Error : Entered month not present in DataBase, please enter a correct month.")
    except psycopg2.OperationalError as error_pass:
        print('\nError : Password incorrect.')


    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()



if __name__ == '__main__':
    __password = login_detail()
    postgresql_check()
    time.sleep(20)
