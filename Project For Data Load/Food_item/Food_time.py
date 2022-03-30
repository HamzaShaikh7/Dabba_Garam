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

    Month_value = str(input("Enter a months : "))
    Date_value = str(input("Enter a Date : "))
    global value
    conn = None
    cur = None

    final_date = f"{2022}-{Month_value}-{Date_value}"

    try:
        conn = psycopg2.connect(
            host=__host,
            dbname=__dbname,
            user=__user,
            password = __password,
            port= __port
            )
        cur = conn.cursor()

        select_outlet = """SELECT 
                        CASE 
                            WHEN delivery_report.payment_mode ISNULL
                                THEN concat((to_char(order_summary.date,'ddMMyy' )),' Food item - ',order_summary.sub_order_type,'_','online')
                            ELSE
                                concat((to_char(order_summary.date,'ddMMyy' )),' Food item - ',order_summary.sub_order_type,'_',delivery_report.payment_mode)
                            END AS "Composite Item Name",
                        '' AS "Sales Description",
                        '' AS "Selling Price",
                        'Sales' AS "Sales Account",
                        'GST' AS "Tax Name",
                        5 AS "Tax Percentage",
                        '' AS "Tax Type",
                        'goods' AS "Product types",
                        1 AS "Source",
                        '' AS "Reference ID",
                        '' AS "Last Sync Time",
                        'Active' AS "Status",
                        'pcs' AS "unit",
                        '' AS "SKU",
                        '' AS "UPC",
                        '' AS "EAN",
                        '' AS "ISBN",
                        '' AS "Part Number",
                        '' AS "purchase price",
                        'Materials' AS "purchase account",
                        '' AS "Purchase Description",
                        'Finished Goods' AS "Inventory Account",
                        '' AS "Reorder Point",
                        '' AS "Preferred Vendor",
                        '' AS "Opening Stock",
                        '' AS "Opening Stock Value",
                        order_wise_cunsumption.rawmaterial AS "mapped items",
                        SUM(order_wise_cunsumption.rm_qty) AS "mapped quantity",
                        TRIM(order_wise_cunsumption.outlet) AS "Outlet"
                    FROM
                        order_summary
                    JOIN
                        order_wise_cunsumption
                        ON
                        order_summary.order_no = order_wise_cunsumption.order_no
                    LEFT JOIN
                        delivery_report
                        ON
                        order_summary.client_orderid  = delivery_report.order_id
                    WHERE
                        order_summary.date = %s
                    GROUP BY
                        order_wise_cunsumption.outlet,
                        delivery_report.payment_mode,
                        order_summary.date,
                        order_wise_cunsumption.rawmaterial,
                        order_summary.sub_order_type
                    ORDER BY
                        order_wise_cunsumption.rawmaterial"""

        cur.execute(select_outlet,[final_date])
        df=cur.fetchall()
        
        df = pd.DataFrame(df)
        df.columns = [['Composite Item Name','Sales Description','Selling Price','Sales Account','Tax Name','Tax Percentage','Tax Type','Product types','Source','Reference ID','Last Sync Time','Status','unit','SKU','UPC','EAN','ISBN','Part Number','purchase price','purchase account','Purchase Description','Inventory Account','Reorder Point','Preferred Vendor','Opening Stock','Opening Stock Value','mapped items','mapped quantity','Outlet']]


        df.to_csv(f"C:\\Users\\a\\Downloads\\Food-item.csv" , index = False)

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