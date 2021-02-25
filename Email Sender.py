#Import
import os
from datetime import datetime, date, timedelta
import time
import pandas as pd
import snowflake.connector
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
import smtplib, ssl
import sys

##create injecting environment variables (will be done by Jenkins)
#Snowflake environment variables
os.environ['SNOWFLAKE_ACCOUNT'] = 'xxxxxx'
os.environ['SNOWFLAKE_USER'] = 'xxxxxx'
os.environ['SNOWFLAKE_PASSWORD']='xxxxxx'
os.environ['SNOWFLAKE_DATABASE'] = 'xxxxxx'
os.environ['SNOWFLAKE_DATAWAREHOUSE'] = 'xxxxxx'

#Email environment variables
os.environ['EMAIL_SMTP_SERVER'] = 'xxxxxx'
os.environ['EMAIL_SMTP_PORT'] = 'xxxxxx'
os.environ['EMAIL_SENDER'] = 'xxxxxx'
os.environ['EMAIL_PASSWORD'] = 'xxxxxx'

#function to get email name
def get_name(email):
    return email.split('@')[0]

#function to create a massage in an HTML format
def get_message(email, email_d):
    #set email connection
    msg = MIMEMultipart()
    msg['Subject'] = "Daily Notification on customers not paying 2nd payment"
    name = get_name(email)
    text = """\
    Dear {},
    Please Contact the following Customers as they did not pay their second payment yet:
    """.format(name)
    html = """\
    <html>
      <head></head>
        <body>
          {0}
        </body>
      </html>
    """.format(email_d[email].to_html())

    # Add HTML/plain-text parts to MIMEMultipart message
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # The email client will try to render the last part first
    msg.attach(part1)
    msg.attach(part2)
    return msg

#function to send emails
def send_mail(context, receiver, msg):
    smtp_server = os.environ['EMAIL_SMTP_SERVER']
    port = os.environ['EMAIL_SMTP_PORT'] # For starttls
    sender_email = os.environ['EMAIL_SENDER']
    password = os.environ['EMAIL_PASSWORD']
    name = get_name(receiver)
    #send the email
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver, msg.as_string())
        status_message = 'Email was send successfully to: {}'.format(name)
        print(status_message)
        add_line(OUTPUT_FILE, name)


#Function to add emails to daily log
def add_line(filename, text):
    with open(filename, 'a') as out:
        out.write(text + '\n')

#function to check if the environment variables are valid
def is_valid (var_name):
    if var_name not in os.environ:
        print('the varialble {} is not exists'.format(var_name))
        return False
    if os.environ[var_name] == "":
        print('error the varialble {} is empty'.format(var_name))
        return False
    return True


#Create SQL Query for flag - if true continue, if false stop
def flag_query():
    return '''
           SELECT MAX(DATE(RELEVANCE_DATE)) AS MAX_DATAWAREHOUSE_DATE
           FROM xxxxxx
           '''

def main_query(target):
    if target == 'manager_1':
        dynamic_text = "Column name to format"
    elif target == 'manager_2':
        dynamic_text = "Column name to format"
    elif target == 'manager_3':
        dynamic_text = "Column name to format"
    else:
        print("unsupported target")
        return

    return '''
        /*BY SELLER EMAIL*/
        WITH CTE
        AS
        (
        SELECT A.CONTRACT_SHORT_NUM,
               E.PART_SUB_FAMILY_NAME,
               D.CONTACT_FULL_NAME,
               D.CONTACT_MAIN_PHONE,
               D.SECONDARY_PHONE AS CONTACT_SECONDARY_PHONE,
               D.CONTACT_STREET,
               D.CONTACT_CITY,
               D.CONTACT_STATE,
               A.CONTRACT_KEY,
               DATE(A.CONTRACT_CREATION_DATE) AS CONTRACT_CREATION_DATE,
               A.RELEVANCE_DATE,
               DATE(A.LAST_PAYMENT_DATE) AS LAST_PAYMENT_DATE,
               DENSE_RANK () OVER (PARTITION BY A.CONTRACT_KEY ORDER BY A.LAST_PAYMENT_DATE) AS CONSECUTIVE_PAYMENTS,
               A.CONTRACT_ACTIVITY_STATUS,
               A.CONSECUTIVE_NO_ACTIVITY_DAYS,
               A.DAILY_PAID_DAYS,
               A.DAILY_IN_LIGHT_DAYS,
               B.CONTRACT_SALES_SELLER,
               B.SALES_CHANNEL_TYPE,
               C.FULL_NAME,
               C.EMAIL,
               C.TSM_TSO_NAME,
               C.TSM_TSO_EMAIL,
               C.RM_NAME,
               C.RM_EMAIL,
               C.MANAGER_NAME,
               C.MANAGER_EMAIL
        FROM xxxxxx A
        LEFT xxxxxx B
        ON A.CONTRACT_KEY = B.CONTRACT_KEY
        LEFT xxxxxx C
        ON B.CONTRACT_CREATED_BY_USER_KEY = C.USER_KEY
        LEFT xxxxxx D
        ON B.MAIN_PAYER_CONTACT_KEY = D.CONTACT_KEY
        LEFT xxxxxx E
        ON A.SPS_SHORT_NUM = E.SERIAL_NUMBER
        WHERE A.CONTRACT_ACTIVITY_STATUS IN ('xxxxxx', 'xxxxxx')
        AND B.CONTRACT_MODEL_TYPE = 'xxxxxx xxxxxx xxxxxx'
        AND DATE(A.CONTRACT_CREATION_DATE) >= xxxxxx
        AND A.LAST_PAYMENT_DATE IS NOT NULL
        AND B.CONTRACT_GENERATION = 'xxxxxx'
        AND A.PARTNER_KEY = 'xxxxxx'
        ORDER BY RELEVANCE_DATE
        ),CTE2
        AS
        (
        SELECT *,
               CASE WHEN CONSECUTIVE_PAYMENTS = 1 THEN MAX(DAILY_PAID_DAYS) OVER (PARTITION BY CONTRACT_KEY ORDER BY CONTRACT_KEY)
                    END AS FIRST_PAYMENT_PAID_DAYS
        FROM CTE
        ),FINAL_RESULT
        AS
        (
        SELECT *
        FROM CTE2
        WHERE DAILY_IN_LIGHT_DAYS = 0
        AND CONSECUTIVE_NO_ACTIVITY_DAYS >= 1
        AND CONSECUTIVE_NO_ACTIVITY_DAYS < 11
        AND CONSECUTIVE_PAYMENTS = 1
        AND FIRST_PAYMENT_PAID_DAYS < 70
        AND RELEVANCE_DATE = CURRENT_DATE()-1
        )
        SELECT CONTRACT_SHORT_NUM AS "Contract Short Number",
               CONTACT_FULL_NAME AS "Contact Full Name",
               CONTACT_MAIN_PHONE AS "Contact Main Phone",
               CONTACT_SECONDARY_PHONE AS "Contact Secondary Phone",
               CONTACT_STATE AS "Contact Sate",
               CONTACT_CITY AS "Contact City",
               CONTACT_STREET AS "Contact Street",
               RELEVANCE_DATE AS "Yesterday Date",
               LAST_PAYMENT_DATE AS "Last Payment Date",
               CONSECUTIVE_NO_ACTIVITY_DAYS AS "Current Dark Days",
               PART_SUB_FAMILY_NAME AS "Eco/Prime Unit",
               SALES_CHANNEL_TYPE AS "Sales Channel",
               CONTRACT_SALES_SELLER AS "Contract Sales Seller",
               {}

        FROM FINAL_RESULT
        ORDER BY CONTRACT_SALES_SELLER,CONSECUTIVE_NO_ACTIVITY_DAYS
        '''.format(dynamic_text)


#Logic def Function
def mail_sender():
    #Create Snowflake engine
    engine = create_engine(URL(
        account = os.environ['SNOWFLAKE_ACCOUNT'],
        user = os.environ['SNOWFLAKE_USER'],
        password = os.environ['SNOWFLAKE_PASSWORD'],
        database = os.environ['SNOWFLAKE_DATABASE'],
        warehouse = os.environ['SNOWFLAKE_DATAWAREHOUSE']
    ))

    #Create Connection
    connection = engine.connect()

    #Create Flag Data Frame
    df_date_flag = pd.read_sql_query(flag_query(), engine)

    #Create yesterday date for runninng script flag
    today = date.today()
    yesterday = today - timedelta(days = 1)

    #data relevant flag
    data_relevant = df_date_flag["max_datawarehouse_date"][0] == yesterday

    #Loop and send the email if the max date in the data warehouse is true based on yesterday's date
    if data_relevant:

        def send_to_group():
            #create for each eamil his own data frame
            #select all unique emails
            unique_eamils = df_daily_status.email.unique()

            #create a dataframe dictionary to store data frames
            email_d = {email : pd.DataFrame() for email in unique_eamils}

            #create for each email it own data frame
            for key in email_d.keys():
                email_d[key] = df_daily_status.loc[:, df_daily_status.columns != 'email' ][df_daily_status.email == key]
                email_d[key].reset_index(drop=True ,inplace = True)

            #Send eamil to each email in the dataframe with his relevant data
            for email in email_d:
                # create html formatted message
                message = get_message(email, email_d)
                # Create a secure SSL context
                context = ssl.create_default_context()
                # send via gmail
                try:
                    send_mail(context, email, message)
                except:
                    print("error sending email to {}.".format(get_name(email)))

        #CREATE DATA FRAME from sellers
        df_daily_status = pd.read_sql_query(main_query('manager_1'),engine)
        send_to_group()

        df_daily_status = pd.read_sql_query(main_query('manager_2'),engine)
        send_to_group()

        df_daily_status = pd.read_sql_query(main_query('manager_3'),engine)
        send_to_group()

        print('Mail sender Done.')
        return True
    else:
        print('The Data is not available yet.')
        return False


def main():
    for var in my_vars:
        if not is_valid(var):
            return # at least 1 environment var is missing
    retry = 1
    while True:
        # mail_sender returns True if emails were sent, and False if data is not relevant
        success = mail_sender()
        if success:
            print("script executed successfuly.")
            return
        retry += 1
        if retry <= MAX_RETRIES:
            print("sleeping for {} seconds before retry {}/{}...".fotmat(DELAY_SECONDS, retry, MAX_RETRIES))
            time.sleep(DELAY_SECONDS)
        else:
            print("data is not relevant after {} retries, aborting.".format(MAX_RETRIES))
            return


#############################################################
# program starts here

# Environment variables list
my_vars = ['SNOWFLAKE_ACCOUNT',
           'SNOWFLAKE_USER',
           'SNOWFLAKE_PASSWORD',
           'SNOWFLAKE_DATABASE',
           'SNOWFLAKE_DATAWAREHOUSE',
           'EMAIL_SMTP_SERVER',
           'EMAIL_SMTP_PORT',
           'EMAIL_SENDER',
           'EMAIL_PASSWORD']
# constants
MAX_RETRIES=15
DELAY_SECONDS=600
OUTPUT_FILE='./output.log'

main()
