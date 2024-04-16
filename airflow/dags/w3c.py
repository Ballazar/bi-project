import datetime as dt
import csv
import airflow
import requests
import os
from datetime import datetime
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json
import mysql.connector
import logging
import psycopg2
from airflow.hooks.postgres_hook import PostgresHook
import logging
import csv
import ipaddress
import subprocess
subprocess.call(['pip', 'install', 'user-agents'])
from user_agents import parse

BaseDir="/opt/airflow/data"
RawFiles=BaseDir+"/Raw/"
Staging=BaseDir+"/Staging/"
StarSchema=BaseDir+"/StarSchema/"
# DimIP = open(Staging+'DimIP.txt', 'r')
# DimUnicIP=open(Staging+'DimIPUniq.txt', 'w')
# uniqCommand="sort "+Staging+"DimIP.txt | uniq > "+Staging+'DimIPUniq.txt'
pg_connection_params = {
    "host": "34.171.184.69",
    "port": "5432",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}
uniqCommand="sort -r "+Staging+"DimIP.txt | uniq > "+StarSchema+'DimIPTable.txt'
uniqDateCommand="sort -u "+Staging+"DimDate.txt > "+Staging+'DimDateUniq.txt'

# uniqCommand="sort -u -o "+Staging+"DimIPUniq.txt " +Staging+"DimIP.txt"
# 2>"+Staging+"errors.txt"
try:   
   os.mkdir(BaseDir)
except:
   print("Can't make BaseDir")
try:
   os.mkdir(RawFiles)
except:
   print("Can't make BaseDir") 
try: 
   os.mkdir(Staging)
except:
   print("Can't make BaseDir") 
try:
   os.mkdir(StarSchema)
except:
   print("Can't make BaseDir") 


def CleanHash(filename):
    print('Cleaning ',filename)
    logging.warning('Cleaning '+filename)
    print (uniqCommand)
    type=filename[-3:len(filename)]
    if (type=="log"):
    
        OutputFileShort=open(Staging+'Outputshort.txt', 'a')
        OutputFileLong=open(Staging+'Outputlong.txt', 'a')

        InFile = open(RawFiles+filename,'r')
    
        Lines= InFile.readlines()
        for line in Lines:
            if (line[0]!="#"):
                Split=line.split(" ")
                
                if (len(Split)==14):
                   
                   OutputFileShort.write(line)
#                    print('Short ',filename,len(Split))
                else:
                   if (len(Split)==18):
                       OutputFileLong.write(line)
#                        print('Long ',filename,len(Split))
                   else:
                       print ("Fault "+str(len(Split)))
    
def DeleteFiles():
    OutputFileShort=open(Staging+'Outputshort.txt', 'w')
    OutputFileLong=open(Staging+'Outputlong.txt', 'w')

def ListFiles():
   
   arr=os.listdir(RawFiles)
   
   if not arr:
      print('List arr is empty')

# Output:
# 'List is empty'

   logging.warning('Starting List Files' +",".join(str(element) for element in arr)) 
   DeleteFiles()
   for f in arr:
       logging.warning('calling Clean '+f)
       CleanHash(f)
       
def BuildFactShort():
    InFile = open(Staging + 'Outputshort.txt', 'r')
    OutFact1 = open(Staging + 'OutFact1.txt', 'a')

    Lines = InFile.readlines()
    for line in Lines:
        Split = line.split(" ")
        Date = Split[0]
        Time = Split[1]
        URI_Stem = Split[4]
        Client_IP = Split[8]
        Browser = Split[9].replace(",","")
        Error = Split[10]
        Time_Taken = Split[-1].strip()  # Remove newline character
        
        # Construct the output string with selected columns
        Out = f"{Date},{Time},{URI_Stem},{Client_IP},{Browser},{Error},{Time_Taken}\n"
        OutFact1.write(Out)

def BuildFactLong():
    InFile = open(Staging + 'Outputlong.txt', 'r')
    OutFact1 = open(Staging + 'OutFact1.txt', 'a')

    Lines = InFile.readlines()
    for line in Lines:
        Split = line.split(" ")
        Date = Split[0]
        Time = Split[1]
        URI_Stem = Split[4]
        Client_IP = Split[8]
        Browser = Split[9].replace(",","")
        Error = Split[12]
        Time_Taken = Split[-1].strip()  # Remove newline character
        
        # Construct the output string with selected columns
        Out = f"{Date},{Time},{URI_Stem},{Client_IP},{Browser},{Error},{Time_Taken}\n"
        OutFact1.write(Out)

def Fact1():
    with open(Staging+'OutFact1.txt', 'w') as file:
        file.write("Date,Time,is_bot,ip,Browser,error,responsetime\n")
    BuildFactShort()
    BuildFactLong()

def robots():
    # Dictionary to track is_robot status for each IP address
    ip_status = {}
    
    InFile = open(Staging + 'OutFact1.txt', 'r')
    UpdatedFile = open(Staging + 'UpdatedOutFact1.txt', 'w')

    # Read the first line (header) and write it to the updated file
    header = InFile.readline().strip()
    UpdatedFile.write(header + '\n')

    # Read each line from the input file and update the cs-uri-stem column
    for line in InFile:
        # Split the line into fields
        fields = line.strip().split(',')

        # Extract IP address and browser info
        ip_address = fields[3]
        browser_info = fields[2]

        # Check if the IP address is in the dictionary
        if ip_address in ip_status:
            # If already marked as a bot, update the value accordingly
            is_bot = ip_status[ip_address]
        else:
            # Determine if the browser info indicates a bot
            is_bot = "True" if "robots.txt" in browser_info else "False"
            # Update the dictionary with the status for this IP address
            ip_status[ip_address] = is_bot
        
        # Update the is_robot column
        fields[2] = is_bot

        # Construct the updated line
        updated_line = ','.join(fields) + '\n'
        UpdatedFile.write(updated_line)


def get_browser_and_os(user_agent_string):
    # Parse the User-Agent string
    user_agent = parse(user_agent_string)
    
    # Extract browser name and operating system
    browser_name = user_agent.browser.family
    os_name = user_agent.os.family
    
    return browser_name, os_name

def getbrowser():
    # Initialize a dictionary to store browser names and IDs
    browser_info = {}

    # Open the input file for reading
    with open(Staging + 'UpdatedOutFact1.txt', 'r') as infile:
        for line in infile:
            # Split each line based on the comma character
            parts = line.strip().split(',')
            
            # Get the User-Agent string from the fifth column
            user_agent_string = parts[4].strip()
            
            # Extract browser name and operating system
            browser_name, os_name = get_browser_and_os(user_agent_string)
            
            # If the browser info is not already in the dictionary, assign it a new ID
            if (browser_name, os_name) not in browser_info:
                browser_info[(browser_name, os_name)] = len(browser_info) + 1

    # Write the browser IDs and names to the output file
    with open(Staging + 'Browsers.txt', 'w') as outfile:
        for (browser_name, os_name), browser_id in browser_info.items():
            outfile.write(f"{browser_id},{browser_name},{os_name}\n")


def getIPs():
    # Create a dictionary to store the index for each unique IP address
    ip_index = {}
    index_counter = 1

    # Open the input file containing data
    with open(Staging+'UpdatedOutFact1.txt', 'r') as infile:
        # Read each line from the input file
        for line in infile:
            # Split the line by comma and get the IP address (assuming IP is the fourth field)
            fields = line.strip().split(',')
            if len(fields) >= 4:
                ip_address = fields[3].strip()
                
                # Check if the IP address contains only digits and dots
                if ip_address.replace('.', '').isdigit():
                    # If the IP address is not already in the index dictionary, assign it a new index
                    if ip_address not in ip_index:
                        ip_index[ip_address] = index_counter
                        index_counter += 1

    # Write the IP indices and IP addresses to the output file
    with open(Staging+'DimIP.txt', 'w') as outfile:
        # Write header to the output file
        outfile.write("IP_ID,IP\n")
        
        # Write unique IP addresses with their corresponding indices
        for ip_address, ip_id in ip_index.items():
            outfile.write(f"{ip_id},{ip_address}\n")
        
        # Write duplicate IP addresses with the same index as the first occurrence
        with open(Staging+'UpdatedOutFact1.txt', 'r') as infile:
            for line in infile:
                fields = line.strip().split(',')
                if len(fields) >= 4:
                    ip_address = fields[3].strip()
                    if ip_address in ip_index:
                        outfile.write(f"{ip_index[ip_address]},{ip_address}\n")

            
def makeDimDate():
    InFile = open(Staging+'UpdatedOutFact1.txt', 'r')
    OutputFile=open(Staging+'DimDate.txt', 'w')

    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(",")
        Out=Split[0]+"\n"
        OutputFile.write(Out)
 
Days=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

 
def getDates():
    InDateFile = open(Staging+'DimDateUniq.txt', 'r')   
    OutputDateFile=open(StarSchema+'DimDateTable.txt', 'w')
    with OutputDateFile as file:
       file.write("Date,Year,Month,Day,DayofWeek\n")
    Lines= InDateFile.readlines()
    
    for line in Lines:
        line=line.replace("\n","")
        print(line)
        try:
            date=datetime.strptime(line,"%Y-%m-%d").date()
            weekday=Days[date.weekday()]
            out=str(date)+","+str(date.year)+","+str(date.month)+","+str(date.day)+","+weekday+"\n"
            
            with open(StarSchema+'DimDateTable.txt', 'a') as file:
               file.write(out)
        except:
            print("Error with Date")
            
def GetLocations():
    DimTablename=StarSchema+'DimIPLoc.txt'
    try:
        file_stats = os.stat(DimTablename)
    
        if (file_stats.st_size >2):
           print("Dim IP Table Exists")
           return
    except:
        print("Dim Table IP does not exist, creating one")
    InFile=open(StarSchema+'DimIPTable.txt', 'r')
    OutFile=open(StarSchema+'DimIPLoc.txt', 'w')
    
    
    Lines= InFile.readlines()
    for line in Lines:
        line=line.replace("\n","")
        # URL to send the request to
        request_url = 'https://geolocation-db.com/jsonp/' + line
#         print (request_url)
        # Send request and decode the result
        try:
            response = requests.get(request_url)
            result = response.content.decode()
        except:
            print ("error reponse"+result)
        try:
        # Clean the returned string so it just contains the dictionary data for the IP address
            result = result.split("(")[1].strip(")")
        # Convert this data into a dictionary
            result  = json.loads(result)
            out=line+","+str(result["country_code"])+","+str(result["country_name"])+","+str(result["city"])+","+str(result["latitude"])+","+str(result["longitude"])+"\n"
#            print(out)
            with open(StarSchema+'DimIPLoc.txt', 'a') as file:
               file.write(out)
        except:
            print ("error getting location")

def insert_data_into_db():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    file_path = '/opt/airflow/data/StarSchema/OutFact1.txt'
    table_name = 'fact_table'  # Ensure this is the correct table name
    schema = 'biproject'

    try:
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)  # skip the header
            insert_query = f'INSERT INTO {schema}.{table_name} ("date", "time", "s_ip", "cs_method", "cs_uri_stem", "cs_uri_query", "s_port", "cs_username", "c_ip", "cs_user_agent", "sc_status", "sc_substatus", "sc_win32_status", "sc_bytes", "cs_bytes", "time_taken") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'            
            for row in reader:
                # Check if the row has the expected number of elements
                if len(row) == len(headers):
                    cursor.execute(insert_query, row)
                else:
                    logging.warning(f"Ignoring row: {row}. Number of columns doesn't match the expected number.")
    except Exception as e:
        logging.error(f"Error during data import: {e}")
    finally:
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Data inserted successfully.")

def validate_and_clean_data():
    input_file_path = '/opt/airflow/data/StarSchema/OutFact1.txt'
    clean_file_path = '/opt/airflow/data/StarSchema/CleanOutFact1.txt'
    invalid_data_path = '/opt/airflow/data/StarSchema/InvalidOutFact1.txt'

    with open(input_file_path, 'r') as infile, \
         open(clean_file_path, 'w') as outfile, \
         open(invalid_data_path, 'w') as errfile:

        headers = infile.readline()
        outfile.write(headers)
        errfile.write(headers)

        for line in infile:
            fields = line.strip().split(',')
            if validate_row(fields):
                outfile.write(line)
            else:
                errfile.write(line)

def validate_row(fields):
    expected_num_fields = 18
    if len(fields) != expected_num_fields:
        return False
    try:
        datetime.strptime(fields[0], "%Y-%m-%d")
        datetime.strptime(fields[1], "%H:%M:%S")
        # Further checks can be added here
    except ValueError:
        return False

    return True

def insert_ip_data():
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()  

    file_path = '/opt/airflow/data/StarSchema/DimIPTable.txt'
    iptable_name = 'ip_data'  # Ensure this is the correct table name
    schema = 'biproject'

    try:
        with open(file_path, 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)  # skip the header
            insert_query = f'INSERT INTO {schema}.{iptable_name} ("ip_id", "ip") VALUES (%s, %s)'
            cursor.executemany(insert_query, reader)
    except Exception as e:
        logging.error(f"Error during data import: {e}")
    finally:
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Data inserted successfully.")

dag = DAG(                                                     
   dag_id="Process_W3_Data",                          
   schedule_interval="@daily",                                     
   start_date=dt.datetime(2023, 2, 24), 
   catchup=False,
)
download_data = PythonOperator(
   task_id="RemoveHash",
   python_callable=ListFiles, 
   dag=dag,
)

DimIp = PythonOperator(
    task_id="DimIP",
    python_callable=getIPs,
    dag=dag,
)
DimRobot = PythonOperator(
        task_id="DimRobot",
        python_callable=robots,
        dag=dag,
)

DateTable = PythonOperator(
    task_id="DateTable",
    python_callable=makeDimDate,
    dag=dag,
)

IPTable = PythonOperator(
    task_id="IPTable",
    python_callable=GetLocations,
    dag=dag,
)

BuildFact1 = PythonOperator(
   task_id="BuildFact1",
   python_callable= Fact1,
   dag=dag,
)

buildbrowser = PythonOperator(
   task_id="Buildbrowser",
   python_callable=getbrowser,
   dag=dag,
)

BuildDimDate = PythonOperator(
   task_id="BuildDimDate",
   python_callable=getDates, 
   dag=dag,
)

uniq = BashOperator(
    task_id="uniqIP",
    bash_command=uniqCommand,
#     bash_command="echo 'hello' > /home/airflow/gcs/Staging/hello.txt",

    dag=dag,
)

uniq2 = BashOperator(
    task_id="uniqDate",
    bash_command=uniqDateCommand,
#     bash_command="echo 'hello' > /home/airflow/gcs/Staging/hello.txt",

    dag=dag,
)

copyfact = BashOperator(
    task_id="copyfact",
#    bash_command=uniqDateCommand,
     bash_command="cp /opt/airflow/data/Staging/OutFact1.txt /opt/airflow/data/StarSchema/OutFact1.txt",

    dag=dag,
)

validate_clean_data_task = PythonOperator(
    task_id='validate_and_clean_data',
    python_callable=validate_and_clean_data,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data_into_db',
    python_callable=insert_data_into_db,
    dag=dag,
)

insert_ip_data_task = PythonOperator(
    task_id='insert_ip_into_db',
    python_callable=insert_ip_data,
    dag=dag,
)
 

# download_data >> BuildFact1 >>DimIp>>DateTable>>uniq>>uniq2>>BuildDimDate>>IPTable

BuildFact1.set_upstream(task_or_task_list=[download_data])
DimRobot.set_upstream(task_or_task_list=[BuildFact1])
DimIp.set_upstream(task_or_task_list=[DimRobot])
buildbrowser.set_upstream(task_or_task_list=[DimRobot])
DateTable.set_upstream(task_or_task_list=[DimRobot])
uniq2.set_upstream(task_or_task_list=[DateTable])
uniq.set_upstream(task_or_task_list=[DimIp])
BuildDimDate.set_upstream(task_or_task_list=[uniq2])
IPTable.set_upstream(task_or_task_list=[uniq])
copyfact.set_upstream(task_or_task_list=[IPTable,BuildDimDate])
validate_clean_data_task.set_upstream(task_or_task_list=[copyfact])
insert_data_task.set_upstream(task_or_task_list=[validate_clean_data_task])
insert_ip_data_task.set_upstream(task_or_task_list=[DimIp])
