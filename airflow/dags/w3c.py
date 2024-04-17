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
from user_agents import parse

BaseDir="/opt/airflow/data"
RawFiles=BaseDir+"/Raw/"
Staging=BaseDir+"/Staging/"
StarSchema=BaseDir+"/StarSchema/"
# DimIP = open(Staging+'DimIP.txt', 'r')
# DimUnicIP=open(Staging+'DimIPUniq.txt', 'w')
# uniqCommand="sort "+Staging+"DimIP.txt | uniq > "+Staging+'DimIPUniq.txt'

uniqCommand="sort -u "+Staging+"DimIP.txt > "+Staging+'DimIPUniq.txt'
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
    InFile = open(Staging+'Outputshort.txt','r')
    OutFact1=open(Staging+'OutFact1.txt', 'a')

    Lines = InFile.readlines()
    for line in Lines:
        Split = line.split(" ")
        Date = Split[0]
        Time = Split[1]
        Client_IP = Split[8]
        Browser = Split[9].replace(",","")
        Error = Split[10]
        Time_Taken = Split[-1].strip()  # Remove newline character
        
        # Construct the output string with selected columns
        Out = f"{Date},{Time},{Client_IP},{Browser},{Error},{Time_Taken}\n"
        OutFact1.write(Out)
def BuildFactLong():
    InFile = open(Staging+'Outputlong.txt','r')
    OutFact1=open(Staging+'OutFact1.txt', 'a')

    Lines = InFile.readlines()
    for line in Lines:
        Split = line.split(" ")
        Date = Split[0]
        Time = Split[1]
        Client_IP = Split[8]
        Browser = Split[9].replace(",","")
        Error = Split[12]
        Time_Taken = Split[-1].strip()  # Remove newline character
        
        # Construct the output string with selected columns
        Out = f"{Date},{Time},{Client_IP},{Browser},{Error},{Time_Taken}\n"
        OutFact1.write(Out)

def Fact1():
    with open(Staging+'OutFact1.txt', 'w') as file:
        file.write("Date,Time,ip,Browser,error,responsetime\n")
    BuildFactShort()
    BuildFactLong()

def extract_browser():
    input_file = 'OutFact1.txt'
    output_file = 'FinalOutFact1.txt'

    def extract(user_agent_string):
        user_agent = parse(user_agent_string)
        browser = user_agent.browser.family
        os = user_agent.os.family
        is_crawler = user_agent.is_bot
        return browser, os, is_crawler

    with open(Staging + input_file, 'r') as infile, open(Staging + output_file, 'w') as outfile:
        header = infile.readline().strip().split(',')  # Read the header from input file
        header.pop(3)
        header.extend(['Browser', 'Operating System', 'Is Crawler'])  # Add new column names
        outfile.write(','.join(header) + '\n')  # Write header to output file
        for line in infile:
            columns = line.strip().split(',')  # Assuming columns are separated by commas

            user_agent_string = columns.pop(3) if len(columns) > 4 else ''  # Remove the 5th column and get its value
            browser, os, is_crawler = extract(user_agent_string)

            # Rename the columns and append browser, OS, and crawler information
            columns.extend([browser, os, str(is_crawler)])

            new_line = ','.join(columns) + '\n'
            outfile.write(new_line)

def getIPs():
    InFile = open(Staging+'OutFact1.txt', 'r')
    OutputFile=open(Staging+'DimIP.txt', 'w')
    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(",")
        Out=Split[2]+"\n"
        OutputFile.write(Out)
def makeDimDate():
    InFile = open(Staging+'OutFact1.txt', 'r')
    OutputFile=open(Staging+'DimDate.txt', 'w')

    Lines= InFile.readlines()
    for line in Lines:
        Split=line.split(",")
        Out=Split[0]+"\n"
        OutputFile.write(Out)
 
Days=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

 
def getDates():
    with open(Staging + 'DimDateUniq.txt', 'r') as InDateFile, \
         open(StarSchema + 'DimDateTable.txt', 'w') as OutputDateFile:
        next(InDateFile)  # Skip the first line (header)
        OutputDateFile.write("Date,Year,Month,Day,DayofWeek\n")

        for line in InDateFile:
            line = line.strip()  # Remove newline character
            if line != "Date":  # Skip lines that contain "Date"
                try:
                    date = datetime.strptime(line, "%Y-%m-%d").date()
                    weekday = Days[date.weekday()]
                    out = f"{date},{date.year},{date.month},{date.day},{weekday}\n"
                    OutputDateFile.write(out)
                except ValueError:
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
    InFile=open(Staging+'DimIPUniq.txt', 'r')
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
           print("error response: " + str(result))
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

def generate_id_tables():
    input_file = 'FinalOutFact1.txt'
    
    # Read input file and extract unique values for each column
    error_types = set()
    browsers = set()
    operating_systems = set()
    
    with open(Staging + input_file, 'r') as infile:
        next(infile)  # Skip header
        for line in infile:
            columns = line.strip().split(',')
            if len(columns) >= 6:  # Ensure that the line has at least 6 columns
                error_types.add(columns[3])  # Fourth column is error type
                browsers.add(columns[5])     # Fifth column is browser
                operating_systems.add(columns[6])  # Sixth column is operating system    with open(StarChema + output_file, 'w') as outfile:
        # Write unique values to separate id tables
    write_id_table('Errorid.txt', error_types)
    write_id_table('Browserid.txt', browsers)
    write_id_table('Osid.txt', operating_systems)
    

def write_id_table(output_file, values):
    with open(StarSchema + output_file, 'w') as outfile:
        for idx, value in enumerate(values, start=1):
            outfile.write(f"{idx},{value}\n")

def map_id_values():
    input_file = 'FinalOutFact1.txt'
    error_id_file = 'Errorid.txt'
    browser_id_file = 'Browserid.txt'
    os_id_file = 'Osid.txt'
    output_file = 'MappedFinalOutFact1.txt'

    # Read the ID tables into dictionaries
    error_id_dict = read_id_table(error_id_file)
    browser_id_dict = read_id_table(browser_id_file)
    os_id_dict = read_id_table(os_id_file)

    # Map ID values onto the original values in the input table and write to output file
    with open(Staging + input_file, 'r') as infile, open(StarSchema + output_file, 'w') as outfile:
        header = next(infile)  # Read header
        outfile.write(header)  # Write header to output file
        for line in infile:
            columns = line.strip().split(',')
            if len(columns) >= 6:
                error_type_id = error_id_dict.get(columns[3], '')
                browser_id = browser_id_dict.get(columns[5], '')
                os_id = os_id_dict.get(columns[6], '')
                mapped_line = ','.join([columns[0], columns[1], columns[2], error_type_id,  browser_id, os_id, columns[7]])
                outfile.write(mapped_line + '\n')

def read_id_table(input_file):
    id_dict = {}
    with open(StarSchema + input_file, 'r') as infile:
        for line in infile:
            idx, value = line.strip().split(',')
            id_dict[value] = idx
    return id_dict


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

DateTable = PythonOperator(
    task_id="DateTable",
    python_callable=makeDimDate,
    dag=dag,
)


BuildFact1 = PythonOperator(
   task_id="BuildFact1",
   python_callable= Fact1,
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

IPTable = PythonOperator(
    task_id="IPTable",
    python_callable=GetLocations,
    dag=dag,
)

browser = PythonOperator(
        task_id="browser",
        python_callable=extract_browser,
        dag=dag,
)
  
idtables = PythonOperator(
        task_id="idtables",
        python_callable=generate_id_tables,
        dag=dag,
)

# Define the task to execute the map_id_values function
map_id_values_task = PythonOperator(
    task_id='map_id_values_task',
    python_callable=map_id_values,
    dag=dag,
)
# download_data >> BuildFact1 >>DimIp>>DateTable>>uniq>>uniq2>>BuildDimDate>>IPTable

BuildFact1.set_upstream(task_or_task_list=[download_data])
DimIp.set_upstream(task_or_task_list=[BuildFact1])
DateTable.set_upstream(task_or_task_list=[BuildFact1])
browser.set_upstream(task_or_task_list=[BuildFact1])
uniq2.set_upstream(task_or_task_list=[DateTable])
uniq.set_upstream(task_or_task_list=[DimIp])
BuildDimDate.set_upstream(task_or_task_list=[uniq2])
IPTable.set_upstream(task_or_task_list=[uniq])
idtables.set_upstream(task_or_task_list=[browser])
map_id_values_task.set_upstream(task_or_task_list=[idtables])
copyfact.set_upstream(task_or_task_list=[IPTable,BuildDimDate])
