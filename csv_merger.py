#/**************************************************************************
#* Copyright 2016 Observational Health Data Sciences and Informatics (OHDSI)
#*
#* Licensed under the Apache License, Version 2.0 (the "License");
#* you may not use this file except in compliance with the License.
#* You may obtain a copy of the License at
#*
#* http://www.apache.org/licenses/LICENSE-2.0
#*
#* Unless required by applicable law or agreed to in writing, software
#* distributed under the License is distributed on an "AS IS" BASIS,
#* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#* See the License for the specific language governing permissions and
#* limitations under the License.
#* 
#* Authors: Milton Aleman
#* Date: 2023
#**************************************************************************/

import sys
import csv
import glob
import os.path
import logging
import datetime 

files = []
header = []
delimiters = []
csv_file_extension = ['csv','txt','cab']

def logging_init():
   now = datetime.datetime.today()
   file_name = str(now.year)+'-'+int_str(now.month)+'-'+int_str(now.day)+'_'+int_str(now.hour)+int_str(now.minute)
   logging.basicConfig(filename='./logs/'+file_name+'.log', encoding='utf-8', level=logging.DEBUG)

def int_str(number):
   if(number < 9):
      return '0'+str(number)
   else:
      return str(number)   

def exeception_handler(msg):
    logging.error(msg)
    print(msg)
    exit(0)

def args_parameters(args):
   print(args)
   print(len(args) == 1 and os.path.exists(args[0]) and os.path.isdir(args[0]))
   if(len(args) == 1 and os.path.exists(args[0]) and os.path.isdir(args[0])):
      logging.info("Directory detected ")
      logging.info('Scanning directory :'+args[0])
      tmp_f = []
      for extension in csv_file_extension:
         tmp_f = glob.glob(args[0]+'/*.'+extension)
         for file in tmp_f:
            files.append(file)
            logging.info('File added :'+file)

   else:
      for x in args:
        validate_directory(x)
        files.append(x)
        logging.info('File added :'+x)

def validate_directory(path):
  check_file_exists = os.path.exists(path)
  check_file = os.path.isfile(path)
  if not(check_file_exists):
    exeception_handler('Error>>'+path+' does not exists')
  elif not(check_file):
    exeception_handler('Error>>'+path+' is not a file')

def get_uncommon_delimiter(line):
    uncommon_delimiters= ['\t',' ','  ',':']
    for delimiter in uncommon_delimiters:
        ref = line.count(delimiter)
        if ref > 0:
            if all([ ref == line.count(delimiter) for i in range(1,5)]):
                return delimiter
    return ''

def create_csv(headers,rows, output_field_name):
   with open(output_field_name, 'w', encoding='UTF8', newline='') as output_file:
      # using csv.writer method from CSV package
      write = csv.writer(output_file, quoting=csv.QUOTE_NONE, escapechar='\\')
      write.writerow(headers)
      write.writerows(rows)

      logging.info('A file has been created : '+output_field_name)
      print('A file has been created : '+output_field_name)

def get_header(csv_data):
   header_final = []
   #build Header
   for x in range(0,len(csv_data)):
    head_csv = list(map(lambda x: x.lower(), csv_data[x][0]))
    if(x != 0):    
       for y in range(0, len(head_csv)):
          if head_csv[y] not in header_final:
             header_final.append(head_csv[y])
    else:
       header_final.extend(head_csv)

   return header_final

def get_body(csv_data, header):
    body_final_rows = []
    #for each csv file get body
    for w in range(0,len(csv_data)):
       csv_content = csv_data[w]  
       #For each file in csv whe need to fill 
       csv_head = list(map(lambda x: x.lower(), csv_content[0]))
       rows = []
       for y in range(1, len(csv_content)):
          row = []
          #For each value in header fill if exists
          for z in range(0, len(header)):
             value = ''
             try:
                ixd = csv_head.index(header[z])
                if(ixd < 0 or ixd > len(csv_content[y])-1):
                   value = 'null'
                else:
                   value = csv_content[y][ixd]
             except ValueError:
              value = 'null'
             row.append(value)   
          body_final_rows.append(row)
   
    return body_final_rows   
       
def get_csv_data(files):
    csv_data = []
    for x in range(0,len(files)):
       rows = []
       logging.info('Openning file : '+files[x])
       print('Openning file : '+files[x])
       validate_directory(files[x]) 
       with open(files[x], newline='') as csvfile:
          csv_sample = csvfile.read(1024)
          try:          
             dialect = csv.Sniffer().sniff(csv_sample)
             csvfile.seek(0)
             reader = csv.reader(csvfile, dialect)
             logging.info('Delimiter '+dialect.delimiter+' detected for file :'+files[x])
          except:
             head = csv_sample.splitlines()
             delimiter = get_uncommon_delimiter(head[0])
             csvfile.seek(0)
             reader = csv.reader(csvfile, delimiter=delimiter)
             logging.info('Delimiter '+delimiter+' detected for file :'+files[x])
          
          for row in reader:
           rows.append(row)
      
       csv_data.append(rows)
    
    return csv_data

         
def main(args):
   logging_init()
   log_hello_str = 'Proccess starting at :'+datetime.datetime.today().strftime("%m-%d-%Y %H:%M:%S")
   print(log_hello_str)
   logging.info(log_hello_str)
   #files = ['C:/Users/STEVEN/Desktop/csv_test/allgy_nm_US_EMR_Allergy_2022SEP.txt']
   args_parameters(args)
   if(len(files) > 0):
      csv_data = get_csv_data(files)   
      header = get_header(csv_data)  
      body = get_body(csv_data,header)
      create_csv(header,body, './output/output.txt')

      log_bye_str = 'The process has finished successfully at :'+datetime.datetime.today().strftime("%m-%d-%Y %H:%M:%S")
      print(log_bye_str)
      logging.info(log_bye_str)
   else:
      print('Not file foud')
      logging.info('Not file foud')


argsx =  sys.argv[1:len(sys.argv)]
main(argsx)