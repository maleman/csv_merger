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
import os.path

files = []
header = []
delimiters = []

def exeception_handler(msg):
    raise Exception(msg)

def validate_directory(path):
  check_file_exists = os.path.exists(path)
  check_file = os.path.isfile(path)
  if not(check_file_exists):
    exeception_handler('Error>>'+path+' does not exists')
  elif not(check_file):
    exeception_handler('Error>>'+path+' is not a file')

def get_delimiter(line):
    common_delimiters= [',',';','\t',' ','|',':']
    for d in common_delimiters:
        ref = line.count(d)
        if ref > 0:
            if all([ ref == line[i].count(d) for i in range(1,1)]):
                return d
    return ''

def create_csv(headers,rows, output_field_name):
   with open(output_field_name, 'w', encoding='UTF8', newline='') as output_file:
      # using csv.writer method from CSV package
      write = csv.writer(output_file, quoting=csv.QUOTE_NONE, escapechar='\\')
      write.writerow(headers)
      write.writerows(rows)

def get_header(csv_data):
   #delimiter 
   header_final = []
   #build Header
   for x in range(0,len(csv_data)):
    lines = csv_data[x][0]
    delimiter = get_delimiter(lines)
    if(delimiter == ''):
       exeception_handler('Could not predict delimiter for file : '+ files[x])

    delimiters.append(delimiter)
    header = lines.split(delimiter)
    #Build final Header
    if(x != 0):
       for y in range(0, len(header)):
          if header[y] not in header_final:
             header_final.append(header[y])
    else:
       header_final.extend(header)

   return header_final

def get_body(csv_data, header):
    body_final_rows = []
    #for each csv file get body
    for w in range(0,len(csv_data)):
       csv_content = csv_data[w]
       csv_rows = []
       #get csv body
       for x in range(0, len(csv_content)):
        csv_row = csv_content[x].split(delimiters[w])
        csv_rows.append(csv_row)

       #For each file in csv whe need to fill 
       csv_head = csv_rows[0]
       rows = []
       for y in range(1, len(csv_rows)):
          row = []
          #For each value in header fill if exists
          for z in range(0, len(header)):
             ixd = csv_head.index(header[z])
             value = ''
             if(ixd < 0):
                value = 'null'
             else:
                value = csv_rows[y][ixd]
             row.append(value)   
          body_final_rows.append(row)
   
    return body_final_rows   
       
def get_csv_data(files):
    csv_data = []
    for x in range(0,len(files)):
       validate_directory(files[x])
       file = open(files[x])  
       lines = file.read().splitlines() 
       csv_data.append(lines)
    return csv_data

         
def main(files):
   files = ['C:\\Users\\STEVEN\\run_3_202308081505.csv','C:\\Users\\STEVEN\\run_3_202308081505.csv']
   csv_data = get_csv_data(files)   
   header = get_header(csv_data)  
   body = get_body(csv_data,header)

   create_csv(header,body, 'output.txt')


argsx =  sys.argv[1:len(sys.argv)] 
main(argsx)