import glob
import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET

# We are with the files from the online repo
#  https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
# that we downloaded and unzip in the folder "source"

# Our file output files for logs and data frame
log_file = "output/log_file.txt" 
target_file = "output/transformed_data.csv" 

# Task 1: Extraction
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe

# Headers of the extracted data need t be knwon and defined
def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])])

    return dataframe


# Now the the extract function
def extract():
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    # Process all csv files
    for csvfile in glob.glob("source/*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    # Process all json files
    for jsonfile in glob.glob("source/*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    # Process all xml files
    for xmlfile in glob.glob("source/*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data

# Task 2: Transformatiom
# The transform() function receive the extracted dataframe dict and apply transform function on height and weight lists
def transform(data): 
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    # data['height'] = round(data.height * 0.0254,2) 
    data['height'] = data['height'].apply(lambda x: round(x * 0.0254, 2))
 
    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    # data['weight'] = round(data.weight * 0.45359237,2)
    data['weight'] = data['weight'].apply(lambda x: round(x * 0.45359237, 2))
    
    return data

# Task 3: Loading and Logging
# 3-1 load_data() to load the transforned_data to the target file
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

# 3-2: log_progress() function to record the message
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')
                

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load_data(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 