## a script that downloads the surveillance file [10minutes.xml],
## saves the details in a database, and performs real-time queries on the data.



import mysql.connector
import xml.etree.ElementTree as ET
from mysql.connector import Error
import datetime
import DownloadFile
import  Queries

url = "https://ims.gov.il/sites/default/files/ims_data/xml_files/imslasthour.xml"

db = mysql.connector.connect(
            host = "localhost",
            user = "root",
            passwd = "123456789",
            database = "Observation"
            )

def CreateTable (cursor: mysql, column_info: list) -> None:
    # Create a table to store observation data with dynamic column names
    #cursor.execute("DROP TABLE IF EXISTS 10MinutesObservation;")
    create_table_query = f'''
        CREATE TABLE IF NOT EXISTS 10MinutesObservation (
            {', '.join(f"{name} {data_type}" for name, data_type in column_info)}
        )
    '''

    cursor.execute(create_table_query)


def GetColParseXml(xml_file: str):
    tree = ET.parse(xml_file)
    root = tree.getroot()
    col_first_chiled=[]
    for element in root[0].iter(): #first_child
        col_first_chiled.append((element.tag,"REAL"))
    basic_col= [("stn_name", "VARCHAR(255)"),("stn_num", "REAL"),("time_obs", "DATETIME")]
    for pair in basic_col:
        col_first_chiled.append(pair)
    return root ,col_first_chiled[1:]


def UpdateLastDate(cursor: mysql, new_date: str) -> None:
    try:
        # Update the value in the table
        cursor.execute(f"UPDATE LastDateObservation SET lastdate = '{new_date}'")
        db.commit()
        print(f"Date {new_date} updated successfully.")
    except Error as e:
        print(f"Error: {e}")

def GetDate(cursor: mysql) -> datetime:
    try:
        # Retrieve the value from the table
        cursor.execute("SELECT lastdate FROM LastDateObservation")
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            print("No date found in the table.")
            return None
    except Error as e:
        print(f"Error: {e}")
        return None

#get the latest date from xml file
def GetLatestDateFromXml(xml_file: str,findmin: bool) :
    try:
        tree = ET.parse(xml_file)
        date_elements = tree.findall('.//time_obs')

        if not date_elements:
            print("No time_obs elements found in the XML.")
            return None

        date_strings = [date_element.text for date_element in date_elements]
        date_objects = [datetime.datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S') for date_str in date_strings]
        if findmin:
            return min(date_objects)
        else:
            return max(date_objects)

    except ET.ParseError as e:
        print(f"XML parse error: {e}")
        return None

'''
The function CheckIfXmlAsNewDataByDate determines the necessity of adding new data by 
comparing the last date stored in the table with the first date from a new XML file,
aiming to prevent duplicates.
'''
def CheckIfXmlAsNewDataByDate(cursor: mysql ,xml_file: str) -> bool:
    cursor.execute("CREATE TABLE IF NOT EXISTS LastDateObservation (lastdate DATETIME)")
    last_date_table = GetDate(cursor)

    first_date_xml = GetLatestDateFromXml(xml_file,True)
    print(f"{first_date_xml=}, {last_date_table=}")


    if last_date_table and last_date_table < first_date_xml:
        return True
    return False

def UpdateDatabases(cursor: mysql , xml_file:str) -> None:
    root, column_info = GetColParseXml(xml_file)

    CreateTable(cursor, column_info)  # TODO add check different row

    column_names_all = [pair[0] for pair in column_info]

    column_names_REAL, column_names_STR = column_names_all[:18], column_names_all[18:]

    for observation in root.findall(".//Observation"):
        # Extract values for real columns
        values_REAL = ', '.join(
            [f"{(observation.find(name).text)}" if observation.find(name).text else "NULL" for name in
             column_names_REAL])
        # Extract values for string columns
        values_STR = ', '.join(
            [f"'{(observation.find(name).text)}'" if observation.find(name).text else "NULL" for name in
             column_names_STR])
        # Combine values for both types of columns
        values = f"{values_REAL},{values_STR}"

        cursor.execute(f'''
                            INSERT INTO 10MinutesObservation ( {', '.join(column_names_all)})
                            VALUES ({values})
                        ''', )



if __name__ == '__main__':
    cursor = db.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS Observation")
    #cursor.execute("DROP TABLE IF EXISTS LastDateObservation;")

    # Download the file
    xml_file =  DownloadFile.DownloadFile(url) #"imslasthour.xml"
    if xml_file:

        # Checking that the data in the file is new - to prevent duplication
        check_file = CheckIfXmlAsNewDataByDate(cursor, xml_file)

        # Update db
        UpdateDatabases(cursor, xml_file)

        # Update date table
        last_date_xml = GetLatestDateFromXml(xml_file, False)
        UpdateLastDate(cursor, last_date_xml)

        # Commit changes
        db.commit()

        # Queries
        Queries.Queries(cursor)



        #cursor.execute("SELECT * FROM LastDateObservation")
        #for x in cursor:
        #    print(x)