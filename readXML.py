# -*- coding: utf-8 -*-
"""
Created on Fri Feb 14 14:47:06 2020

@author: pranali
"""


#importing required libraries.
import xml.etree.ElementTree as ET
import json
import pandas as pd
import re
import sqlite3
import numpy as np
#os.curdir("C:/Users/daniel/Documents/test")
pd.set_option('display.max_columns', None)  
pd.set_option('display.expand_frame_repr', False)
pd.set_option('max_colwidth', -1)


# Function to parse the Json File.
def parseJSON(jsonfile):
    # Open the json file and load it into the data variable.
    with open(jsonfile) as f:
        data = json.load(f)
    # Load the fields data of json to a dataframe.
    df=pd.DataFrame([data[0]['fields']],dtype='str')
    # Load the data from dataframe to sqllite db by creating a local connection and a table.
    conn = sqlite3.connect('TestDB4.db')
    c = conn.cursor()
    c.execute('CREATE TABLE if not exists loans (rowid INTEGER PRIMARY KEY) WITHOUT ROWID')
    df.to_sql('loans', conn, if_exists='replace', index = False)
    #c.execute('ALTER TABLE loans ADD CONSTRAINT PRIMARY KEY (username);')
    c.execute('''  SELECT * FROM loans ''')
    conn.commit()
    # Printing all the rows whether the data has been loaded or not.
    for row in c.fetchall():
        print (row)
    c.close()
    conn.close()
    return data


# Creating a function for parsing the xml data for getting the credit profile from the JSON file.
def parseCreditProfile(xmlfile):    
    # create element tree object 
    # Removing the namespaces in the xmlfile.
    xmlfile = re.sub('\\sxmlns="[^"]+"', '', xmlfile, count=2)
    # finding the root of the xml file.
    root=ET.fromstring(xmlfile)
    root.findall('./Products/CreditProfile/')
    # Find the tag names under credit profile and load them into tag set.
    tag_names = {t.tag for t in root.findall('./Products/CreditProfile/')}
    print(tag_names)
    # Create empty list.
    items = []
    # Iterating through every tag in the tag name. For each tag under CreditProfile we will create a table with tag name
    # and then we will loop through every child of that tag making it as a column. Then we will convert the data of every tag into a dataframe 
    # and then we will load it into the sql table. For every tag table username will be the foreign(reference key).
    for tag in tag_names:
        # Getting the foreign key for the table.
        key=json_data[0]['fields']['username']
        conn = sqlite3.connect('TestDB4.db')
        c = conn.cursor()
        # Creating table with tag name.
        c.execute('CREATE TABLE if not exists '+ tag+' (rowid INTEGER PRIMARY KEY) WITHOUT ROWID')
        # Looping through every item(row) of the tag
        for item in root.findall('./Products/CreditProfile/'+tag):
        # Initializing  empty dictionary to store every child of the particular item.
            report = {}
            # for every item(row) looping through every child of that item.
            for child in item:
                # Checking if there is any subchild for that particular child. If yes then lopp through that subchild.
                if len(child):             
                    for subchild in child:
                        if subchild.text is None:
                            subchild.text=None
                            # Loading the subchild tag as key in the report dictionary and subchild text as value for that key in the dictionary.
                            report[subchild.tag]=subchild.text
                        else:
                            # Loading the subchild tag as key in the report dictionary and subchild text as value for that key in the dictionary.
                            report[subchild.tag]=subchild.text
                else:
                    if child.text is None:
                    # Loading the child tag as key in the report dictionary and child text as value for that key in the dictionary.
                        report[child.tag]=None
                    # Loading the child tag as key in the report dictionary and subchild text as value for that key in the dictionary.
                    else:
                        report[child.tag] = child.text
            # Finally appending each item of report dictionary to items list.
            items.append(report)
        # Creating a data frame from items containing all the data of tag table.
        df=pd.DataFrame(items,dtype='str')
        df = df.replace(r'^\s*$', np.nan, regex=True)
        df = df.astype(object).where(pd.notnull(df),None)
        # Setting the foreign key column for the tag dataframe.
        df['UserId']=key
        # Converting tag dataframe to tag table.
        df.to_sql(tag, conn, if_exists='replace', index = False)
        #c.execute('ALTER TABLE ' + tag+' ADD CONSTRAINT fk_user_id FOREIGN KEY (UserId) REFERENCES loans(username);')
        c.execute('''  SELECT * FROM '''+tag)
        conn.commit()
        # Printing out all the rows of a tag table.
        for row in c.fetchall():
            print (row)
        # Resetting the list to a empty list.
        items=[]
        c.close()
        conn.close()
        
def parseTransaction(xmlfile):
    # Removing the namespaces in the xmlfile.
    xmlfile = re.sub('\\sxmlns="[^"]+"', '', xmlfile, count=2)
    # finding the root of the xml file.
    root=ET.fromstring(xmlfile)
    # Create empty list.
    items = []
    # Getting the foreign key for the table.
    key=json_data[0]['fields']['username']
    conn = sqlite3.connect('TestDB4.db')
    c = conn.cursor()
    # Creating a table to store transactions.
    c.execute('CREATE TABLE if not exists transactions (rowid INTEGER PRIMARY KEY) WITHOUT ROWID')
    # Transaction details are at the root of the xml data. Hence looping through every item at the root to get al the transactions.
    for item in root.findall('.'):
        # Initializing  empty dictionary to store every child of the particular item.
        report = {}
        # for every item(row) looping through every child of that item.
        for child in item:
            if len(child):
                for subchild in child:
                # Checking if there is any subchild for that particular child. If yes then lopp through that subchild.
                    if subchild.tag is 'CreditProfile':
                        if subchild.text is None:
                            subchild.text=None
                 # Loading the subchild tag as key in the report dictionary and subchild text as value for that key in the dictionary.
                            report[subchild.tag]=subchild.text
                        else:
                 # Loading the subchild tag as key in the report dictionary and subchild text as value for that key in the dictionary.
                            report[subchild.tag]=subchild.text
            else:
                if child.tag is not 'Products':
                    if child.text is None:
                 # Loading the child tag as key in the report dictionary and child text as value for that key in the dictionary.
                        report[child.tag]=None
                    elif child.tag is not 'Products':
                 # Loading the child tag as key in the report dictionary and child text as value for that key in the dictionary.
                        report[child.tag] = child.text
        # Finally appending each item of report dictionary to items list.
        items.append(report)
    # Creating a data frame from items containing all the data of transactions table
    df=pd.DataFrame(items,dtype='str')
    df = df.replace(r'^\s*$', np.nan, regex=True)
    df = df.astype(object).where(pd.notnull(df),None)
    # Setting the foreign key column for the dataframe.
    df['UserId']=key
    # Converting dataframe to a table.
    df.to_sql('transactions', conn, if_exists='replace', index = False)
    #c.execute(''' ALTER TABLE transactions ADD CONSTRAINT pk_transaction_id PRIMARY KEY (transactionid) FOREIGN KEY (UserId) REFERENCES loans(username)''')
    c.execute('''  SELECT * FROM transactions''')
    conn.commit()
    # Printing out all the rows of a tag table.
    for row in c.fetchall():
        print (row)
    # Resetting the list to a empty list.
    items=[]
    c.close()
    conn.close()

# Main Function.
if __name__== "__main__":
    # Load and parse the json data using parseJSON function
    json_data=parseJSON('prequalresult.json')
    # Find  the xml part in the json data.
    xmlfile=json_data[0]['fields']['xml_data']
    # Parse the xmlfile data using parseTransaction function to get transaction details.
    print(parseTransaction(xmlfile))
    # Parse the xmlfile data using parseCreditProfile function to get the creditprofile details.
    print(parseCreditProfile(xmlfile))
