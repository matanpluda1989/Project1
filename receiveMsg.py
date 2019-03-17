import pika
import sqlite3
from sqlite3 import Error
import csv
import datetime
import json
import os
from xml.etree.ElementTree import ElementTree
from xml.etree.ElementTree import Element
import xml.etree.ElementTree as etree
import time

#The path of all the output files (csv, json, xml)
outputPath = "C:\\Program Files (x86)\\Python37-32\\ex\\files\\"

def getCurrentTime():
    time = (datetime.datetime.now().strftime('%D')).replace('/', '-')
    i1 = (datetime.datetime.now().time())
    i1 = i1.microsecond 
    i1 = str(i1)
    i1 = i1[-3:-1]
    time = time + "_" + i1
    return time

flag = False


#Function to connection to the db
def create_connection(db_file):
    try:
        conn = sqlite3.connect(db_file) 
        print("Message received")
        return conn
    except Error as e:
        print("Message received but ", end = "") 
        print(e)
        print()

    return None 


#insert data to table 'invoices_per_country'
def create_table1(conn, rows):
    cur = conn.cursor()

    createTable1 = "CREATE TABLE IF NOT EXISTS invoices_per_country ("
    createTable1 += "Country NVARCHAR (40) PRIMARY KEY, "
    createTable1 += "QtyInvoices INTEGER NOT NULL);"
    
    try:
        cur.execute(createTable1)
        conn.commit()
        print("Table 'invoices_per_country' was created (or alreadt exists)") 
    except Error:
        print("Error - Table 'invoices_per_country' was not created")


    insertData = "INSERT INTO invoices_per_country ("
    insertData += "Country, QtyInvoices) "
    insertData += "VALUES "
    insertData += "(?, ?); "

    #Query to case the record is existing in DB but the data was updated 
    updateData = "UPDATE invoices_per_country SET "
    updateData += "QtyInvoices = ? WHERE Country = ?;"

    try:
        for row in rows:
            taskUpdate = (row[1], row[0])
            taskInsert = (row[0], row[1])
            cur.execute(updateData, taskUpdate)
            conn.commit()
            cur.execute(insertData, taskInsert)
            conn.commit()
        print("Data was received to 'invoices_per_country' ") 
    except Error:
        print("Data already exists in 'invoices_per_country' ")

    return None


#insert data to table 'disc_data'
def create_table2(conn, rows, country, year):
    cur = conn.cursor()

    createTable2 = "CREATE TABLE IF NOT EXISTS disc_data ("
    createTable2 += "DiscName NVARCHAR (40) , "
    createTable2 += "Country NVARCHAR (40) , "
    createTable2 += "QtyInvoices INTEGER NOT NULL, "
    createTable2 += "year INTEGER, "
    createTable2 += "PRIMARY KEY (DiscName, Country, year) );"
    try:
        cur.execute(createTable2)
        conn.commit()
        print("Table 'disc_data' was created (or alreadt exists)") 
    except Error:
        print("Error - Table 'disc_data' was not created")


    insertData = "INSERT INTO disc_data (DiscName, Country, QtyInvoices, year) "
    insertData += "VALUES (?, ?, ?, ?);" 

    #Query to case the record is existing in DB but the data was updated 
    updateData = "UPDATE disc_data SET "
    updateData += "QtyInvoices = ? WHERE DiscName = ? AND Country = ? AND year = ?;"

    taskUpdate = (rows[0][1], rows[0][0], country, year) 
    taskInsert = (rows[0][0], country, rows[0][1], year) 

    try:
        cur.execute(updateData, taskUpdate)
        conn.commit()
        cur.execute(insertData, taskInsert)
        conn.commit()
        print("Data was received to 'disc_data' ")    
    except Error:
        print("Data already exists in 'disc_data' ")
    
    return None


#Function to create of csv file
def create_csv(conn):
    cur = conn.cursor()
    
    query = "SELECT BillingCountry, (sum (invoice_items.Quantity) ) "
    query += "FROM invoices " 
    query += "INNER JOIN invoice_items ON invoice_items.InvoiceId = invoices.InvoiceId "
    query += "GROUP BY BillingCountry"

    cur.execute(query)
    conn.commit()
 
    rows = cur.fetchall()

    #Create table in DB and insert values the the table
    create_table1(conn, rows)

    time = getCurrentTime()

    _outputPath = outputPath + 'msgCsv_' + time + '.csv'
        
    if os.path.exists(outputPath):
        with open(_outputPath, 'w', newline = '') as writeFile:
            writer = csv.writer(writeFile)
            writer.writerow(['Country','Num of invoices'])
            writer.writerows(rows)

        writeFile.close()
    
    else:
        print("There is a problem - CSV file was not created.")
        return "error" 

    return None 

#Function to create of json file
def create_json(conn):
    cur = conn.cursor()

    query = "SELECT DISTINCT country, GROUP_CONCAT(albums.Title, ', ') " 
    query += "FROM customers "
    query += "INNER JOIN invoices ON customers.CustomerId = invoices.CustomerId "
    query += "INNER JOIN invoice_items ON invoices.Invoiceid = invoice_items.Invoiceid "
    query += "INNER JOIN tracks ON invoice_items.TrackId = tracks.TrackId " 
    query += "INNER JOIN albums ON tracks.AlbumId = albums.AlbumId "
    query += "GROUP BY country "
    
    cur.execute(query)
    conn.commit()
 
    rows = cur.fetchall()
    
    data = {}  
    data['albums'] = []  
    for row in rows:
        data['albums'].append({  
        'Country': row[0],
        'Albums': row[1]
        })
    
    time = getCurrentTime()

    _outputPath = outputPath + 'msgJson_' + time + '.json'
    if os.path.exists(outputPath):
        with open(_outputPath, 'w') as writeFile:  
            json.dump(data, writeFile)
    else:
        print("There is a problem - Json file was not created.")
        return "error" 

    return None 


#Function to create of xml file
def create_xml(conn, country, year):
    cur = conn.cursor()
    
    country = country.decode('utf8')
    year = year.decode('utf8')

    query = "SELECT albums.Title, (sum (invoice_items.Quantity) ) cnt  "
    query += "FROM invoices "  
    query += "INNER JOIN invoice_items ON invoice_items.InvoiceId = invoices.InvoiceId "
    query += "INNER JOIN tracks ON invoice_items.TrackId = tracks.TrackId " 
    query += "INNER JOIN albums ON tracks.AlbumId = albums.AlbumId "
    query += "INNER JOIN genres ON tracks.GenreId = genres.GenreId  "
    query += "WHERE BillingCountry = ? "
    query += "AND strftime('%Y',InvoiceDate) >= ? " 
    query += "AND genres.Name = 'Rock' "
    query += "GROUP BY albums.AlbumId "
    query += "ORDER BY cnt DESC "
    query += "LIMIT 1 "

    cur.execute(query, (country, year,))
    conn.commit()
 
    rows = cur.fetchall()
   
    #Create table in DB and insert values the the table
    create_table2(conn, rows, country, year)

    root = Element('disc_data')
    ElementTree(root)

    album = Element('album')
    root.append(album)
    album.text = str(rows[0][0])

    countryTree = Element('country')
    root.append(countryTree)
    countryTree.text = country

    invoices = Element('invoices')
    root.append(invoices)
    invoices.text = str(rows[0][1])

    yearTree = Element('year')
    root.append(yearTree)
    yearTree.text = year

    time = getCurrentTime()
    _outputPath = outputPath + 'msgXML_' + time + '.xml'

    mydata = etree.tostring(root)  
    mydata = mydata.decode('utf8')
    if os.path.exists(outputPath):
        myfile = open(_outputPath, "w")  
        myfile.write(mydata)  
    else:
        print("There is a problem - XML file was not created.")
        return "error" 

    return None 


#The function running every received message from the queqe by the 'pika' libary
def callback(ch, method, properties, body):
    st = str(body)
    i1 = st.find('*')
    i2 = st.find('&') 
    i3 = len(st)
    path = body[0:i1 - 2]
    country = body[i1 - 1:i2 - 2]
    year = body[i2 - 1:i3 - 1]
    
    #print(str(path) + ' '  + str(country) + ' ' + str(year))
    
    #connection to db 
    conn = create_connection(path)
    
    err = None
    if conn != None:
        
        #create json
        err = create_json(conn)

        #create xml
        err = create_xml(conn, country, year)
        
        #create csv
        err = create_csv(conn)
        
        if err != "error":
            print("Files were created in - " + outputPath + "\n") 

        conn.close()




connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
queueName = 'msg1'
channel.queue_declare(queue=queueName)

channel.basic_consume(callback,
                      queue=queueName,
                      no_ack=True)

print('\n Waiting for messages. To exit press CTRL+C \n')
channel.start_consuming()


