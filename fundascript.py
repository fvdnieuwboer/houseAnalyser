from bs4 import BeautifulSoup
import datetime
import pycurl
import certifi
import requests
from io import BytesIO
import sqlite3
import re
import json
from random import randint
from time import sleep

def sql_database():
    conn = sqlite3.connect('Funda_Data.db') #Opens Connection to SQLite database file.

    #number of pages of houses in the overview for specific price bracket
    conn.execute('''CREATE TABLE IF NOT EXISTS NumberOfHouses(
        lower_price_limit INTEGER NOT NULL,
        upper_price_limit INTEGER NOT NULL,
        pages INTEGER NOT NULL,
        date DATE NOT NULL
    );''')

    conn.commit() # Commits the entries to the database
    
    #identification of the houses
    conn.execute('''CREATE TABLE IF NOT EXISTS Houses(
        house_id INTEGER PRIMARY KEY,
        funda_global_id INTEGER NOT NULL,
        discovery_date DATE NOT NULL,
        house_name TEXT NOT NULL,
        postalcode TEXT NOT NULL,
        city TEXT NOT NULL,
        neighborhood TEXT NOT NULL,
        house_size INTEGER NOT NULL,
        land_size INTEGER NOT NULL,
        rooms INTEGER NOT NULL,
        funda_url TEXT NOT NULL
    );''')
    conn.commit() # Commits the entries to the database

    #time based data about houses
    conn.execute('''CREATE TABLE IF NOT EXISTS HousePrices(
        house_id INTEGER NOT NULL,
        date DATE NOT NULL,
        asking_price INTEGER NOT NULL,
        house_labels TEXT NOT NULL,
        FOREIGN KEY (house_id) REFERENCES Houses (house_id) ON DELETE CASCADE ON UPDATE NO ACTION
    );''')
    conn.commit() # Commits the entries to the database

    # time based data about views
    conn.execute('''CREATE TABLE IF NOT EXISTS HouseViews(
        house_id INTEGER NOT NULL,
        date DATE NOT NULL,
        views INTEGER NOT NULL,
        likes INTEGER NOT NULL,
        FOREIGN KEY (house_id) REFERENCES Houses (house_id) ON DELETE CASCADE ON UPDATE NO ACTION
    );''')
    conn.commit() # Commits the entries to the database
    conn.close()

def check_existing_house(funda_global_id):
    conn = sqlite3.connect('Funda_Data.db')
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM Houses WHERE funda_global_id='+(str(funda_global_id)))
    result = cur.fetchone()[0]
    conn.close()
    if result == 0:
        return False
    else:
        return True

def find_house(funda_global_id):
    conn = sqlite3.connect('Funda_Data.db')
    cur = conn.cursor()
    cur.execute('SELECT house_id FROM Houses WHERE funda_global_id='+(str(funda_global_id)))
    result = cur.fetchone()[0]
    conn.close()
    return result

def find_house_price(house_id,date,asking_price,labels):
    conn = sqlite3.connect('Funda_Data.db')
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM HousePrices WHERE house_id='+(str(house_id))+' AND asking_price=\''+str(asking_price)+'\''+' AND house_labels=\''+labels+'\'')
    result = cur.fetchone()[0]
    conn.close()
    if result == 0:
        return True
    else:
        return False
    
def checkViews(house_id,date):
    conn = sqlite3.connect('Funda_Data.db')
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM HouseViews WHERE house_id='+(str(house_id))+' AND date=\''+date+'\'')
    result = cur.fetchone()[0]
    conn.close()
    if result == 0:
        return True
    else:
        return False

def get_webpage(url):
    # Creating a buffer as the cURL is not allocating a buffer for the network response
    result = BytesIO()
    c = pycurl.Curl()
    #initializing the request URL
    c.setopt(c.URL, url)
    #setting options for cURL transfer  
    c.setopt(c.WRITEDATA, result)
    #setting the file name holding the certificates
    c.setopt(c.CAINFO, certifi.where())
    #setting browser to avoid bots page
    c.setopt(pycurl.USERAGENT, 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36')
    # perform file transfer
    c.perform()
    #Ending the session and freeing the resources
    c.close()
    return result.getvalue()

def add_new_house(funda_global_id, house_name, postalcode, city, house_size, land_size, url):
    rooms = ""
    discovery_date = datetime.date.today().strftime('%Y-%m-%d')
    detailBuffer = get_webpage(url) #Temporary call, to be repaced later
    detailSoup = BeautifulSoup(detailBuffer, features="html.parser")
    neighborhood = (detailSoup.find('a',attrs={'fd-m-left-2xs--bp-m fd-display-block fd-display-inline--bp-m'})).text.strip()
    for room in detailSoup.findAll('span',attrs={'kenmerken-highlighted__value fd-text--nowrap'}):
        rooms = room.text   
    conn = sqlite3.connect('Funda_Data.db')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO Houses (funda_global_id, discovery_date, house_name, postalcode, city, neighborhood, house_size, land_size, rooms, funda_url) VALUES (?,?,?,?,?,?,?,?,?,?)',(funda_global_id, discovery_date, house_name, postalcode, city, neighborhood, house_size, land_size, rooms, url))
    conn.commit()
    conn.close()
    # Sleep a random number of seconds (between 1 and 3)
    sleep(randint(1,3))
    
##################
# Make separate def here to directly pull the views and likes object
##################
def create_views_likes(house_id,date,views,likes):
    conn = sqlite3.connect('Funda_Data.db')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO HouseViews VALUES (?,?,?,?)',(house_id,date,views,likes))
    conn.commit()
    conn.close()

def gather_views_likes(house_id,object_id):
    buffer = get_webpage('https://www.funda.nl/objectinsights/getdata/'+str(object_id)+'/') 
    soup = BeautifulSoup(buffer, features="html.parser")
    try:
        site_json=json.loads(soup.text)
    except:
        numberViews = 0
        numberSaves = 0
    try:
        numberViews = site_json['NumberOfViews']
    except:
        numberViews = 0
    try:
        numberSaves = site_json['NumberOfSaves']
    except:
        numberSaves = 0
    create_views_likes(house_id,datetime.date.today().strftime('%Y-%m-%d'),numberViews,numberSaves)
    
##################
# Make separate def here to directly pull the views and likes object
##################
def update_labels_price(house_id,asking_price,labels):
    conn = sqlite3.connect('Funda_Data.db')
    cursor = conn.cursor()
    cursor.execute('INSERT INTO HousePrices VALUES (?,?,?,?)',(house_id,(datetime.date.today().strftime('%Y-%m-%d')),asking_price,labels))
    conn.commit()
    conn.close()
    
#################################
# Make separate def here overview page analyser
def processPage(overview_url):
    buffer = get_webpage(overview_url) #Get the overview
    soup = BeautifulSoup(buffer, features="html.parser")
    
    # Traverse through captured html populate houses 
    for huis in soup.findAll('div', attrs={'class':['search-result-main','search-result-main-promo']}):
        # funda_global_id INTEGER NOT NULL,
        funda_global_id=json.loads(huis.find('div', attrs={'class':'user-save-object'})['data-listing-tracking-properties'])['global_id']

        # Is this an known house, create it
        if not(check_existing_house(funda_global_id)):
            # deep dive in the house page itself
            house_name = (huis.find('h2', attrs={'class':'search-result__header-title fd-m-none'})).text.strip()
            postalcode = (huis.find('h4', attrs={'class':'search-result__header-subtitle fd-m-none'})).text.strip()[:7]
            city =(huis.find('h4', attrs={'class':'search-result__header-subtitle fd-m-none'})).text.strip()[8:]
            try:
                house_size = int(re.sub('\s+m²$','',(huis.find('span', attrs={'title':'Gebruiksoppervlakte wonen'})).text))
            except:
                house_size = 0
            try:
                land_size = int((re.sub('\s+m²$','',(huis.find('span', attrs={'title':'Perceeloppervlakte'})).text)).replace('.',''))
            except:
                land_size = 0
            url= 'https://www.funda.nl/'+(huis.find('a', attrs={'data-object-url-tracking':'resultlist'}))['href']
            #invoke subroutine
            add_new_house(funda_global_id, house_name, postalcode, city, house_size, land_size, url)        
        
        # For debug, print the house name
        print((huis.find('h2', attrs={'class':'search-result__header-title fd-m-none'})).text.strip())
        
        # Get the house identifier from the system
        house_id = find_house(funda_global_id)
        
        # Get the labels labels
        house_labels=[]
        labels = huis.find('ul', attrs={'labels search-result__header-labels'})
        if not(labels is None): 
            for label in labels.findAll('li'):
                house_labels.append(label.text)
    
        # Get the price
        try:
            asking_price = int(re.sub('^€\s|\.|\sk.k.$|\sv.o.n.$','',huis.find('span', attrs={'class':'search-result-price'}).text.strip()))
        except:
            asking_price = 0
                
        if house_id != 0:
            if find_house_price(house_id,datetime.date.today().strftime('%Y-%m-%d'),asking_price,json.dumps(house_labels)):
                update_labels_price(house_id,asking_price,json.dumps(house_labels))    
            
            # Update Views + Likes
            if checkViews(house_id,datetime.date.today().strftime('%Y-%m-%d')):
                gather_views_likes(house_id, funda_global_id)
            
##############
# Main code

#################
print('Starting new run')

sql_database() #Initiate the database

buffer = get_webpage('https://www.funda.nl/koop/haarlem/') #To understand number of pages
soup = BeautifulSoup(buffer, features="html.parser")

totalpages=[] #List to store rating of the product
# Traverse through captured html populate pages
for pages in soup.findAll('div', attrs={'class':'pagination-pages'}):
    for page in pages.findAll('a'):
        totalpages.append(int(re.sub(r"^\s+Pagina\s|\s+$","",page.text)))

print("Total pages to process: ", max(totalpages))

currentpage = 1
# For each of the subsequent pages
while currentpage <= max(totalpages):
    print('https://www.funda.nl/koop/haarlem/p'+str(currentpage)+'/')
    processPage(('https://www.funda.nl/koop/haarlem/p'+str(currentpage)+'/'))
    
    currentpage+=1
    
    # Sleep a random number of seconds (between 2 and 10)
    sleep(randint(2,10))

