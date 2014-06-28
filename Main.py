__author__ = 'vyt'
import requests
from bs4 import BeautifulSoup
import sqlite3
import datetime

link = 'http://www.cvonline.lt/darbo-skelbimai/visi?sort=tp_id&dir=desc&page=1000'
testlink = 'http://www.cvonline.lt/darbo-skelbimai/visi?sort=tp_id&dir=desc&page=5'
conn = sqlite3.connect("Database.db")

# Takes furthest page link as argument and does magic
def make_pages_list(link):

    # Makes a list of all the Url's that are currently in DB and of all active (marked as such) Url's in DB
    listof_LinksInDB, listof_AllActiveUrlsInDB = make_listoflinkinDB()

    page = requests.get(link)
    soup = BeautifulSoup(page.text, 'lxml')
    looking_fornumber = soup.find(id="pagination").find_all('a')[-1].get('href')
    number_ofpages = int(looking_fornumber[looking_fornumber.index('page=')+5:])
    get_each_Ad(link, number_ofpages, listof_LinksInDB, listof_AllActiveUrlsInDB)



# Part of make_pages_list: takes a number of pages and existing URLS, as well as Active URLS
#  and updates Ad_table with new Urls, also Updates existing Urls that expired. Returns a list
#  of Urls that are not yet parsed for Job_table

def get_each_Ad(link, number_ofpages, listoflinksinDB, listofactiveUrls):

    info_listofnewAds = []  # This is a container of all new URLs data, so that all data can be pushed to DB at once
    list_ofnewUrls = []
    list_ofallUrls = []

    for number in range(number_ofpages+1):
        pagelink = link[0:link.index('page=')+5] + str(number)

        page = requests.get(pagelink)
        soup = BeautifulSoup(page.text, 'lxml')
        onePage = soup.find_all('tr', itemtype="http://schema.org/JobPosting")
        for JobAd in onePage:
            Url = JobAd.find('a', class_="contentJobTitle").get('href')
            list_ofallUrls.append(Url)
            if Url not in listoflinksinDB:
                templist = []
                Name = JobAd.find('a', class_="contentJobTitle").contents[0][1:]
                Views = JobAd.find('td', itemtype="http://schema.org/Place").contents[2][11:]
                DatePosted = JobAd.find(itemprop="datePosted").contents[0]
                DateExpires = JobAd.find_all('p')[2].contents[0][22:]

                templist.append(Url)
                templist.append(DatePosted)
                templist.append(DateExpires)
                templist.append('Active')
                templist.append(str(datetime.date.today()))
                templist.append(Views)
                templist.append(Name)
                info_listofnewAds.append(tuple(templist))

                list_ofnewUrls.append(Url)


    # Check if All active URLs from DB are still active in new parse, if not update DB table with current date
    for activeUrl in listofactiveUrls:
        if activeUrl not in list_ofallUrls:
            conn.execute("UPDATE Ad_table set DateExpired = ? WHERE Url = ?", (str(datetime.date.today()), activeUrl))
            conn.commit()


    conn.executemany("INSERT INTO Ad_table VALUES (?,?,?,?,?,?,?)", info_listofnewAds)
    conn.commit()

    return list_ofnewUrls # A new function is supposed to take this and make a parse for Job information


def make_listoflinkinDB():
    listoflinksinDB = []
    listofactiveUrls = []
    retrieveUrls = conn.execute("SELECT * FROM Ad_table").fetchall()
    for var in retrieveUrls:
        listoflinksinDB.append(var[0])
        if var[3] == 'Active':
            listofactiveUrls.append(var[0])

    return listoflinksinDB, listofactiveUrls

listofpages = make_pages_list(link)


#####################################################
