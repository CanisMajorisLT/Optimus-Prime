_author__ = 'vyt'
import requests
from bs4 import BeautifulSoup
import sqlite3
import datetime

conn = sqlite3.connect("Database.db")

# Takes furthest page link as argument and does magic
def update_Main_Ad_Table_0():
    link_for_getting_number_of_pages = 'http://www.cvonline.lt/darbo-skelbimai/visi?sort=tp_id&dir=desc&page=1000'
    rss_link = "http://www.cvonline.lt/darbo-skelbimai/visi?sort=tp_id&dir=desc&type=rss&page=0"

    # Makes a list of all the Url's that are currently in DB and of all active (marked as such) Url's in DB
    list_of_links_in_DB, list_of_all_active_urls_in_DB = make_listoflinkinDB()

    page = requests.get(link_for_getting_number_of_pages)
    soup = BeautifulSoup(page.text, 'lxml')
    looking_for_number = soup.find(id="pagination").find_all('a')[-1].get('href')
    number_of_pages = int(looking_for_number[looking_for_number.index('page=')+5:])

    update_Main_Ad_Table_1(rss_link, number_of_pages, list_of_links_in_DB, list_of_all_active_urls_in_DB)



# Part of update_Main_Ad_Table_0: takes a number of pages and existing URLS, as well as Active URLS
#  and updates Main_Ad_Table with new Urls, also Updates existing Urls that expired.

def update_Main_Ad_Table_1(rss_link, number_ofpages, listoflinksinDB, listofactiveUrls):

    list_ofallUrls = []


    for number in range(number_ofpages+1):
        pagelink = rss_link[0:rss_link.index('page=')+5] + str(number)
        info_listofnewAds = []  # This is a container of all new URLs data, so that all data can be pushed to DB at once
        page = requests.get(pagelink)
        soup = BeautifulSoup(page.text, 'lxml')
        one_page = soup.find_all('item')


        for JobAd in one_page:
            url = JobAd.find('guid').get_text()
            list_ofallUrls.append(url)

            if url not in listoflinksinDB:

                temporary_list = [] # Temporary container for data taken from URL that is latter appended to info_listof...

                name = JobAd.find('job_function').get_text()
                views = JobAd.find('td', itemtype="http://schema.org/Place").contents[2][12:]
                date_posted = JobAd.find(itemprop="datePosted").get_text()
                date_expires = JobAd.find_all('p')[2].contents[0][20:]
                try:
                    city = JobAd.find('category').get_text()
                except:
                    city = "Unspecified"
                try:
                    industry = JobAd.find('job_industry').get_text()
                except:
                    industry = "Unspecified"
                try:
                    employer_or_adagency = JobAd.find('employer').get_text()
                except:
                    employer_or_adagency = "Unspecified"


                # Performs a parse with URL for content (keywords)
                content_page = requests.get(url)
                content_soup = BeautifulSoup(content_page.text, 'lxml')
                content = content_soup.find('meta', {'name':'keywords'})['content']


                temporary_list.append(url)
                temporary_list.append(date_posted)
                temporary_list.append(date_expires)
                temporary_list.append('None')
                temporary_list.append(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                temporary_list.append('Active')
                temporary_list.append(views)
                temporary_list.append(name)
                temporary_list.append(city)
                temporary_list.append(industry)
                temporary_list.append(content)
                temporary_list.append(employer_or_adagency)

                info_listofnewAds.append(tuple(temporary_list))

            # Updates Views, DateParsed with values of a new parse
            if url in listofactiveUrls:

                views_updated = JobAd.find('td', itemtype="http://schema.org/Place").contents[2][12:]
                old_values = conn.execute("SELECT Views, DateParsed, DatePublished FROM Main_Ad_Table WHERE Url = ?", (url,)).fetchone()
                new_value_views = old_values[0] + ',' + views_updated
                new_value_parsed = old_values[1] + ',' + datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                conn.execute("UPDATE Main_Ad_Table set Views = ?, DateParsed = ? WHERE url = ?",
                             (new_value_views, new_value_parsed, url))


        print(number)

        # Update DB with info of all new URLs using a list containing data of all new URLs
        conn.executemany("INSERT INTO Main_Ad_Table VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", info_listofnewAds)
        conn.commit()

    # Check if All active URLs from DB are still active in new parse, if not update DB table with current date
    for activeUrl in listofactiveUrls:
        if activeUrl not in list_ofallUrls:
            print(activeUrl)  #čia šitas print šiai debuginimui :)
            conn.execute("UPDATE Main_Ad_Table set IsActive = ?, DateExpired = ? WHERE url = ?",
                         ("Inactive", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), activeUrl)) #sukurt naują stalą
            conn.commit()






# Function called by update_Main_Ad_Table_0 to get existing URL list from DB
def make_listoflinkinDB():
    list_ofurlsindb = []
    list_ofactiveurls = []
    retrieve_urls = conn.execute("SELECT * FROM Main_Ad_Table").fetchall()
    for var in retrieve_urls:
        list_ofurlsindb.append(var[0])
        if var[5] == 'Active':
            list_ofactiveurls.append(var[0])

    return list_ofurlsindb, list_ofactiveurls

update_Main_Ad_Table_0()


#####################################################
