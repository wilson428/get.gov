import urllib, sqlite3, StringIO, json, os
from lxml.html import etree, parse
import datetime
from urlparse import urlparse
import scrapelib

#intialize scraper and parser
s = scrapelib.Scraper(requests_per_minute=60, follow_robots=False)
parser = etree.HTMLParser()

#get current working directory for file system access in Cloud9IDE
cwd = os.getcwd()

#returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

conn = sqlite3.connect(cwd + '/petitions/data/petitions.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

c.execute('''
    CREATE TABLE IF NOT EXISTS petitions
       ("pid" VARCHAR(64) PRIMARY KEY ,
        "title" VARCHAR(100),
        "url" VARCHAR(100),
        "text" TEXT,"created" DATE,
        "signatures" INTEGER,
        "updated" DATETIME,
        "ignore" BOOL NOT NULL  DEFAULT 0,
        "dead" BOOL NOT NULL  DEFAULT 0)
    ''')

conn.commit()

#scan WH site, add any new petitions to DB
def get_petitions(start=1):
    for pg in range(start,1000):
        #The WH site loads petitions from an external HTML doc in a JSON shell
        url = "https://petitions.whitehouse.gov/petitions/more/all/%d/2/0/" % pg
        try:
            raw = s.urlopen(url).encode('utf-8')
        except scrapelib.HTTPError as e:
            print "Error downloading %s" % url
        resp = json.loads(raw)
        if "markup" not in resp or len(resp["markup"]) == 0:
            print "No results at page %i" % pg
            return
        page = etree.parse(StringIO.StringIO(resp['markup']), parser)
        count = 0
        for petition in page.xpath("body/div"):
            #get uid for each petition from main div id
            pid = petition.xpath('@id')[0].split("-")[1]
            if len(pid) < 10:
                continue            
            title = petition.xpath("div/div/a/text()")[0]
            url = petition.xpath("div/div/a/@href")[0]

            #if this petition is not in our DB
            if c.execute("SELECT * FROM petitions WHERE pid = '%s'" % pid).fetchone() == None:
                count += 1
                info = crawl(url)
                c.execute('''INSERT OR IGNORE INTO petitions (pid, title, url, text, created) VALUES (?,?,?,?,?)''', (pid, title, url, info["text"], info["created"]))
        conn.commit()   
        print "Found %i petitions on page %i" % (count, pg)
        if (count == 0):
            return
        
#visit the page for each petition and get the vitals
#this should work both for petitions gleaned from the WH site and ones found on Twitter
def crawl(path):
    if path[0] == "/":
        path = "http://petitions.whitehouse.gov" + path
    
    resp = urllib.urlopen(path)
    url = resp.geturl()
    if urlparse(url).netloc != "petitions.whitehouse.gov":
        return {}
    page = etree.parse(StringIO.StringIO(resp.read()), parser)    
    try:
        title = page.xpath("//h1[@class='title']/text()")[0].strip()
        text = "\n".join(page.xpath("//div[@id='petitions-individual']/div/div/p/text()"))
        raw_date = page.xpath("//div[@class='date']/text()")[0].strip()
        tags = page.xpath("//div[@class='issues']/a/text()")
        pid = page.xpath("//a[@class='load-next no-follow active']/@rel")[0]
    except:
        print "no luck with ", url
        return {}
    #convert to SQLite format
    created = datetime.datetime.strptime(raw_date, "%b %d, %Y").strftime("%Y-%m-%d")
    #we'll handle signatures separately
    return {
        "pid": pid,
        "title": title,
        "text": text,
        "tags": tags,
        "created": created,
        "url": urlparse(url).path
    }

#update the number of signatures for every petition in our DB
def get_signatures():
    for petition in c.execute("SELECT * FROM petitions ORDER BY signatures").fetchall():
        url = "http://petitions.whitehouse.gov" + petition['url']
        page = etree.parse(StringIO.StringIO(urllib.urlopen(url).read()), parser)
        pid = petition['pid']
        
        #update total signatures
        signatures = page.xpath("//div[@class='num-block num-block2']/text()")
        try:
            signatures = int(signatures[0].replace(",", ''))
            print pid, signatures
            c.execute('''UPDATE petitions SET signatures = ?, updated = ? WHERE pid = ?''', (signatures, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), petition['pid']))
            conn.commit()
        except:
            print url
            
'''
SEARCH TWITTER FOR PETITIONS NOT YET ON WH site
'''
import oauth2 as oauth       

#Register a Twitter app to get credentials: https://dev.twitter.com/apps/new
CONSUMER_KEY = "gUuCyW6gXQjJYyQuJaIofg"
CONSUMER_SECRET = "4kKrkGjigTXKDOgd0MkmaW7m403Y17TsqW6LvapxjdA"
ACCESS_TOKEN = "15741451-9u4QH2KYwsl9kI1j63fxzIYwDPmkDZrtHkMKJ105I"
ACCESS_TOKEN_SECRET = "HTSt62bLZEH7DoZz8tFopj2p5NL69qbf62iru48U"

# Create your consumer with the proper key/secret.
consumer = oauth.Consumer(key=CONSUMER_KEY, secret=CONSUMER_SECRET)
token = oauth.Token(ACCESS_TOKEN,ACCESS_TOKEN_SECRET)

# Create our client.
client = oauth.Client(consumer, token)

def search_twitter(url="http://search.twitter.com/search.json?q=whitehouse%20petitions&rpp=100&include_entities=true&result_type=mixed"):
    print "SEARCHING", url
    resp, content = client.request(url)
    results = json.loads(content)    
    if 'results' not in results:
        return    
    for result in results['results']:
        for url in result['entities']['urls']:
            info = crawl(url['expanded_url'])
            if info:
                print info["title"].encode("ascii", "replace")
                c.execute('''INSERT OR IGNORE INTO petitions (pid, title, url, text, created) VALUES (?,?,?,?,?)''', (info["pid"], info["title"], info['url'], info["text"], info["created"]))
        conn.commit()        

    if 'next_page' in results:
        search_twitter("http://search.twitter.com/search.json" + results["next_page"])

#get_petitions()
#search_twitter()
get_signatures()

conn.close()
