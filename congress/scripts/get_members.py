import urllib, sqlite3, StringIO, json, os, re
from lxml.html import etree, parse
import datetime
from urlparse import urlparse

cwd = os.getcwd()
parser = etree.HTMLParser()

#returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def get_bio(pid):
    page = parse("http://bioguide.congress.gov/scripts/biodisplay.pl?index=%s" % pid)
    name = page.xpath("//a[@name='Top']/text()")
    #print name[0], name[0].split(",")
    if len(name) == 0:
        return 1

    last = name[0].split(",")[0].strip().title()
    first = name[0].split(",")[1].strip().split(" ")[0]
    if len(name[0].split(",")[1].strip().split(" ")) > 1:
        middle = name[0].split(",")[1].strip().split(" ")[1]
    else:
        middle = ""
    nickname = re.search("\(([A-z]+)\)", name[0])
    if nickname:
        nickname = nickname.group(1)
    else:
        nickname = ""
    bio = page.xpath("//p/text()")[0]
    lifespan = re.findall("\d{4}", page.xpath("//font[@size=4]/text()")[0])
    birth = -1 if len(lifespan) == 0 else lifespan[0]
    death = -1 if len(lifespan) < 2 else lifespan[1]
    if len(lifespan) > 1:
        death = lifespan[1]
    c.execute('''INSERT OR IGNORE INTO bios (pid, first, last, middle, nickname, born, died, bio) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (pid, first, last, middle, nickname, birth, death, bio))
    conn.commit()
    return 0

def get_congress(cong):
    params = urllib.urlencode({'congress': cong})
    results = urllib.urlopen('http://bioguide.congress.gov/biosearch/biosearch1.asp', params)
    page = etree.parse(StringIO.StringIO(results.read()), etree.HTMLParser())
    nas = 1
    for member in page.xpath("//table")[1].xpath("tr")[1:]:
        name = member.xpath("td/a/text()")
        print name
        if len(name) == 0:
            name = ""
            print nas
            nas += 1
            continue        
        else:
            name = name[0]
            pid = member.xpath("td/a/@href")[0].split("=")[1]
        stats = member.xpath("td/text()")
        c.execute('''INSERT OR IGNORE INTO terms (pid, name, dates, position, party, state, congress)
            VALUES (?,?,?,?,?,?,?)''', (pid, name, stats[0], stats[1][0], stats[2], stats[3], int(stats[4])))        
    conn.commit()
        
conn = sqlite3.connect(cwd + '/congress/data/members.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

c.execute('''
    CREATE TABLE IF NOT EXISTS bios
       ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "pid" VARCHAR(64) UNIQUE,
        "last" VARCHAR(64),
        "first" VARCHAR(64),
        "middle" VARCHAR(64),
        "nickname" VARCHAR(64),        
        "born" INTEGER,
        "died" INTEGER,
        "start" INTEGER,
        "end" INTEGER,
        "bio" TEXT,
        "img" VARCHAR(100),
        "imgcredit" VARCHAR(100))'''
    )

c.execute('''
    CREATE TABLE IF NOT EXISTS terms
       ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "pid" VARCHAR(64),
        "name" VARCHAR(100),
        "dates" VARCHAR(64),
        "position" VARCHAR(2),
        "party" VARCHAR(64),        
        "state" VARCHAR(5),
        "congress" INTEGER,
        CONSTRAINT "unq" UNIQUE (pid, congress))'''
    )

def get_all_sessions():
    for cong in range(1, 114):
        get_congress(cong)

def get_all_bios():
    for asc in range(65, 91):
        misses = 0
        for i in range(1, 10000):
            pid = chr(asc) + str(i).rjust(6, '0')
            print pid
            misses += get_bio(pid)
            if misses > 10:
                break

def find_dupes():
    for bio in c.execute("SELECT * FROM bios").fetchall():
        name = re.search("\(See (.*?)\)", bio["bio"])
        if name:
            name = name.group(1)
            last = name.split(",")[0].strip().title().replace(".", "")
            first = name.split(",")[1].strip().split(" ")[0].replace(".", "")
            if len(name.split(",")[1].strip().split(" ")) > 1:
                middle = name.split(",")[1].strip().split(" ")[1].replace(".", "")
            else:
                middle = ""
            nickname = re.search("\(([A-z]+)\)", name)
            if nickname:
                nickname = nickname.group(1)
            else:
                nickname = ""
            print last, first, middle, nickname
            m = c.execute('''SELECT * from bios WHERE last=? and first=? and middle=?''' , (last, first, middle)).fetchone()
            if m != []:
                print m
                #c.execute("UPDATE bios SET alternate = '%s', altname = '%s' WHERE pid = '%s'" % (bio['pid'], name, m['pid']))
                #c.execute("DELETE FROM bios WHERE pid = '%s'" % bio['pid'])
                #conn.commit()
            

conn.commit()
conn.close()