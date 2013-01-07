import urllib, sqlite3, StringIO, json, os, re
from lxml.html import etree, parse
import datetime
from urlparse import urlparse
from collections import defaultdict

cwd = os.getcwd()
parser = etree.HTMLParser()

#returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

conn = sqlite3.connect(cwd + '/congress/data/members.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

first_inits = []
for cong in range(1, 114):
    inits = {}
    inits['total'] = 0
    
    members = c.execute("SELECT * from terms where congress = %d" % cong).fetchall()
    for member in members:
        inits['total'] += 1
        if member['name'] == "":
            continue
        last = member['name'].split(",")[0].title()
        first = member['name'].split(",")[1].strip().split(" ")[0].title()
        if first != "":
            if first[-1] not in inits:
                inits[first[-1]] = { 'size': 0, 'instances': [] }
            inits[first[-1]]["size"] += 1
            inits[first[-1]]["instances"].append(member['name'])
    first_inits.append(inits)

f = open(cwd + "/congress/data/first_name_last_letter.json", 'w')
f.write(json.dumps(first_inits, indent=2))
f.close()

print "done"


#longest serving
'''
for member in c.execute("SELECT name, count(*) as count FROM terms group by pid order by count desc").fetchall():
    if member['count'] >= 15:
        print member
'''


#conn.commit()
conn.close()