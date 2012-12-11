import json, urllib, StringIO, re, sqlite3, os
from lxml.html import etree, parse, HTMLParser, tostring, fromstring

cwd = os.getcwd()

#connect to SQLite db
conn = sqlite3.connect(cwd + '/nominations/data/nominations.sqlite')
c = conn.cursor()

c.execute('''CREATE TABLE IF NOT EXISTS nominations ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "pin" VARCHAR(16) UNIQUE, "control_no" VARCHAR(16) UNIQUE, "congress" INTEGER, "date" DATETIME, "name" VARCHAR(64), "state" VARCHAR(32), "text" TEXT, "position" VARCHAR(32), "organization" VARCHAR(64), "committee" VARCHAR(64), "split" BOOL)''')
c.execute('''CREATE TABLE IF NOT EXISTS actions ("id" INTEGER PRIMARY KEY AUTOINCREMENT, "pin" VARCHAR(16), "action_no" INTEGER, "date" DATETIME, "type" VARCHAR(32), "action" TEXT, UNIQUE(pin, date, type, action))''')
conn.commit()

parser = etree.HTMLParser()
URL = "http://thomas.loc.gov/cgi-bin/thomas"

def get_congress(cong, N):
    #POST data to pass to THOMAS
    data = {
        "database":"nominations",
        "MaxDocs":"%d" % N,
        "querytype":"phrase",
        "query":"",
        "Stemming":"Yes",
        "congress":"%d" % cong,
        "CIVcategory":"on",
        "committee":"",
        "LBDateSel":"FLD606",
        "EBSDate":"",
        "EBEDate":"",
        "sort":"sh_docid_c",
        "submit":"SEARCH"
    }

    resp = urllib.urlopen(URL, data=urllib.urlencode(data))
    results = etree.parse(StringIO.StringIO(resp.read()), parser)
    nominations = results.xpath('//div[@id="content"]/p[2]/a/@href')

    start = 0
    end = len(nominations)

    for i in range(start, end):
        nomination = nominations[i]
        url = "http://thomas.loc.gov" + nomination
        try:
            get_nomination(url)
        except Exception as e:
            print url
            print e
        if i % 100 == 0:
            print i

def get_nomination(url, dummy=False):
    page = parse(url)

    #see if this is a split record. If so, return
    split = 0
    links = page.xpath('//div[@id="content"]/a/@href')
    if '/help/nom/split.html' in links:
        split = 1
        print "Split record at", url
    
    facts = page.xpath('//div[@id="content"]/comment()')
    d = re.match("(\d{4})(\d{2})(\d{2})", facts[6].text)
    d = "%s-%s-%s" % (d.group(1), d.group(2), d.group(3))
    info = {
        "congress": int(facts[2].text),
        "date": d,
        "name": facts[-4].text,
        "position": facts[-5].text,
        "state": facts[-6].text[2:]
    }
    
    facts = {}
    labels = page.xpath('//div[@id="content"]/span[@class="elabel"]')
    for label in labels:
        if label.tail:
            facts[label.xpath("text()")[0].strip()[:-1]] = label.tail.strip()
    print facts
    info["pin"] = facts["Nomination"]
    info["text"] = facts["Nominee"]
    if "Referred to" in facts:
        info["committee"] = facts["Referred to"]
    else:
        info["committee"] = ""
    info["organization"] = facts["Organization"]
    info["control_no"] = facts["Control Number"]

    if not dummy:
        c.execute('''INSERT OR IGNORE INTO nominations (pin, control_no, congress, date, name, state, text, position, organization, committee, split) VALUES (?,?,?,?,?,?,?,?,?,?,?)''', (info["pin"], info["control_no"], info["congress"], info["date"], info["name"], info["state"], info["text"], info["position"], info["organization"], info["committee"], split))

    #GET FLOOR ACTIONS
    actions = page.xpath("//dl/*")
    action_no = 0    
    for action in actions:
        d = action.xpath("comment()")[0].text
        d = re.match("(\d{4})(\d{2})(\d{2})", d)
        d = "%s-%s-%s" % (d.group(1), d.group(2), d.group(3))
        body = action.xpath("strong/text()")[0]
        report = action.xpath("text()")[1].split("-")[1:]
        report = "-".join(report).strip()
        if not dummy:
            c.execute('''INSERT OR IGNORE INTO actions (pin, action_no, date, type, action) VALUES (?,?,?,?,?)''', (info["pin"], action_no, d, body, report))
        action_no += 1
    conn.commit()


#for cong in range(110, 113):
#    get_congress(cong, 5000)
#get_congress(107, 5000)
get_nomination("http://thomas.loc.gov/cgi-bin/ntquery/D?nomis:27:./temp/~nomis3bwOfU::")
conn.close()
