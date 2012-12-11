import json, re, sqlite3, csv, os
from collections import defaultdict

cwd = os.getcwd()

#returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

#categorize the action as a stage in the nomination process based on keywords
def categorize(text):
    if re.match("^Confirmed", text):
        if "Voice Vote" in text:
            return { "action" : "confirmed", "subject": "voice" }
        else:
            vote = re.search("Yea-Nay Vote. (\d+) - (\d+)", text)
            return { "action" : "confirmed" }#, "subject": (vote.group(1), vote.group(2)) }
    if re.search("returned to the president", text.lower()):
        return { "action": "returned", "subject": "" }
    if "withdrawal" in text.lower():
        return { "action": "withdrawn", "subject" : "" }
    if "reject" in text.lower() or "disapprov" in text.lower():
        return {"action": "rejected", "subject" : "" }
    #workaround for Ruth Bader Ginsburg
    if "additional views filed" in text.lower():
        return {"action": "confirmed", "subject" : "" }
    #print text
    return { "action": "unknown", "subject" : "" }
    
#connect to SQLite db
conn = sqlite3.connect(cwd + '/nominations/data/nominations.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

nominations = []

#http://en.wikipedia.org/wiki/Cabinet_of_the_United_States
def isbigdeal(nomination):
    #if re.match("^(Deputy )?Secretary", nomination['position']):
    if re.search("^Secretary", nomination['position']) and re.search("Department", nomination['organization']):
        return "Cabinet"
    if re.search("^attorney general$", nomination['position'].lower()):
        return "Cabinet"
    if re.search("Supreme Court|Chief Justice", nomination['text']):
        return "Supreme Court"
    if re.search("Circuit Judge", nomination['text']):
        return "Judges"
    if re.match("^United States Attorney", nomination['position']):
        return "U.S. Attorneys"
    '''
    if re.search("^Director", nomination['position']) and not re.match("^Department of", nomination['organization']):
        return "Executive"
    '''
    return False

def write_data(cong):
    print cong
    nominations = []
    for nomination in c.execute("SELECT * FROM nominations WHERE congress = %d ORDER BY date" % cong).fetchall():
        #print nomination['name']
        if isbigdeal(nomination):
            actions =  c.execute("SELECT * FROM actions WHERE pin = '%s' order by action_no" % nomination['pin']).fetchall()
            if len(actions) <= 1:
                continue
            first, last = actions[0], actions[-1]

            nominations.append({
                'name': nomination['name'],
                'pin': nomination['pin'],
                'position': nomination['position'],
                'organization': nomination['organization'],
                'category': isbigdeal(nomination),
                'start': { "date": first['date'], "text": first['action'] },
                'end' : { "date": last['date'], "text": last['action'], 'result': categorize(last['action'])}
            })

    f = open(cwd + "/nominations/data/congress/%d.json" % cong, 'w')
    f.write(json.dumps(nominations, indent=2))
    f.close()
    
def sankey(cong):
    print cong
    #categories of categories
    categories = {}
    nodes = []
    links = []
    data = { "nodes": [], "links": [] }
    for nomination in c.execute("SELECT * FROM nominations WHERE congress = %d ORDER BY date" % cong).fetchall():
        #see if this nomination is worth counting, according to isbigdeal function
        category = isbigdeal(nomination)
        if category:
            if category == "Supreme Court":
                print nomination['name'].encode('ascii', 'replace'), nomination['position'], category
            nodes.append(category) if category not in nodes else False
            if category not in categories:
                categories[category] = defaultdict(list)
            actions =  c.execute("SELECT * FROM actions WHERE pin = '%s' order by action_no" % nomination['pin']).fetchall()
            if len(actions) <= 1:
                continue
            first, last = actions[0], actions[-1]
            result = categorize(last['action'])['action'];
            if result == "unknown":
                continue
            nodes.append(result) if result not in nodes else False
            name = nomination["name"].split(",")[1] + " " + nomination["name"].split(",")[0]            
            categories[category][result].append([name, nomination["position"]])
            
    for node in nodes:
        data["nodes"].append({ 'name': node })

    for category in categories:
        for result in categories[category]:
            #print category, result
            link = {
                'category': category,
                'result': result,
                'source': nodes.index(category),
                'target': nodes.index(result),
                'value': len(categories[category][result]),
                'names': categories[category][result]
            }
            data["links"].append(link)
            
    f = open(cwd + "/nominations/data/congress/links_%s.json" % cong, 'w')
    f.write(json.dumps(data, indent=2))
    f.close()    

#for cong in range(100, 113):
#    sankey(cong)

sankey(103)

conn.close()
