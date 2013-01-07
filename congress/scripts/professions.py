import sqlite3, json, os, re
import nltk
#from nltk import word_tokenize

cwd = os.getcwd()

#returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

conn = sqlite3.connect(cwd + '/congress/data/members.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

f = open(cwd + "/congress/data/sample.json", 'w')

professions = {
    "agriculture": ["farm", "agricul"],
    "law": ["law", "bar"]
}

def ptag(profession):
    def repl(match):
        output = "<span class='%s'>%s</span>" % (profession, match.group(0))
        #print output
        return output
    return repl
    
output = []

for sample in c.execute("SELECT * FROM bios ORDER BY Random() LIMIT 5").fetchall():
    s = dict(sample)
    for p in professions:
        reg = r"\b(" + "|".join(professions[p]) + r")\b"
        #print reg
        s["bio"] = re.sub(reg, ptag(p), s["bio"].encode("ascii", "replace"))
    s["bio"] = s["bio"].replace(";", "<br />")
    output.append(s);

f.write(json.dumps(output, indent=2))

f.close()


    
    
#conn.commit()
conn.close()