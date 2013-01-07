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

output = []

patterns = [
    ["date", r"((Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.? \d+[ \d]{0,5})"],
    ["date", r"\b\d{4}\b"],
]

for sample in c.execute("SELECT * FROM bios ORDER BY Random() LIMIT 5").fetchall():
    biolines = []
    for bioline in sample["bio"].replace("\n", " ").split(';'):
        #get parts of speech for each phrase, and make mutable        
        pos = map(list, nltk.pos_tag(nltk.word_tokenize(bioline)))
        print pos
        sentence = []
        w = 0
        while w < len(pos) - 1:
            #if next word has same grammar type as this word (including plurals)
            if pos[w][1] == pos[w + 1][1] or pos[w][1] + "S" == pos[w + 1][1] or pos[w][1] == pos[w + 1][1] + "S":
                pos[w][0] += " " + pos[w + 1][0]
                pos.pop(w + 1)
                '''
            if w < len(pos) - 2 and pos[w + 1][0] == "of" and pos[w][1] == pos[w + 2][1] or pos[w][1] + "S" == pos[w + 2][1] or pos[w][1] == pos[w + 2][1] + "S":
                pos[w][0] += " " + pos[w + 1][0]
                pos.pop(w + 1)
                pos[w][0] += " " + pos[w + 1][0]
                pos.pop(w + 1)      
                '''
            else:
                w += 1
        biolines.append(" ".join(['<span class="%s">%s</span> ' % (x[1], x[0]) for x in pos]))
    o = dict(sample)
    o["biolines"] = biolines
    print o["biolines"]
    output.append(o);

f.write(json.dumps(output, indent=2))

f.close()


    
    
#conn.commit()
conn.close()