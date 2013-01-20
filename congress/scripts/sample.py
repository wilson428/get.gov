import sqlite3, json, os, re
import nltk
from collections import defaultdict
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
    
c.execute('''
    CREATE TABLE IF NOT EXISTS jobs
       ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
        "pid" VARCHAR(64),
        "job" VARCHAR(100),
        "verb" VARCHAR(64),
        CONSTRAINT "unq" UNIQUE (pid, job, verb))'''
    )    
    
f = open(cwd + "/congress/data/sample.json", 'w')

grammar = r"""
   DATE: {<NNP><CD><,><CD>}
   DATE: {<CD>}
   PNP:  {<NNP.*>+}   # chunk determiner/possessive, adjectives and nouns
   PNP:  {<PNP>+<,|CC|TO|DT|IN>+<PNP>+}
   PNP:  {<PNP>+<,|CC|TO|DT|IN>+<PNP>+}
   CNP:  {<JJ.*>?<NN.*>+ }"""

cp = nltk.RegexpParser(grammar)
    
def condense(parsed):
    sentence = []
    for n in parsed:
        if isinstance(n, nltk.tree.Tree): 
            sentence.append((" ".join([x[0] for x in n.leaves()]), n.node))
        else:
            sentence.append(n)
    return sentence

def extract(bio): 
    insights = []
    biolines = []
    for phrase in re.sub("\(.*?\)", "", bio['bio'].encode("ascii", "replace").replace("\n", " ").replace("-", " - ")).split(";"):
        if len(phrase.strip().split(" ")) == 1:
            #biolines.append("<span class='CNP'>%s</span>" % phrase.strip())
            insights.append(("", phrase.strip()))
            continue
        parsed = condense(cp.parse(nltk.pos_tag(nltk.word_tokenize(phrase))))
        verb = ""
        for n in parsed:
            if re.match("VB.*?", n[1]):
                verb = n[0]
            elif n[1] == "CNP":
                insights.append((verb, n[0]))
        
        #biolines.append(" ".join(["<span class='%s'>%s</span>" % (x[1], x[0]) for x in parsed]))
    return insights

def sample(M, N):
    #get five random bios for testing
    #f = open(cwd + "/congress/data/sample.json", 'w')
    output = []
    #for sample in c.execute("SELECT * FROM bios ORDER BY Random() LIMIT 5").fetchall():
    for sample in c.execute("SELECT pid, bio FROM bios LIMIT %d OFFSET %d" % (M, N)).fetchall():
        print sample['pid']
        biolines = []
        insights = []
        for phrase in re.sub("\(.*?\)", "", sample['bio'].encode("ascii", "replace").replace("\n", " ").replace("-", " - ")).split(";"):
            if len(phrase.strip().split(" ")) == 1:
                biolines.append("<span class='CNP'>%s</span>" % phrase.strip())
                insights.append(("", phrase.strip()))
                continue
            parsed = condense(cp.parse(nltk.pos_tag(nltk.word_tokenize(phrase))))
            verb = ""
            for n in parsed:
                if re.match("VB.*?", n[1]):
                    verb = n[0]
                elif n[1] == "CNP":
                    insights.append((verb, n[0]))
            
            biolines.append(" ".join(["<span class='%s'>%s</span>" % (x[1], x[0]) for x in parsed]))
            for insight in insights:
                c.execute('''INSERT OR IGNORE INTO jobs (pid, job, verb) VALUES (?,?,?)''', (sample['pid'], insight[0], insight[1]))

        '''
        o = dict(sample)
        o["biolines"] = biolines
        o["insights"] = insights
        #print o["biolines"]
        output.append(o);
        '''
    conn.commit()
    #f.write(json.dumps(output, indent=2))    
    #f.close()

co = 8000;
while co < 13000:
    sample(10, co + 10)
    print co
    co += 10
    
   
    
#conn.commit()
conn.close()