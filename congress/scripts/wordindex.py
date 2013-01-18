import urllib, sqlite3, StringIO, json, os, re
from lxml.html import etree, parse
import datetime
from urlparse import urlparse
from collections import defaultdict

from nltk.stem.wordnet import WordNetLemmatizer
from nltk.stem.porter import PorterStemmer

cwd = os.getcwd()
lemmatize = WordNetLemmatizer()
stem = PorterStemmer()

#returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

conn = sqlite3.connect(cwd + '/congress/data/members.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

def get_index():
    concordance = defaultdict(list)
    for member in c.execute("SELECT * from bios").fetchall():
        for word in re.findall(r"\b[A-z\']+\b", member['bio'].lower()):
            #word = lemmatize.lemmatize(word)
            #word = stem.stem(word)            
            if len(word) > 2:# and word[0] == word[0].lower():
                concordance[word].append(member['pid'])

    '''
    words = concordance.keys()
    stems = [lemmatize.lemmatize(x) for x in words]
    print len(list(set(stems)))
    print len(list(set(words)))
    '''
    
    #remove duplicate members
    for word in concordance:
        concordance[word] = list(set(concordance[word]))#.sort()

    f = open(cwd + "/congress/data/word_index.json", 'w')
    f.write(json.dumps(concordance, sort_keys=True))
    f.close()

def get_phonebook():
    members = {}
    for member in c.execute("SELECT * from bios").fetchall():
        members[member['pid']] = {}
        for index in ["first", "last", "middle", "born", "died"]:
            members[member['pid']][index] = member[index]   
    
    f = open(cwd + "/congress/data/phonebook.json", 'w')
    f.write(json.dumps(members, sort_keys=True))
    f.close()    

def get_termbook():
    members = {}
    #for member in c.execute("select *, group_concat(congress) as sessions from terms group by pid").fetchall():
    for member in c.execute("select b.pid, b.first, b.last, b.middle, group_concat(congress) as sessions from terms as t LEFT JOIN bios as b ON t.pid = b.pid group by t.pid").fetchall():
        #members[member['pid']] = map(int, member['sessions'].split(","))
        members[member['pid']] = {
            'pid': member['pid'],
            'first': member['first'],
            'middle': member['middle'],
            'last': member['last'],
            'terms': map(int, member['sessions'].split(","))
        }
    f = open(cwd + "/congress/data/termbook.json", 'w')
    f.write(json.dumps(members, sort_keys=True))
    f.close()

def get_all_bios():
    f = open(cwd + "/congress/data/bios.txt", 'w')
    count = 0
    for member in c.execute("SELECT * FROM bios").fetchall():
        data = dict(member)
        data.pop("parsed")
        data.pop("nouns")
        count += 1
        f.write(json.dumps(data) + "\n")
    print count
    f.close()

def get_bios():
    includes = ["last", "first", "middle", "nickname", "born", "died", "party", "state", "position", "bio"]
    for session in range(1,114):
        f = open(cwd + "/congress/data/sessions/%s.json" % session, 'w')
        members = {}
        for member in c.execute("SELECT * FROM bios as b LEFT JOIN terms as t on b.pid = t.pid WHERE t.congress = %d" % session).fetchall():
            members[member["pid"]] = {}
            for include in includes: 
                members[member["pid"]][include] = member[include]
        f.write(json.dumps(members, indent=2, sort_keys=True))
        f.close()    

def reduce():
    index = json.load(open(cwd + "/congress/data/word_index.json", 'r'))
    c = 0
    print len(index.keys())
    while c < len(index.keys()):
        if len(index[index.keys()[c]]) < 2:
            #print index.keys()[c]
            index.pop(index.keys()[c])
        else:
            c += 1
    print len(index.keys())
    f = open(cwd + "/congress/data/wordindex_shorter.json", 'w')
    f.write(json.dumps(index, sort_keys=True))
    f.close()

#conn.commit()
conn.close()