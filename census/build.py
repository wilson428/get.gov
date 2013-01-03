import json, urllib, re, sqlite3, os
from collections import defaultdict

#returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

dir = os.getcwd() + "/"

conn = sqlite3.connect(dir + 'projections.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

#build table

c.execute('CREATE TABLE IF NOT EXISTS projections \
           ("id" INTEGER PRIMARY KEY AUTOINCREMENT, \
            "year" INTEGER, \
            "hispanic" VARCHAR(20), \
            "race" VARCHAR(10), \
            "sex" VARCHAR(10), \
            "age" INTEGER, \
            "number" INTEGER, \
            CONSTRAINT "unq" UNIQUE (year, hispanic, race, sex, age))')
conn.commit()

#CSV format: HISP,RACE,SEX,YEAR,TOTAL_POP,POP_[0-99],
#Description: http://www.census.gov/population/projections/files/filelayout/NP2012_D1.pdf
ORIGIN = [ "Total", "Not Hispanic", "Hispanic" ]
RACE = [ "All", "White", "Black", "AIAN", "Asian", "NHPI", "Two+", "White+", "Black+", "AIAN+", "Asian+", "NHPI+" ]
SEX  = [ "Both", "Male", "Female" ]

def get_data():
    url = "http://www.census.gov/population/projections/files/downloadables/NP2012_D1.csv"
    page = urllib.urlopen(url).read()

    projs = re.split("\n", page)
    print len(projs)

    for line_number in range(1, len(projs)): #first line is labels   
    #for line_number in range(1, 100): #first line is labels   
        line = projs[line_number].split(",")

        #total pop
        c.execute('''INSERT OR IGNORE INTO projections (year, hispanic, race, sex, age, number) VALUES (?,?,?,?,-1,?)''',
            (int(line[3]), ORIGIN[int(line[0])], RACE[int(line[1])], SEX[int(line[2])], int(line[4]) ))
            
        for age in range(101):
            c.execute('''INSERT OR IGNORE INTO projections (year, hispanic, race, sex, age, number) VALUES (?,?,?,?,?,?)''',
                (int(line[3]), ORIGIN[int(line[0])], RACE[int(line[1])], SEX[int(line[2])], age, int(line[age + 5]) ))
            
        if line_number % 100 == 0:
            conn.commit()
    conn.commit()
    
def races():
    f = open(dir + "demographics.csv", "w")
    f.write("year,white,whiteh,white+,black,blackh,black+,asian,asianh,asian+,hispanic,total\r\n")
    for y in range(2012, 2061):
        d = {
            'white': c.execute('''SELECT * from projections WHERE hispanic='Not Hispanic' AND race = 'White' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'whiteh': c.execute('''SELECT * from projections WHERE hispanic='Total' AND race = 'White' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'white+': c.execute('''SELECT * from projections WHERE hispanic='Total' AND race = 'White+' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'black': c.execute('''SELECT * from projections WHERE hispanic='Not Hispanic' AND race = 'Black' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'blackh': c.execute('''SELECT * from projections WHERE hispanic='Total' AND race = 'Black' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'black+': c.execute('''SELECT * from projections WHERE hispanic='Total' AND race = 'Black+' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'asian': c.execute('''SELECT * from projections WHERE hispanic='Not Hispanic' AND race = 'Asian' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'asianh': c.execute('''SELECT * from projections WHERE hispanic='Total' AND race = 'Asian' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'asian+': c.execute('''SELECT * from projections WHERE hispanic='Total' AND race = 'Asian+' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'hispanic': c.execute('''SELECT * from projections WHERE hispanic='Hispanic' AND race = 'All' AND sex = "Both" AND year = %i''' % y).fetchone()['number'],
            'total': c.execute('''SELECT * from projections WHERE hispanic='Total' AND race = 'All' AND sex = "Both" AND year = %i''' % y).fetchone()['number']            
        }
        f.write("%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i,%i\r\n" % (y,d['white'],d['whiteh'],d['white+'],d['black'],d['blackh'],d['black+'],d['asian'],d['asianh'],d['asian+'],d['hispanic'],d['total']))
    f.close()

l = lambda x: str(x['number'])
l2 = lambda x: str(x)


def get_all_byage():
    url = "http://www.census.gov/population/projections/files/downloadables/NP2012_D1.csv"
    page = urllib.urlopen(url).read()
    projs = re.split("\n", page)
    key = "0_0_0"
    demo = defaultdict(list)
    for line_number in range(1, len(projs)): #first line is labels   
    #for line_number in range(1, 200): #first line is labels   
        line = projs[line_number].split(",")
        try:
            mykey = "%s_%s_%s" % (line[0], line[1], line[2])
        except:
            mykey = ""
        if mykey != key or line_number == len(projs) - 1:
            print key
            f = open(dir + "/data/cohort_%s.csv" % key, 'w')
            for cohort in demo:
                f.write(str(cohort) + "," + ",".join(map(l2, demo[cohort])) + "\r\n")
            f.close()
            key = mykey
            demo = defaultdict(list)
        year = int(line[3])
        for i in range(5,len(line)):
            #demo[year - i + 5].append((year, int(line[i])))     
            demo[year - i + 5].append(int(line[i]))

def get_all():
    for h in range(2, len(ORIGIN)):
        for r in range(0, len(RACE)):
            for s in range(0, len(SEX)):
                print s
                f = open(dir + "data/%d_%d_%d.csv" % (h,r,s), "w")
                
                for age in range(101):
                    q = '''SELECT number from projections WHERE hispanic="%s" AND race = "%s" AND sex = "%s" AND age = %d AND year > 2000 ORDER BY year''' % (ORIGIN[h],RACE[r],SEX[s],age)
                    m = c.execute(q).fetchall()
                    o = "%d,%d,%d,%d,%s" % (h,r,s,age,",".join(map(l, m)))
                    f.write(o)
                f.close()
                print h,r,s
                print q

#get_all_byage()
count = 0
chars = 0
mx = 0
page = urllib.urlopen("http://www.census.gov/population/projections/files/downloadables/NP2012_D1.csv").read()
for line in re.split("\n", page):
    if line == "":
        break
    figs = map(int, re.findall("\d+", line))
    if max(figs) > mx:
        mx = max(figs)
    count += len(figs)
    chars += len(line)
print chars, count, mx

conn.commit()
conn.close()