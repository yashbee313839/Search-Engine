import urllib2
import sqlite3 as sqlite
from bs4 import BeautifulSoup
from urlparse import urljoin
import re

ignr=set(['the','of','to','and','a','in','is','it'])

class crawler:
    def __init__(self, dbname):
        self.con=sqlite.connect(dbname)
    def __del__(self):
        self.con.close()
    def dbcommit(self):
        self.con.commit()
    def getentryid(self, table, field, value, createnew=True):
        cur=self.con.execute("select rowid from %s where %s='%s'"%(table, field, value))
        res=cur.fetchone()
        if res==None:
            cur=self.con.execute("insert into %s (%s) values ('%s')"%(table, field, value))
            return cur.lastrowid
        else:
            return res[0]

    def addtoindex(self,url,soup):
        if self.isindexed(url): return
        print 'Indexing %s'%url
        text=self.gettextonly(soup)
        words=self.saperatewords(text)
        urlid=self.getentryid('urllist', 'url', url)

        for i in range(len(words)):
            word=words[i]
            if word in ignr: continue
            wordid=self.getentryid('wordlist', 'word', word)
            self.con.execute("insert into wordlocation(urlid, wordid, location) \
                    values(%d, %d, %d)"%(urlid, wordid, i))

    def gettextonly(self, soup):
        v=soup.string
        #print soup.contents
        if v == None:
            c=soup.contents
            txtr=''
            for t in c:
                subtext=self.gettextonly(t)
                txtr +=subtext+'\n'
            return txtr
        else:
            return v.strip()

    def saperatewords(self, text):
        splitter=re.compile('\\W*')
        return [s.lower(  ) for s in splitter.split(text) if s!='']

    def isindexed(self, url):
        cur=self.con.cursor()
        u=cur.execute("select rowid from urllist where url='%s'"%url).fetchone
        u=cur.fetchone()
        if u!=None:
            v=cur.execute('select * from wordlocation where urlid=%d'%u[0])
            if v!=None : return True
        return False

      def addlinkref(self,urlFrom,urlTo,linkText):
        words=self.separatewords(linkText)
        fromid=self.getentryid('urllist', 'url', urlForm)
        toid=self.getentryid('urllist', 'url', urlTo)
        if fromid == toid: return
        cur=self.con.execute("insert into link(fromid,toid) values (%d,%d)"% (fromid,toid))
        linkid=cur.lastrowid
        for word in words:
            wordid=self.getentryid('worldlist', 'word', word)
            self.con.execute("insert into linkwords(linkid, wordid) values (%d, %d)"%(linkid, wordid))

    def crawl(self,pages,depth=2):
        for i in range(depth):
            newpages=set( )
            for page in pages:
                try:
                    c=urllib2.urlopen(page)
                except:
                    print "Could not open %s"%page
                    continue
                soup=BeautifulSoup(c.read( ), 'html.parser')
                self.addtoindex(page,soup)
                links=soup('a')
                for link in links:
                    ur= dict(link.attrs)
                    if(ur.get('href')):
                        url=urljoin(page, link['href'])
                        if url[0:4]=='http' and not self.isindexed(url):
                            newpages.add(url)
                    self.dbcommit( )
            pages=newpages

    def createindextables(self):
        self.con.execute('create table urllist(url)')
        self.con.execute('create table wordlist(word)')
        self.con.execute('create table wordlocation(urlid,wordid,location)')
        self.con.execute('create table link(fromid integer,toid integer)')
        self.con.execute('create table linkwords(wordid,linkid)')
        self.con.execute('create index wordidx on wordlist(word)')
        self.con.execute('create index urlidx on urllist(url)')
        self.con.execute('create index wordurlidx on wordlocation(wordid)')
        self.con.execute('create index urltoidx on link(toid)')
        self.con.execute('create index urlfromidx on link(fromid)')


class searcher:
    def __init__(self, dbname):
        self.con=sqlite.connect(dbname)

    def __del__(self):
        self.con.close()

    def getmatchrows(self, q):
        fieldlist='w0.urlid'
        tablelist=''
        clauselist=''
        wordids=[]

        words=q.split(' ')
        tn=0

        for word in words:
            cur=self.con.execute("select rowid from wordlist where word='%s'"%word)
            wordrow=cur.fetchone()
            if wordrow != None:
                wordid=wordrow[0]
                wordids.append(wordid)
                if tn>0:
                    tablelist+=','
                    clauselist+='and'
                    clauselist+='w%d.urlid=w%d.urlid and '%(tn-1,tn)
                fieldlist+=',w%d.location'%tn
                tablelist+='wordlocation w%d'%tn
                clauselist+='w%d.wordid=%d'%(tn,wordid)
                tn+=1
        fullquery='select %s from %s where %s'%(fieldlist,tablelist,clauselist)
        cur=self.con.execute(fullquery)
        rows=[row for row in cur]
        return rows,wordids

    def frequencyscore(self, rows):
        counts=dict([(row[0], 0) for row in rows])
        print counts
        for row in rows: counts[row[0]] += 1
        return self.normalizescore(counts)

    def locationscore(self,rows):
        locations=dict([(row[0],1000000) for row in rows])
        for row in rows:
            loc=sum(row[1:])
            if loc<locations[row[0]]: locations[row[0]]=loc
        return self.normalizescores(locations,smallIsBetter=1)
    
    def getscoredlist(self, rows, wordids):
        totalscores={}
        totalscores=dict([(row[0], 0) for row in rows])
        weights= [(1.0, self.frequencyscore(rows))]
        for (weight, scores) in weights:
            for url in totalscores:
                totalscores[url] += weight * scores[url]
        return totalscores

    def geturlname(self, _id):
        cur=self.con.execute("select url from urllist where rowid=%d"%_id)
        return cur.fetchone()
    
    def query(self, q):
        rows, wordids=self.getmatchrows(q)
        scores=self.getscoredlist(rows, wordids)
        rankedscores=sorted([(score,url) for (url,score) in scores.items(  )],reverse=1)
        for (score, urlid) in rankedscores[0:10]:
            print '%f\t%s'%(score, self.geturlname(urlid))


    def normalizescore(self, scores, smallIsBetter=0):
        vsmall=0.00001
        if smallIsBetter:
            minscore=min(scores.values())
            return dict([(u, float(minscore)/max(vsmall,l)) for (u,l) in scores.items()])
        else:
            maxscore=max(scores.values())
            if maxscore==0: maxscore=vsmall
return dict([(u, float(c)/maxscore) for (u,c) in scores.items()])