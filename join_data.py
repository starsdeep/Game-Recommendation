#encoding=utf-8
__author__ = 'starsdeep'

import sys
reload(sys)
sys.setdefaultencoding('UTF8')
import MySQLdb


class DataJoinProcessor():

    online_pkg_dict = {}
    online_title_dict = {}
    ano_pkg_dict = {}
    wandoujia_title_dict = {}

    num_online = 0
    num_ano = 0
    num_wandoujia = 0
    num_joined = 0
    num_final_added = 0

    def __init__(self):
        pass


    def Strip(self,fields):
        for field in fields:
            if isinstance(field,unicode):
                field = field.strip()
        if len(fields[2]) <= 2:
            fields[2] =''
        
        if fields[1] == 'hjf.game.ncgy10':
            print 'title:' + fields[0]
            print 'pkgname:' + fields[1]
            print 'tag:'  + fields[2]
            print 'len of tag:' + str(len(fields[2]))

        return fields

    def Has_tags_and_description(self,fields):
        return len(fields[2].strip()) > 0  or len(fields[3].strip()) > 0


    def Input_online_data(self):
        try:
            conn = MySQLdb.connect(host='10.99.48.85', port=3308,user='app_vertical', passwd='CKD2agk2TBU3m', db='app_vertical', charset='utf8')
            cur = conn.cursor()
            self.num_online = cur.execute("select title,package_name,tags,description,download_count,comment_count,size,business_type,download_url from all_app where resource_type=1 and  title!='' and package_name!='' and download_url!='' ")
            self.online_pkg_dict = {item[1].strip(): self.Strip(list(item)) for item in cur.fetchall()}
        except Exception, e:
            print "[Error]:", e
        finally:
            cur.close()
            conn.close()
 

    def Input_annotation_data(self):
        try:
            conn = MySQLdb.connect(host='10.99.20.92', user='root', passwd='shenma123', db='app_platform', charset='utf8')
            cur = conn.cursor()
            self.num_ano = cur.execute("select app,android_pkg,tags from app_annotation  where app!='' and android_pkg!='' and tags!='' ")
            self.ano_pkg_dict = {item[1].strip(): self.Strip(list(item)) for item in cur.fetchall()}
        except Exception, e:
            print "[Error]:", e
        finally:
            cur.close()
            conn.close()
        print "a sample from ano_pkg_dict : " + str(self.ano_pkg_dict['org.cocos2d.fishingjoy3.uc'.decode('utf-8')])

    def Input_wandoujia_data(self):
        try:
            conn = MySQLdb.connect(host='10.99.20.92', user='root', passwd='shenma123', db='mobile_game', charset='utf8')
            cur = conn.cursor()
            self.num_wandoujia = cur.execute("select title,tag,description,download_num,comment_num,size,url from wandoujia_data_new")
            self.wandoujia_title_dict = {item[0]: list(item) for item in cur.fetchall()}
        except Exception, e:
            print "[Error]:", e
        finally:
            cur.close()
            conn.close()
       #print "a sample from wandoujia_title_dict : " + str(self.wandoujia_title_dict['捕鱼达人'])

    def Join_online_ano(self):
        for pkg,game in self.online_pkg_dict.items():
            if pkg in self.ano_pkg_dict:
                game[2] += '' + self.ano_pkg_dict[pkg][2]
                self.num_joined += 1
            #else:
            #    print pkg
            #    print 'pkg of online data:' + str(type(pkg))
            #    print 'pkg of ano data:' + str(type(pkg))
        #self.online_title_dict = {item[0]: list(item) for item in self.online_pkg_dict.values()}


    def Join_online_wandoujia(self):
        for title, game in self.wandoujia_title_dict.items():
            if title not in self.online_title_dict:
                #在插入数据之前需要对数据进行对齐
                game.insert(1,"")
                game.insert(7,0)
                self.online_title_dict[title] = game


    def Add_data_to_DB(self):
        try:
            conn = MySQLdb.connect(host='10.99.20.92', user='root', passwd='shenma123', db='mobile_game', charset='utf8')
            cur = conn.cursor()
            sqlcmd = "insert into all_game(title,pkg_name,tags,description,download_count,comment_count,size,business_type,url) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for game in self.online_pkg_dict.values():
                try:
                    if self.Has_tags_and_description(game):
                        self.num_final_added += 1
                        cur.execute(sqlcmd,tuple(game))
                except Exception, e:
                    print "[Error]:", e
                    print game[0]
                    sys.exit()
            conn.commit()
        except Exception, e:
            print "[Error]:", e
            sys.exit()
        finally:
            cur.close()
            conn.close()

    def PrintResult(self):
        print "Data input from online database:" + str(self.num_online)
        print "Data input from annotation database:" + str(self.num_ano)
        print "Data joined from online data and annotation data:" + str(self.num_joined)
        print "Data Added to dabase:" + str(self.num_final_added)


if __name__ == '__main__':
    s = DataJoinProcessor()
    s.Input_online_data()
    print "input online data ok"
    s.Input_annotation_data()
    print "input annotation data ok"
    s.Join_online_ano()
    print "join online data and annotation data ok"
    s.Add_data_to_DB()
    print "add data to database ok"
    print "Result:"
    s.PrintResult()
    

