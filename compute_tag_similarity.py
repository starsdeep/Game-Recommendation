#encoding=utf-8
__author__ = 'starsdeep'

import sys,os
import MySQLdb
reload(sys)
sys.setdefaultencoding('UTF8')
curPath = os.path.abspath(os.path.split(os.path.realpath(__file__))[0])
sys.path.append(curPath + "/../common/")
sys.path.append(curPath + "/../filter/")
from StringTool import StringTool
import math
from collections import defaultdict


class TagSimilarityProcessor():

    num_all_game = 0
    pkg_game_dict = {}    #pkg -> game_list
    pkg_similarity_dict = {} # pkg -> dict(other:similarity)
    tag_pkgs_dict = defaultdict(set) # tag -> set(urls)
    tag_weight_dict = {}



    def __init__(self):
        pass

    def cleanData(self,fields):
        for field in fields:
            if isinstance(field,unicode):
                field.strip()
        if len(fields[2]) <= 2:
            fields[2] = ''
        return fields

    def dataOk(self, fields):
        for field in fields:
            if len(field) < 1:
                return False
        return True


    def inputData(self):
        try:
            conn = MySQLdb.connect(host='10.99.20.92', user='root', passwd='shenma123', db='mobile_game', charset='utf8')
            cur = conn.cursor()
            self.num_all_game = cur.execute("select title,pkg_name,tags,url from all_game where title!='' and pkg_name!='' and tags!='' and url!='' ")
            for item in cur.fetchall():
                item = self.cleanData(list(item))
                if not self.dataOk(item):
                    continue
                #input game
                self.pkg_game_dict[item[1]] = item
                #input tags
                item[2].replace('、', '\007')
                for tag in item[2].split('\007'):
                    tag = StringTool.normalizedStr(tag.strip())
                    if len(tag) > 0:
                        self.tag_pkgs_dict.setdefault(tag, {})
                        self.tag_pkgs_dict[tag].add(item[1])

        except Exception, e:
            print "[Error]:", e
        finally:
            cur.close()
            conn.close()


    def computeTagWeight(self):
        self.tag_weight_dict = {tag: len(pkgs) for tag,pkgs in self.tag_pkgs_dict}

    def computeSingleSimilarity(self,tags1, tags2):
        return sum([self.tag_weight_dict[tag] * self.tag_weight_dict[tag] for tag in tags1 & tags2])

    def computeAllSimilarity(self):
        for pkg,game in self.tag_pkgs_dict():
            pkg_similarity_dict = {other: self.computeSingleSimilarity(pkg,other) for other,game in self.tag_pkgs_dict.items() if pkg!=other}


    def saveResult(self,outputfile):
        of = open(outputfile,'w')

        for pkg, game in self.pkg_game_dict.items():






if __name__ == '__main__':
    if len(sys.argv) != 2:
        print "<output_file>"
        sys.exit()
    s = TagSimilarityProcessor()
    s.inputData()
    s.computeTagWeight()
    s.computeAllSimilarity()
    s.saveResult(sys.argv[1])