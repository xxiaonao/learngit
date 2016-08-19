
from pyspark import SparkContext,SparkConf

conf=SparkConf().setAppName("simple App")
sc = SparkContext(conf=conf)

def turnoverrate(a,b):
    return float(len(b-a))/float(len(a))


line1=sc.textFile("s3://bilin-data/hadoop/data_log/trace/20160701/*/")\
        .map(lambda line : line.split("\t"))\
        .filter(lambda line:(len(line)==13) and (line[2]=="207" or line[2]=="209"))
line2=sc.textFile("s3://bilin-data/hadoop/data_log/trace/20160702/*/")\
        .map(lambda line : line.split("\t"))\
        .filter(lambda line:(len(line)==13) and (line[2]=="207" or line[2]=="209"))
line3=sc.textFile("s3://bilin-data/hadoop/data_log/trace/20160703/*/")\
        .map(lambda line : line.split("\t"))\
        .filter(lambda line:(len(line)==13) and (line[2]=="207" or line[2]=="209"))
line4=sc.textFile("s3://bilin-data/hadoop/data_log/trace/20160704/*/")\
        .map(lambda line : line.split("\t"))\
        .filter(lambda line:(len(line)==13) and (line[2]=="207" or line[2]=="209"))
line5=sc.textFile("s3://bilin-data/hadoop/data_log/trace/20160705/*/")\
        .map(lambda line : line.split("\t"))\
        .filter(lambda line:(len(line)==13) and (line[2]=="207" or line[2]=="209"))
line6=sc.textFile("s3://bilin-data/hadoop/data_log/trace/20160706/*/")\
        .map(lambda line : line.split("\t"))\
        .filter(lambda line:(len(line)==13) and (line[2]=="207" or line[2]=="209"))
line7=sc.textFile("s3://bilin-data/hadoop/data_log/trace/20160707/*/")\
        .map(lambda line : line.split("\t"))\
        .filter(lambda line:(len(line)==13) and (line[2]=="207" or line[2]=="209"))

id1=line1.map(lambda line : (line[4],line[3]))\
         .groupByKey()\
         .mapValues(lambda line: set(line)).filter(lambda line :len(line[1])>5)
id2=line2.map(lambda line : (line[4],line[3]))\
         .groupByKey()\
         .mapValues(lambda line: set(line))
id3=line3.map(lambda line : (line[4],line[3]))\
         .groupByKey()\
         .mapValues(lambda line: set(line))
id4=line4.map(lambda line : (line[4],line[3]))\
         .groupByKey()\
         .mapValues(lambda line: set(line))
id5=line5.map(lambda line : (line[4],line[3]))\
         .groupByKey()\
         .mapValues(lambda line: set(line))
id6=line6.map(lambda line : (line[4],line[3]))\
         .groupByKey()\
         .mapValues(lambda line: set(line))
id7=line7.map(lambda line : (line[4],line[3]))\
         .groupByKey()\
         .mapValues(lambda line: set(line))

idcount1=id1.mapValues(lambda line: len(line))
idcount2=id2.mapValues(lambda line: len(line))
idcount3=id3.mapValues(lambda line: len(line))
idcount4=id4.mapValues(lambda line: len(line))
idcount5=id5.mapValues(lambda line: len(line))
idcount6=id6.mapValues(lambda line: len(line))
idcount7=id7.mapValues(lambda line: len(line))

rate1=id1.join(id2).mapValues(lambda x: turnoverrate(x[0],x[1]))
rate2=id1.join(id3).mapValues(lambda x: turnoverrate(x[0],x[1]))
rate3=id1.join(id4).mapValues(lambda x: turnoverrate(x[0],x[1]))
rate4=id1.join(id5).mapValues(lambda x: turnoverrate(x[0],x[1]))
rate5=id1.join(id6).mapValues(lambda x: turnoverrate(x[0],x[1]))
rate6=id1.join(id7).mapValues(lambda x: turnoverrate(x[0],x[1]))

output=idcount1.join(idcount2).join(idcount3).join(idcount4).join(idcount5).join(idcount6).join(idcount7)\
               .join(rate1).join(rate2).join(rate3).join(rate4).join(rate5).join(rate6).saveAsTextFlie("pixel_ip_turnoverrate")
#               .mapValues(lambda line :str(line[0][0][0][0][0][0][0][0][0][0][0][0])+"\t"+str(line[0][0][0][0][0][0][0][0][0][0][0][1])+"\t"+str(line[0][0][0][0][0][0][0][0][0][0][1])+"\t"+str(line[0][0][0][0][0][0][0][0][0][1])+"\t"+str(line[0][0][0][0][0][0][0][0][1])+"\t"+str(line[0][0][0][0][0][0][0][1])+"\t"+str(line[0][0][0][0][0][0][1])+"\t"+str(line[0][0][0][0][0][1])+str(line[0][0][0][0][1])+"\t"+str(line[0][0][0][1])+"\t"+str(line[0][0][1])+"\t"+str(line[0][1])+"\t"+str(line[1]))\
#               .map(lambda line :line[0]+"\t"+line[1])\
#               .saveAsTextFlie("pixel_ip_turnoverrate")
