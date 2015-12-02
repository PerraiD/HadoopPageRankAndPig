# HadoopPageRankAndPig

###This repository provide three projects about pageRank :

* A crawler to get links from an url 
* PageRank algorithm implementation in Hadoop
* PageRank algorithm implementation in Hadoop Pig Latine embedded in JAVA

## The Crawler

The provided crawler require an url to crawl and return a file  with this format : 

```
URL RANK outgoingURL1,outgoingURL2,.....

```
the ouput file is store in _your project path_/output/output.txt

This file is used for pageRank projects (see under)

### Uses
---------------------
##PageRank With Hadoop

An implementation of PageRank in Hadoop

###Uses 
#### to see the code and compile it 
You need to have maven2.

* In the project directory execute this command to get the dependencies 
```
mvn install 
```
you can now import the project 
* If you use eclipse use this command to be able to import project
```
mvn eclipse:eclipse
```
#### Launch the project 

* You can configure the run application to launch the project into your IDE 
to do this your need to indicate Params arguments like :
```
PathToYourInputFile numberOfPageRankingTurn
/home/user/document/input.txt 4
```
* Otherwise you can use the JAR file with Hadoop library like 
(remember that _mvn install_ import the hadoop library, so execution under the ide is more easy) 

The program take to argument :

1. Path to the input File (See crawler)
2. number of turn for the page rank 

In a terminal launch : 
```
./PathToHadoopLibrary/bin/hadoop.sh jar PathtoHadoopPageRank/ PageRankMapReduce PathToInputFile numberOfPageRankTurn
```
