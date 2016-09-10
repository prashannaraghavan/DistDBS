Mapper

Classname: EquiJoinMapper
Input Parameter : Object,Text
Output Parameter: Text, Text

Description:

The mapper gets the input from the text file uploaded to the HDFS.
The mapper checks the input text file line by line and parses
a record separated by delimiter ",". According to project 
specification the joincolumn(1st index) is stored as a key and
the whole tuple is stored as the value for the context.write function.

This is turn will group tuples with similar join column together and is sent
to the reducer for further processing.

Reducer

Classname:EquiJoinReducer
Input Parameter : Text,Text
Output Parameter: Text, Text

Description:

A list of text returned by the mapper is then manipulated using 
two for loops. The first for loop will iterate through all the 
tuples returned by the mapper for a particular join column. The second
for loop iterates from the 2 tuple until the last tuple of the mapper.
During the iteration, the tablenames are checked. If they are dissimilar
tuple from first for loop and tuple from second for loop is joined
and stored in hdfs using context.write fuction.

Driver
Method name: main()
Input Parameter : Input file path
Output Parameter: Output file path
Description:

This is the driver class which sets the configurations for the equijoin to run
in the hadoop environment.
A job object is created using Job.getInstance
and the classname is given as input to job.setJarByClass

Mapper and Reducer class are set to respective class names
using setMapperClass and setReducerClass

setOutputKeyClass and setOutputValueClass is both set to Text.

And then the input path and the output path is set using
FileInputFormat.addInputPath nad FileOutputFormat.setOutputPath

The program is then exited when the job is completed and the result 
received from the Reducer is stored into HDFS.

Steps to run the code

1.) Create a Java project in eclipse
2.) Clear the package name and name the project as equijoin. 
3.) After the implementation right click the project, click build path->configure build path->libraries->add external jars
4.) Add the following jars
	hadoop-2.6.0/share/hadoop/common/hadoop-common-2.6.0.jar
	hadoop-2.6.0/share/hadoop/mapreduce/hadoop-mapreduce-client-common-2.6.0.jar
	hadoop-2.6.0/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.0.jar
5.) Right click the project, export->Java->jar file
6.) Save it in the home directory
7.) Start hadoop and go to the hadoop-2.6.0/bin folder
8.) Execute the following commands. Assume input file is called input.txt in /home/hduser/input.txt
	./hadoop fs -mkdir -p input
	./hadoop fs -ls input/
	./hadoop dfs -put /home/hduser/input.txt input/
	./hadoop fs -cat hdfs://master:54310/user/hduser/input/input.txt
	./hadoop jar /home/hduser/equijoin.jar equijoin hdfs://master:54310/user/hduser/input hdfs://master:54310/user/hduser/output
