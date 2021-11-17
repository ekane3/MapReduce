# YARN & MapReduce 2
## *Emile .E. EKANE*
## *28/10/2021*


<br><br>

# 1. MapReduce JAVA
In this tutorial, we will use write MapReduce jobs using JAVA. Before that, we
will install prerequirements, if you already have them, skip thoses parts
<br><br>

### 1.1 Install OpenJDK 8
### 1.2 Git
Download and install 64-bit Git for Windows Setup from [here.](https://git-scm.com/download/win)
### 1.3 IntelliJ IDEA
### 1.4 Git
Clone the project.Get link [here](https://github.com/makayel/hadoop-examples-mapreduce)
### 1.5 Import the project
Generate the JAR using maven lifecycle **package**.

On the bottom of the window, you will see the building process, wait for an
[INFO] BUILD SUCCESS message
    IMAGE
You will see a new folder package, inside this folder you will find the JAR. We
will use the JAR *-with-dependencies.jar
    IMAGE

### 1.6 Send the JAR to the edge node

Download and Install FileZilla for Windows Connect to edge node using those parameters : 
- Host: sftp://hadoop-edge01.efrei.online
- Username: ssh username
- Password: ssh password
- Port: 22

You can slide files from left (your directories) to the right (edge node)

Let's try to run a job precisely the wordcount job.

Before that, we upload the jar to edge

We rename the file and finally run the wordcount job.

```sh
[emile.ekane-ekane@hadoop-edge01 ~]$ ls
57333-0.txt.1  hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar  message.txt  secret-of-the-universe.txt  trashtext.txt
bonjour.txt    local.txt                                                         outline.txt  sudoku.dta                  ulysse.txt
gut.txt        mapper.py                                                         reducer.py   text.txt                    vinci.txt
[emile.ekane-ekane@hadoop-edge01 ~]$ mv hadoop-examples-mapreduce-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop-examples-mapreduce-coded.jar
[emile.ekane-ekane@hadoop-edge01 ~]$ ls
57333-0.txt.1  gut.txt                            local.txt  message.txt  reducer.py                  sudoku.dta  trashtext.txt  vinci.txt
bonjour.txt    hadoop-examples-mapreduce-coded.jar  mapper.py  outline.txt  secret-of-the-universe.txt  text.txt    ulysse.txt
[emile.ekane-ekane@hadoop-edge01 ~]$ hdfs dfs -put hadoop-examples-mapreduce-coded.jar

```

```sh
yarn jar  hadoop-examples-mapreduce-coded.jar wordcount /user/emile.ekane-ekane/trashtext.txt /user/emile.ekane-ekane/trashtextCount

[emile.ekane-ekane@hadoop-edge01 ~]$ hdfs dfs -ls /user/emile.ekane-ekane/trashtextCount
Found 2 items
-rw-r--r--   3 emile.ekane-ekane emile.ekane-ekane          0 2021-10-28 13:49 /user/emile.ekane-ekane/trashtextCount/_SUCCESS
-rw-r--r--   3 emile.ekane-ekane emile.ekane-ekane         35 2021-10-28 13:49 /user/emile.ekane-ekane/trashtextCount/part-r-00000
[emile.ekane-ekane@hadoop-edge01 ~]$ hdfs dfs -tail /user/emile.ekane-ekane/trashtextCount/part-r-00000
jsujujujsjhdjdfkjhqdhkbfqjqkdbgf        1

```

## 1.7 Remarkable trees of Paris

We are going to write some MapReduce jobs on the remarkable trees of Paris using this [dataset](https://github.com/makayel/hadoop-examples-mapreduce/blob/main/src/test/resources/data/trees.csv).

Let's download the file and put it in our HDFS home directory.

```
> hdfs dfs -put trees.csv 


[emile.ekane-ekane@hadoop-edge01 ~]$ hdfs dfs -put trees.csv
[emile.ekane-ekane@hadoop-edge01 ~]$ hdfs dfs -ls
Found 18 items
drwx------   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-28 14:00 .Trash
drwx------   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-28 13:50 .staging
drwxr-xr-x   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-18 17:06 NewDir
drwxr-xr-x   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-24 12:12 data
drwxr-xr-x   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-24 19:04 gutenberg
drwxr-xr-x   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-27 08:26 gutenberg-output
drwxr-xr-x   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-27 10:43 gutenberg-outputs
-rw-r--r--   3 emile.ekane-ekane emile.ekane-ekane   58977084 2021-10-28 13:37 hadoop-examples-mapreduce-1-Emile
-rw-r--r--   2 emile.ekane-ekane emile.ekane-ekane        540 2021-10-24 19:45 mapper.py
drwxr-xr-x   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-28 09:23 raw
-rw-r--r--   2 emile.ekane-ekane emile.ekane-ekane       1053 2021-10-24 19:45 reducer.py
-rw-r--r--   2 emile.ekane-ekane emile.ekane-ekane        162 2021-10-21 14:15 sudoku.dta
-rw-r--r--   2 emile.ekane-ekane emile.ekane-ekane     798774 2021-10-21 13:41 text.txt
-rw-r--r--   3 emile.ekane-ekane emile.ekane-ekane         33 2021-10-28 13:45 trashtext.txt
drwxr-xr-x   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-28 13:49 trashtextCount
-rw-r--r--   3 emile.ekane-ekane emile.ekane-ekane      16778 2021-10-28 14:16 trees.csv
drwxr-xr-x   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-21 13:49 wordcount
drwxr-xr-x   - emile.ekane-ekane emile.ekane-ekane          0 2021-10-28 13:28 wordcountt

```

> Remember to ignore the first row in every mapper on this dataset.

## 1.7.1 Districts containing trees (very easy)
Write a MapReduce job that displays the list of distinct containing trees in this
file. Obviously, it’s twenty or less different arrondissements, but exactly how
many?
Distinct.java
```java
package com.opstty.job;

import com.opstty.mapper.DistrictMapper;
import com.opstty.reducer.DistrictReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class District {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: distinct Districts <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "district");
        job.setJarByClass(District.class);
        job.setMapperClass(DistrictMapper.class);
        job.setCombinerClass(DistrictReducer.class);
        job.setReducerClass(DistrictReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```

DistinctMapper.java
```java
public class DistrictMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    public int line = 0 ;

    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        if ( line!= 0 ){
            context.write(new IntWritable(Integer.parseInt(value.toString().split(";")[1])),new IntWritable(1));
        }
        line++;
    }

}

```

DistinctReducer.java
```java
public class DistrictReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}
```

Results by running ``yarn jar  hadoopDistinct.jar district trees.csv /user/emile.ekane-ekane/jobs/distinct``

```sh
[emile.ekane-ekane@hadoop-edge01 ~]$  hdfs dfs -ls /user/emile.ekane-ekane/jobs/distinct
Found 2 items
-rw-r--r--   3 emile.ekane-ekane emile.ekane-ekane          0 2021-11-04 10:37 /user/emile.ekane-ekane/jobs/distinct/_SUCCESS
-rw-r--r--   3 emile.ekane-ekane emile.ekane-ekane         80 2021-11-04 10:37 /user/emile.ekane-ekane/jobs/distinct/part-r-00000
[emile.ekane-ekane@hadoop-edge01 ~]$  hdfs dfs -cat /user/emile.ekane-ekane/jobs/distinct/part-r-00000
3       1
4       1
5       2
6       1
7       3
8       5
9       1
11      1
12      29
13      2
14      3
15      1
16      36
17      1
18      1
19      6
20      3
```

## 1.7.2 Show all existing species (very easy)
Write a MapReduce job that displays the list of different species trees in this
file.

Species.java
```java
import com.opstty.mapper.SpeciesMapper;
import com.opstty.reducer.SpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Species {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Trees species <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "species");
        job.setJarByClass(Species.class);
        job.setMapperClass(SpeciesMapper.class);
        job.setCombinerClass(SpeciesReducer.class);
        job.setReducerClass(SpeciesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

SpeciesMapper.java
```java
public class SpeciesMapper extends Mapper<Object, Text, Text, NullWritable> {

    public int line = 0 ;

    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        if ( line!= 0 ){
            context.write(new Text(value.toString().split(";")[3]),NullWritable.get());
        }
        line++;
    }

}
```

SpeciesReducer.java
```java
public class SpeciesReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
```
Results by running ``yarn jar  hadoopDistinct.jar species trees.csv /user/emile.ekane-ekane/jobs/distinct``
```sh
[emile.ekane-ekane@hadoop-edge01 ~]$  hdfs dfs -cat /user/emile.ekane-ekane/jobs/species/part-r-00000
araucana
atlantica
australis
baccata
bignonioides
biloba
bungeana
cappadocicum
carpinifolia
colurna
coulteri
decurrens
dioicus
distichum
excelsior
fraxinifolia
giganteum
giraldii
glutinosa
grandiflora
hippocastanum
ilex
involucrata
japonicum
kaki
libanii
monspessulanum
nigra
nigra laricio
opalus
orientalis
papyrifera
petraea
pomifera
pseudoacacia
sempervirens
serrata
stenoptera
suber
sylvatica
tomentosa
tulipifera
ulmoides
virginiana
x acerifolia

```


## 1.7.3 Number of trees by kinds (easy)
Write a MapReduce job that calculates the number of trees of each kinds
(species, district, etc.) in this file.

Treeskinds.java
```java
public class Treeskind {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Number trees by kind <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "treeskind");
        job.setJarByClass(Treeskind.class);
        job.setMapperClass(TreeskindMapper.class);
        job.setCombinerClass(TreeskindReducer.class);
        job.setReducerClass(TreeskindReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

TreeskindsMapper.java
```java
public class TreeskindMapper extends Mapper<Object, Text, Text, IntWritable> {

    public int line = 0 ;

    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        if ( line!= 0 ){
            context.write(new Text(value.toString().split(";")[3]),new IntWritable(1));
        }
        line++;
    }

}

```
TreeskindsReducer.java
```java
public class TreeskindReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    int sum = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        for (IntWritable val: values){
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
```
Results by running ``yarn jar  hadoopDistinct.jar treeskind trees.csv /user/emile.ekane-ekane/jobs/treeskinds``
```sh
[emile.ekane-ekane@hadoop-edge01 ~]$ hdfs dfs -cat  /user/emile.ekane-ekane/results/treeskind/part-r-00000
araucana        1
atlantica       4
australis       8
baccata 14
bignonioides    21
biloba  33
bungeana        46
cappadocicum    60
carpinifolia    78
colurna 99
coulteri        121
decurrens       144
dioicus 168
distichum       195
excelsior       223
fraxinifolia    253
giganteum       288
giraldii        324
glutinosa       361
grandiflora     399
hippocastanum   440
ilex    482

```


## 1.7.4 Maximum height per kind of tree (average)
MaxHeightjob.java
```java	
public class Maxheightkind {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Max Height per kind <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "maxheightkind");
        job.setJarByClass(Maxheightkind.class);
        job.setMapperClass(MaxHeightKindMapper.class);
        job.setCombinerClass(MaxHeightKindReducer.class);
        job.setReducerClass(MaxHeightKindReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```
Mapper.java
```java

public class MaxHeightKindMapper extends Mapper<Object, Text, Text, FloatWritable> {

    public int line = 0 ;

    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        if ( line!= 0 ){

            try{
                //Lets get the heights of the trees
                Float hauteur = Float.parseFloat(value.toString().split(";")[6]);
                context.write(new Text(value.toString().split(";")[3]),new FloatWritable(hauteur));
            }catch(NumberFormatException ex) {
                // System.out.println(""+ ex);
            }

        }
        line++;
    }

}
```
Reducer.java
```java
public class MaxHeightKindReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(key, new FloatWritable(StreamSupport.stream(values.spliterator(), false)
                .map((v) -> { return v.get(); })
                .max(Float::compare).get()));
    }
}

```

Results by running ``yarn jar  hadoopDistinct.jar maxheightkind trees.csv /user/emile.ekane-ekane/jobs/maxheightkind``
```sh
[emile.ekane-ekane@hadoop-edge01 ~]$ hdfs dfs -head /user/emile.ekane-ekane/results/maxheightkind/part-r-00000
araucana        9.0
atlantica       25.0
australis       16.0
baccata 13.0
bignonioides    15.0
biloba  33.0
bungeana        10.0
cappadocicum    16.0
carpinifolia    30.0
colurna 20.0
coulteri        14.0
decurrens       20.0
dioicus 10.0
distichum       35.0
excelsior       30.0
fraxinifolia    27.0
giganteum       35.0
giraldii        35.0
glutinosa       16.0
grandiflora     12.0
hippocastanum   30.0
ilex    15.0
involucrata     12.0
japonicum       10.0
kaki    14.0
libanii 30.0
monspessulanum  12.0
nigra   30.0
nigra laricio   30.0
opalus  15.0
orientalis      34.0
papyrifera      12.0
petraea 31.0
pomifera        13.0
pseudoacacia    11.0
sempervirens    30.0
serrata 18.0
stenoptera      30.0
suber   10.0
sylvatica       30.0
tomentosa       20.0
tulipifera      35.0
ulmoides        12.0
virginiana      14.0
x acerifolia    45.0

```
## 1.7.5 Sort the trees height from smallest to largest (average)
job.java
```java
public class SortTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Sort Trees Height from Smallest to Largest <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "sortTrees");
        job.setJarByClass(SortTrees.class);
        job.setMapperClass(SortTreesMapper.class);
        //job.setCombinerClass(SortTreesReducer.class);
        job.setReducerClass(SortTreesReducer.class);
        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```

Mapper.java
```java
public class SortTreesMapper extends Mapper<Object, Text, FloatWritable, Text> {

    public int line = 0 ;

    public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
        if ( line!= 0 ){

            try{
                String[] line_tokens = value.toString().split(";");
                Float height = Float.parseFloat(line_tokens[6]);
                context.write(new FloatWritable(height),
                        new Text(line_tokens[11] + " - " + line_tokens[2] + " " + line_tokens[3] + " (" + line_tokens[4] + ")"));
            }catch(NumberFormatException ex) {
                // System.out.println(""+ ex);
            }

        }
        line++;
    }

}

```

Reducer.java
```java
public class SortTreesReducer extends Reducer<FloatWritable,Text, Text, FloatWritable> {
    public void reduce(FloatWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text val: values){
            context.write(val,key);
        }
    }
}

```

Results by running ``yarn jar  hadoopDistinct.jar sortTrees trees.csv /user/emile.ekane-ekane/jobs/sort``
```
[emile.ekane-ekane@hadoop-edge01 ~]$ hdfs dfs -cat /user/emile.ekane-ekane/results/sort/part-r-00000
3 - Fagus sylvatica (Fagaceae)  2.0
89 - Taxus baccata (Taxaceae)   5.0
62 - Cedrus atlantica (Pinaceae)        6.0
39 - Araucaria araucana (Araucariaceae) 9.0
44 - Styphnolobium japonicum (Fabaceae) 10.0
32 - Quercus suber (Fagaceae)   10.0
95 - Pinus bungeana (Pinaceae)  10.0
61 - Gymnocladus dioicus (Fabaceae)     10.0
63 - Fagus sylvatica (Fagaceae) 10.0
4 - Robinia pseudoacacia (Fabaceae)     11.0
93 - Diospyros virginiana (Ebenaceae)   12.0
66 - Magnolia grandiflora (Magnoliaceae)        12.0
50 - Zelkova carpinifolia (Ulmaceae)    12.0
7 - Eucommia ulmoides (Eucomiaceae)     12.0
48 - Acer monspessulanum (Sapindacaees) 12.0
58 - Diospyros kaki (Ebenaceae) 12.0
33 - Broussonetia papyrifera (Moraceae) 12.0
71 - Davidia involucrata (Cornaceae)    12.0
36 - Taxus baccata (Taxaceae)   13.0
6 - Maclura pomifera (Moraceae) 13.0
68 - Diospyros kaki (Ebenaceae) 14.0
96 - Pinus coulteri (Pinaceae)  14.0
94 - Diospyros virginiana (Ebenaceae)   14.0
91 - Acer opalus (Sapindaceae)  15.0
5 - Catalpa bignonioides (Bignoniaceae) 15.0
70 - Fagus sylvatica (Fagaceae) 15.0
2 - Ulmus carpinifolia (Ulmaceae)       15.0
98 - Quercus ilex (Fagaceae)    15.0
28 - Alnus glutinosa (Betulaceae)       16.0
78 - Acer cappadocicum (Sapindaceae)    16.0
75 - Zelkova carpinifolia (Ulmaceae)    16.0
16 - Celtis australis (Cannabaceae)     16.0
64 - Ginkgo biloba (Ginkgoaceae)        18.0
83 - Zelkova serrata (Ulmaceae) 18.0
23 - Aesculus hippocastanum (Sapindaceae)       18.0
60 - Fagus sylvatica (Fagaceae) 18.0
34 - Corylus colurna (Betulaceae)       20.0
51 - Platanus x acerifolia (Platanaceae)        20.0
43 - Tilia tomentosa (Malvaceae)        20.0
15 - Corylus colurna (Betulaceae)       20.0
11 - Calocedrus decurrens (Cupressaceae)        20.0
1 - Corylus colurna (Betulaceae)        20.0
8 - Platanus orientalis (Platanaceae)   20.0
20 - Fagus sylvatica (Fagaceae) 20.0
35 - Paulownia tomentosa (Paulowniaceae)        20.0
12 - Sequoiadendron giganteum (Taxodiaceae)     20.0
87 - Taxodium distichum (Taxodiaceae)   20.0
13 - Platanus orientalis (Platanaceae)  20.0
10 - Ginkgo biloba (Ginkgoaceae)        22.0
47 - Aesculus hippocastanum (Sapindaceae)       22.0
86 - Platanus orientalis (Platanaceae)  22.0
14 - Pterocarya fraxinifolia (Juglandaceae)     22.0
88 - Liriodendron tulipifera (Magnoliaceae)     22.0
18 - Fagus sylvatica (Fagaceae) 23.0
24 - Cedrus atlantica (Pinaceae)        25.0
31 - Ginkgo biloba (Ginkgoaceae)        25.0
92 - Platanus x acerifolia (Platanaceae)        25.0
49 - Platanus orientalis (Platanaceae)  25.0
97 - Pinus nigra (Pinaceae)     25.0
84 - Ginkgo biloba (Ginkgoaceae)        25.0
73 - Platanus orientalis (Platanaceae)  26.0
65 - Pterocarya fraxinifolia (Juglandaceae)     27.0
42 - Platanus orientalis (Platanaceae)  27.0
85 - Juglans nigra (Juglandaceae)       28.0
76 - Pinus nigra laricio (Pinaceae)     30.0
19 - Quercus petraea (Fagaceae) 30.0
72 - Sequoiadendron giganteum (Taxodiaceae)     30.0
54 - Pterocarya stenoptera (Juglandaceae)       30.0
29 - Zelkova carpinifolia (Ulmaceae)    30.0
27 - Sequoia sempervirens (Taxodiaceae) 30.0
25 - Fagus sylvatica (Fagaceae) 30.0
41 - Platanus x acerifolia (Platanaceae)        30.0
77 - Taxodium distichum (Taxodiaceae)   30.0
55 - Platanus x acerifolia (Platanaceae)        30.0
69 - Pinus nigra (Pinaceae)     30.0
38 - Fagus sylvatica (Fagaceae) 30.0
59 - Sequoiadendron giganteum (Taxodiaceae)     30.0
52 - Fraxinus excelsior (Oleaceae)      30.0
37 - Cedrus libanii (Pinaceae)  30.0
22 - Cedrus libanii (Pinaceae)  30.0
30 - Aesculus hippocastanum (Sapindaceae)       30.0
80 - Quercus petraea (Fagaceae) 31.0
9 - Platanus orientalis (Platanaceae)   31.0
82 - Platanus x acerifolia (Platanaceae)        32.0
46 - Ginkgo biloba (Ginkgoaceae)        33.0
45 - Platanus orientalis (Platanaceae)  34.0
56 - Taxodium distichum (Taxodiaceae)   35.0
81 - Liriodendron tulipifera (Magnoliaceae)     35.0
17 - Platanus x acerifolia (Platanaceae)        35.0
53 - Ailanthus giraldii (Simaroubaceae) 35.0
57 - Sequoiadendron giganteum (Taxodiaceae)     35.0
26 - Platanus x acerifolia (Platanaceae)        40.0
74 - Platanus x acerifolia (Platanaceae)        40.0
40 - Platanus x acerifolia (Platanaceae)        40.0
90 - Platanus x acerifolia (Platanaceae)        42.0
21 - Platanus x acerifolia (Platanaceae)        45.0

```
## 1.7.6 District containing the oldest tree (difficult)
OldestTreeDistrict.java
```java
public class OldestTreeDistrict {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: OldestTreeDistrict <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "oldestTreeDistrict");
        job.setJarByClass(OldestTreeDistrict.class);
        job.setMapperClass(OldestTreeDistrictMapper.class);
        //job.setCombinerClass(SortTreesReducer.class);
        job.setReducerClass(OldestTreeDistrictReducer.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

```	

Mapper.java
```java
public class OldestTreeDistrictMapper extends Mapper<Object, Text, NullWritable, MapWritable> {

    public int curr_line = 0;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (curr_line != 0) {
            try {
                Integer year = Integer.parseInt(value.toString().split(";")[5]);
                MapWritable map = new MapWritable();
                map.put(new IntWritable(Integer.parseInt(value.toString().split(";")[1])), new IntWritable(year));
                context.write(NullWritable.get(), map);
            } catch (NumberFormatException ex) {
                // If the year is not a integer, skip by catching the error from the parseFloat() method
            }
        } curr_line++;
    }

}
```

Reducer.java
```java
public class OldestTreeDistrictReducer extends Reducer<NullWritable, MapWritable, IntWritable, IntWritable> {
    public void reduce(NullWritable key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {

        ArrayList<Integer[]> district_years = (ArrayList<Integer[]>) StreamSupport.stream(values.spliterator(), false)
                .map( mw ->  (new Integer[] { ((IntWritable) mw.keySet().toArray()[0]).get(), ((IntWritable) mw.get(mw.keySet().toArray()[0])).get() }))
                .collect(Collectors.toList());
        // Copies the iterable to an arraylist so multiple operations can be done on the iterable

        int min_year = district_years.stream().map((arr) -> arr[1]).min(Integer::compare).get();

        district_years.stream().filter(arr -> arr[1] == min_year).map(arr -> arr[0]).distinct().forEach((district) -> { try {
            context.write(new IntWritable(min_year), new IntWritable(district));
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        } });
    }
}

```

Results by running ``yarn jar  hadoop.jar oldestTreeDistrict trees.csv /user/emile.ekane-ekane/jobs/oldest``
```
1601	5
1601	3
```

## 1.7.7 District containing the most trees (very difficult)
DistrictMaxTrees.java
```java
public class DistrictMaxTrees {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: Districtmaxtrees <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "districtmaxtrees");
        job.setJarByClass(DistrictMaxTrees.class);
        job.setMapperClass(DistrictMaxTreesMapper.class);
        //job.setCombinerClass(SortTreesReducer.class);
        job.setReducerClass(DistrictMaxTreesReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```
Mapper  .java
```java
public class DistrictMaxTreesMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

    public int curr_line = 0;
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if (curr_line != 0) {
            context.write(new IntWritable(Integer.parseInt(value.toString().split(";")[1])), new IntWritable(1));
        }
        curr_line++;
    }
}
```

Reducer.java
```java
public class DistrictMaxTreesReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public ArrayList<Integer[]> sum_districts = new ArrayList<Integer[]>();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        sum_districts.add(new Integer[] {key.get(), sum });
    }

    public void cleanup(Context context) {
        int max = sum_districts.stream().map(arr -> arr[1]).max(Integer::compare).get();
        sum_districts.stream().filter(arr -> arr[1] == max)
                .forEach(arr -> {
                    try {
                        context.write(new IntWritable(arr[0]), new IntWritable(max));
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                });
    }
}

```
Results by running ``yarn jar  hadoopDistinct.jar districtmaxtrees trees.csv /user/emile.ekane-ekane/jobs/MaxDistrict``
```
16	36
```	


Global `AppDriver.java` code
```java
public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,"A map/reduce program that counts the words in the input files.");
            programDriver.addClass("district", District.class ,"A map/reduce program that displays the list of distinct containing trees in this file.");
            programDriver.addClass("species", Species.class ,"A map/reduce program that displays  the list of different species trees in this file.");
            programDriver.addClass("treeskind", Treeskind.class,"A map/reduce program that calculates the number of trees of each kinds");
            programDriver.addClass("maxheightkind", Maxheightkind.class,"A map/reduce program that  calculates the height of the tallest tree of each kind.");
            programDriver.addClass("sortTrees", SortTrees.class,"A map/reduce program that sort the trees height from smallest to largest.");
            programDriver.addClass("oldestTreeDistrict", OldestTreeDistrict.class,"A map/reduce program that returns the district(s) with the oldest tree(s) in the Remarkable Trees of Paris dataset, using a sort.");
            programDriver.addClass("districtmaxtrees", DistrictMaxTrees.class,"A map/reduce program that returns the district(s) with the most trees in the Remarkable Trees of Paris dataset, checking through all the data, using the Reducer's cleanup.");

            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
```	