Spark Learning Notes
========================

Steps to start Spark programming
==================================

1. SparkConf conf = new SparkConf().setAppName("Starting Spark").setMaster("local[*]");

Expl:
>Via this line we are setting/loading the Spark Configuration first.
> setAppName("Starting Spark") --> Just sets the Application name, which will appear in reports
> setMaster("local[*]")  ---> means we are setting the context to run SPARK IN LOCAL MODE, i.e. we DONOT HAVE A CLUSTER yet.
[*] : indicates that USE ALL CORES AVAILABLE in the machine. If we donot specify *, Spark will run on a SINGLE THREAD and we will NOT GET FULL Spark Performance.



2.  JavaSparkContext sc = new JavaSparkContext(conf);

Expl: This line helps us get connected to a Spark cluster. Though we dont have cluster now this will give us a handle to local spark context for now.


After these 2 lines, we have Spark instantiated. Now we can run Spark commands, eg load data, map reduce etc etc....



3. JavaRDD<Double> myRdd = sc.parallelize(inputData);

Expl: With "parallelize", we load input data.
Eg: Here inputData is a list of Double, so here essentially we are asking Spark to load the list and create an RDD.

N.B. Here we have JavaRDD, allows us to use ordinary Java methods on this RDD.
Actually Spark is written in Scala, and Spark has RDDs implemented in Scala. Here we are using JavaRDD which is a wrapper object on the Scala RDD,
just so that we can use regular Java methods.



************************************************************************* 
**************************    REDUCE function  **************************
*************************************************************************

Lets say we have a list of Double as:

List<Double> inputData = new ArrayList<>();
inputData.add(35.5);
inputData.add(40.2);
inputData.add(58.23);
inputData.add(25.63);
inputData.add(45.63);
inputData.add(78.221);
inputData.add(98.36);

Intention: By Reduce we want to sum them all up.

Q. How Reduce works ??

******* Lec: 7 Very important **********

Q. Why we use lamba functions in Reduce ?

A. This is beacuse the data is spread across multiple JVMs. So if we run a normal loop or something that will work on a single JVM. It CANNOT span across multiple JVMs.
But if we have the operation defined as lambda functions, then driver sends this operation to the nodes to perform individual reduce operations parallely
and the finally it accumulates all results via yet another reduce process. (Lec: 7)

> Function2<Double,Double,Double> ==> means we need to supply a Function which takes in 2 Doubles and the output is also a Double.

> Remember: The return type of a reduce function should always be same as that of the inputs. So if we need a different return type, we need to look to something other than reduce.

Example:
>  Double result = myRdd.reduce((value1, value2) -> value1 + value2); --> we need not specify types in lambda function since its already inferred from the JavaRDD<Double> line above.

Sysout(result)  ==> see o/p


************************************************************************* 
**************************   Mapping function  **************************
*************************************************************************

Intention: Lets say we have a list of integers eg:

List<Double> inputData = new ArrayList<>();
inputData.add(35);
inputData.add(12);
inputData.add(90);
inputData.add(20);

in an RDD. So now via mapping we are going to send the lambda function of say "sqrt(value)"

									  RDD-1							RDD-2
_________________________			_______                       __________
|                        |          |      |					 |          |
| function = sqrt(value) |          |  35  |------ sqrt(35) -----| 5.91608  |
|________________________|          |      |                     |          |
|  12  |------ sqrt(12) -----| 3.46410  |
|      |                     |          |
|  90  |------ sqrt(90) -----| 9.48683  |
|      |                     |          |
|  20  |------ sqrt(20) -----| 4.47214  |  
|      |                     |          |
|______|                     |__________|


> So here above by the mapping lambda function: sqrt(value) we want to take the square root of the integers in RDD1 and get the corresponding sqrt values in RDD-2.

> Now RDD is ALWAYS IMMUTABLE, so we cannot modify RDD-1.

> So via Mapping we will be getting another 2nd RDD i.e. RDD-2

> Also note that via MAPPING function, the datatype of o/p and i/p CAN BE different. (unlike reduce).


Eg:

//5. Mapping function
JavaRDD<Double> sqrtRdd = myRdd.map(value -> Math.sqrt(value));
sqrtRdd.foreach(val -> System.out.println("sqrt: "+val));


//6. How many elements in sqrtRdd  -------------> count() function
System.out.println("count: "+sqrtRdd.count());


//Displaying count with map and reduce
long singleIntRdd = sqrtRdd.map(value -> 1L).reduce((val1, val2) -> val1 + val2);
System.out.println("count: "+singleIntRdd);


>> sqrtRdd.collect().forEach(System.out::println);

If we use:  sqrtRdd.forEach(System.out::println) ==> We get Not Serializable error.
> This is because though we are running on local, still Spark works like distributed fashion, i.e. the sqrtRdd can be spread across multiple machines/node.
So Spark through foreach method tries to send System.out::println function to all the nodes. But in Java System.out::println is NOT SERIALIZABLE.
This happens if we have multiple CPUs.

> So in this case we need to use collect() method first: sqrtRdd.collect() ===> which collects all nodes data and reduces to a simple Java List. on which we can now use forEach()

N.B. Spark method is foreach  ===> with lowercase 'e'.
Java method is forEach   ===> with uppercase 'E'.


****************************************
**************   Tuple  **************
***************************************
Requirement:

Say we have inputData as a List:

List<Integer> inputData = new ArrayList<>();
inputData.add(45);
inputData.add(25);
inputData.add(9);
inputData.add(2);
inputData.add(144);

// Req: --> Say we want o/p like: val,sqrt(val) eg: (25,5)
> We can create a Java class like:

class ValWithSqRt{

	private int val;
	private double sqrt;

    public ValWithSqRt(int val,double sqrt){
    	this.val=val;
    	this.sqrt = Math.sqrt(val);

    }
}
public String toString(){
return Sting.valueOf(val)+" "+Sting.valueOf(sqrt);
}

then use this as:

ValWithSqRt valWithSqRt = new ValWithSqRt(25);
Sysout(valWithSqRt.toString());
& use this object in a loop for all members of input data


==> But for requirement like this it is much better sybtactically to use Tuple as:

JavaRdd<Integer> integerRdd = sc.parallelize(inputData);
JavaRDD<Tuple2<Integer,Double>> valWithSqrt = integerRdd.map(val -> new Tuple2(val, Math.sqrt(val)));

new Tuple2(val, Math.sqrt(val)) :  This is Tuple2 and o/p when printed as:
================================
valWithSqrt.collect.forEach(System.out::println);
O/P:
(45,6.708203932499369)
(25,5.0)
(9,3.0)
(2,1.4142135623730951)
(144,12.0)


//Tuple3 Demo
// Req: --> Say we want o/p like: val,sqrt(val),val+sqrt(val) eg: (25,5,30)
JavaRDD<Tuple3> tuple3DemoRdd = integersRDD.map(value -> new Tuple3(value, Math.sqrt(value), value + Math.sqrt(value)));
tuple3DemoRdd.collect().stream().forEach(System.out::println);

O/p:
(45,6.708203932499369,51.70820393249937)
(25,5.0,30.0)
(9,3.0,12.0)
(2,1.4142135623730951,3.414213562373095)
(144,12.0,156.0)


****************************************
**************  PairRDD  **************
***************************************
> Allows to hold key:value pair.

> Intuition like Java Map : Difference is it allows Duplicate Keys eg:

WARN :  Monday 7th April
ERROR :  Tuesday 8th April
FATAL :  Tuesday 8th April
ERROR :  Wednesday 9th Ma
WARN :  Friday 10h April

So, we see we have 2 WARN, 2 ERROR as keys.

4. sc.close();  ==> Close Spark context finally.




