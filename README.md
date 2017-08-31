# Frequent Itemsets
Apache Spark Distributed Application

- Apache Spark application developed in Python to find frequent itemsets with minimum support ratio given transaction data.
- Implemented SON algorithm for finding frequent itemsets with Apriori algorithm as discovery algorithm for each chunk.

Core Technology: Apache Spark, Python, AWS (Amazon EC2 and Amazon S3).

# Execution
The program takes 3 arguments:
- Input file
- Minimum support ratio
- Output file

```
> bin/spark-submit son.py baskets.txt .3 output.txt
```

*baskets.txt* is a text file which contains a basket (a list of comma-separated item numbers) per line. For example

```
1,2,3
1,2,5
1,3,4
2,3,4
1,2,3,4
2,3,5
1,2,4
1,2
1,2,3
1,2,3,4,5
```

*.3* is a minimum support ratio (that is, for an itemset to be frequent, it should appear in at least 30% of the baskets).

*output.txt* is the name of output file


# Output
All frequent itemsets are written to the output file where each line of the file contains one itemset (a list of comma-separated item numbers). For example,

```
4
1,3,4
1,2,3
2
1,3
2,4
2,3
1
2,3,4
1,4
3
3,4
1,2,4
2,5
1,2
5
```
