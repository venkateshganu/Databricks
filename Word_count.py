text_file = sc.textFile("FileStore/tables/sample.txt")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("FileStore/tables/sample27.txt")
#display(dbutils.fs.ls("FileStore/tables/sample12.txt/part-00000"))
sqlContext.read.format("csv").load("/FileStore/tables/sample14.txt/").show()