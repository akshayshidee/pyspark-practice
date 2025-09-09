import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark=SparkSession.builder.appName("sparkexample.com").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

emp = [(1,"Smith",-1,"2018","10","M",3000), \
    (2,"Rose",1,"2010","20","M",4000), \
    (3,"Williams",1,"2010","10","M",1000), \
    (4,"Jones",2,"2005","10","F",2000), \
    (5,"Brown",2,"2010","40","",-1), \
      (6,"Brown",2,"2010","50","",-1) \
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined", \
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.show()

dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)

deptDF.show()

#####>>>>>> inner join (inner)
print("Inner join:")
empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "inner").show()

#####>>>>>> left join (left/leftouter/left_outer)
print("Left join:")
empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "left").show()

# empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "leftouter").show()

# empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "left_outer").show()

#####>>>>>> right join (right/rightouter/right_outer)
print("Right join:")
empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "right").show()

#####>>>>>> full join (outer/full/fullouter/full_outer)
print("Full join:")
empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "full").show()

#####>>>>>> left Semi join (left_semi/semi/leftsemi)
print("Left Semi join:")
empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "leftsemi").show()

#####>>>>>> left anti join (left_anti/anti/leftanti)
print("Left Anti join:")
empDF.join(deptDF, empDF.emp_dept_id==deptDF.dept_id, "leftsemi").show()

#######>>>>>self join -- nothing specific in sprak, but we can use above mentioned joins
print("Self Join:")
empDF.alias("emp1").join(empDF.alias("emp2"), \
    col("emp2.superior_emp_id")==col("emp1.emp_id")\
    ).select(col("emp1.emp_id"),col("emp1.name"),col("emp2.superior_emp_id"),col("emp2.name")).show()

