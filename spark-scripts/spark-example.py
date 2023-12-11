import pyspark
from pandas import DataFrame, Series
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import col

sparkcontext = pyspark.SparkContext.getOrCreate(
    conf=(pyspark.SparkConf().setAppName("Dibimbing"))
)
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

# read data
retail_data = spark.read.csv('data/online-retail-dataset.csv', header=True)

# transform data
retail_data = retail_data.withColumn("InvoiceNo", col("InvoiceNo").cast("int"))
retail_data = retail_data.withColumn('InvoiceDate', F.to_timestamp('InvoiceDate', format='d/M/yyyy H:mm'))
retail_data = retail_data.withColumn("Quantity", col("Quantity").cast("int"))
retail_data = retail_data.withColumn("UnitPrice", col("UnitPrice").cast("decimal(10,2)"))
retail_data = retail_data.withColumn("CustomerID", col("CustomerID").cast("int"))

# Hitung penjualan terbanyak per bulan
retail_data.createOrReplaceTempView("revenue_data")
# Menghitung revenue per bulan dengan SQL
revenue_per_month = spark.sql("""
    SELECT 
        DATE_FORMAT(InvoiceDate, 'yyyy-MM') AS Month,
        SUM(Quantity * UnitPrice) AS total_revenue
    FROM 
        revenue_data
    WHERE 
        InvoiceDate IS NOT NULL
    GROUP BY 
        DATE_FORMAT(InvoiceDate, 'yyyy-MM')
    ORDER BY 
        Month
""")

# Menampilkan hasil perhitungan
revenue_per_month.show()

# save ke csv
revenue = revenue_per_month.toPandas()
revenue.to_csv("revenue.csv", index = False)

# rank produk dengan revenue terbanyak
rank_product = spark.sql("""
    SELECT 
        YEAR(InvoiceDate) AS year,
        MONTH(InvoiceDate) AS month,
        Description,
        SUM(Quantity * UnitPrice) AS total_revenue,
        RANK() OVER (PARTITION BY YEAR(InvoiceDate), MONTH(InvoiceDate) ORDER BY SUM(Quantity * UnitPrice) DESC) AS ranking
    FROM 
        revenue_data
    WHERE 
        InvoiceDate IS NOT NULL
    GROUP BY 
        YEAR(InvoiceDate),
        MONTH(InvoiceDate),
        Description
""")
# Menampilkan hasil perhitungan
rank_product.show()

# save ke csv
ranking_product_by_revenue = rank_product.toPandas()
ranking_product_by_revenue.to_csv("rank_product_revenue.csv", index = False)