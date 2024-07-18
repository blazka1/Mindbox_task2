from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, coalesce

# Создание сессии Spark
spark = SparkSession.builder.appName("ProductCategory").getOrCreate()

# Пример данных для продуктов
products = spark.createDataFrame([
    (1, "Product1"),
    (2, "Product2"),
    (3, "Product3"),
], ["product_id", "product_name"])

# Пример данных для категорий
categories = spark.createDataFrame([
    (1, "Category1"),
    (2, "Category2"),
], ["category_id", "category_name"])

# Пример данных для связей продуктов и категорий
product_categories = spark.createDataFrame([
    (1, 1),
    (1, 2),
    (2, 1),
], ["product_id", "category_id"])

# Соединяем продукты с категориями через таблицу связей
product_category_pairs = products \
    .join(product_categories, "product_id", "left_outer") \
    .join(categories, "category_id", "left_outer") \
    .select(products.product_name, categories.category_name)

# Находим продукты без категорий
products_without_categories = products \
    .join(product_categories, "product_id", "left_outer") \
    .filter(col("category_id").isNull()) \
    .select("product_name").distinct()

# Объединяем результаты: пары "продукт-категория" и продукты без категорий
# Создаем колонку с null значением для продуктов без категорий и объединяем оба датафрейма
result = product_category_pairs \
    .union(products_without_categories.withColumn("category_name", lit(None))) \
    .select(col("product_name"), col("category_name"))

# Отображаем результат
result.show()
