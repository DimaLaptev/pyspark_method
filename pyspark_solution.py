from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def get_product_category_pairs(products_df: DataFrame, categories_df: DataFrame, relations_df: DataFrame) -> DataFrame:
    """
    Args:
        products_df: DataFrame с колонками [id, name, description]
        categories_df: DataFrame с колонками [id, name, parent_id]  
        relations_df: DataFrame с колонками [product_id, category_id]
    
    Returns:
        DataFrame с колонками [product_name, category_name]
        где category_name может быть NULL для продуктов без категорий
    """
    
    # LEFT JOIN products с relations для получения всех продуктов 
    # (включая те, у которых нет связей с категориями)
    products_with_relations = products_df.alias("p") \
        .join(
            relations_df.alias("r"), 
            col("p.id") == col("r.product_id"), 
            "left"
        )
    
    # LEFT JOIN с categories для получения названий категорий
    # Продукты без категорий будут иметь NULL в category_name
    result = products_with_relations \
        .join(
            categories_df.alias("c"),
            col("r.category_id") == col("c.id"),
            "left"
        ) \
        .select(
            col("p.name").alias("product_name"),
            col("c.name").alias("category_name")
        )
    
    return result


def get_product_category_pairs_alternative(products_df: DataFrame, categories_df: DataFrame, relations_df: DataFrame) -> DataFrame:
    """
    Альтернативная реализация с более явным разделением логики.
    Args:
        products_df: DataFrame с колонками [id, name, description]
        categories_df: DataFrame с колонками [id, name, parent_id]
        relations_df: DataFrame с колонками [product_id, category_id]
    
    Returns:
        DataFrame с колонками [product_name, category_name]
    """
    
    # Получаем пары продукт-категория для продуктов с категориями
    products_with_categories = products_df.alias("p") \
        .join(relations_df.alias("r"), col("p.id") == col("r.product_id"), "inner") \
        .join(categories_df.alias("c"), col("r.category_id") == col("c.id"), "inner") \
        .select(
            col("p.name").alias("product_name"),
            col("c.name").alias("category_name")
        )
    
    # Получаем продукты без категорий
    products_without_categories = products_df.alias("p") \
        .join(relations_df.alias("r"), col("p.id") == col("r.product_id"), "left_anti") \
        .select(
            col("p.name").alias("product_name"),
            col("null").cast("string").alias("category_name")
        )
    
    # Объединяем результаты
    result = products_with_categories.union(products_without_categories)
    
    return result


# Пример использования и тестирования
def example_usage():

    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    
    spark = SparkSession.builder.appName("ProductCategoryPairs").getOrCreate()
    
    # Products DataFrame
    products_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True)
    ])
    
    products_data = [
        (1, "iPhone", "Смартфон Apple"),
        (2, "MacBook", "Ноутбук Apple"),
        (3, "Молоко", "Молочный продукт"),
        (4, "Хлеб", "Хлебобулочное изделие"),
        (5, "Мышь", "Компьютерная мышь")  # продукт без категории
    ]
    
    products_df = spark.createDataFrame(products_data, products_schema)
    
    # Categories DataFrame
    categories_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("parent_id", IntegerType(), True)
    ])
    
    categories_data = [
        (1, "Электроника", None),
        (2, "Продукты питания", None),
        (3, "Смартфоны", 1),
        (4, "Компьютеры", 1),
        (5, "Молочные продукты", 2)
    ]
    
    categories_df = spark.createDataFrame(categories_data, categories_schema)
    
    # Relations DataFrame
    relations_schema = StructType([
        StructField("product_id", IntegerType(), True),
        StructField("category_id", IntegerType(), True)
    ])
    
    relations_data = [
        (1, 3),  # iPhone -> Смартфоны
        (1, 1),  # iPhone -> Электроника (многие ко многим)
        (2, 4),  # MacBook -> Компьютеры
        (2, 1),  # MacBook -> Электроника
        (3, 5),  # Молоко -> Молочные продукты
        (3, 2),  # Молоко -> Продукты питания
        (4, 2)   # Хлеб -> Продукты питания
        # Мышь (id=5) намеренно не имеет связей
    ]
    
    relations_df = spark.createDataFrame(relations_data, relations_schema)
    
    # Тестируем основной метод
    result = get_product_category_pairs(products_df, categories_df, relations_df)
    
    print("Результат основного метода:")
    result.show()
    
    # Тестируем альтернативный метод
    result_alt = get_product_category_pairs_alternative(products_df, categories_df, relations_df)
    
    print("\nРезультат альтернативного метода:")
    result_alt.show()
    
    spark.stop()


if __name__ == "__main__":
    example_usage() 