'''readme of project'''

# PySpark Реализация связей продуктов и категорий

## Предположил такую структуру данных

Реализация использует следующие датафреймы:

- **products_df**: [id, name, description]
- **categories_df**: [id, name, parent_id]
- **relations_df**: [product_id, category_id]

## Функциональность

Метод `get_product_category_pairs` возвращает датафрейм со всеми парами "Имя продукта - Имя категории" и именами всех продуктов, у которых нет категорий.

## Запуск

```bash
pip install -r requirements.txt
python pyspark_solution.py
```
