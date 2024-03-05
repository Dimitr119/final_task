# Аналіз продажів телевізорів

**Запитання:** В якому штаті було куплено найбільше телевізорів покупцями від 20 до 30 років за першу декаду вересня?

**SQL-запит:**
```sql
WITH RelevantPurchases AS (
  SELECT 
    s.client_id,
    s.product_name,
    s.purchase_date
  FROM `de-07-dmytro-shpatakovskyi.silver.sales` s
  JOIN `de-07-dmytro-shpatakovskyi.gold.customers` c ON s.client_id = c.client_id
  WHERE s.product_name = 'TV'
    AND s.purchase_date BETWEEN '2022-09-01' AND '2022-09-10'
    AND EXTRACT(YEAR FROM s.purchase_date) - EXTRACT(YEAR FROM c.birth_date) BETWEEN 20 AND 30
    AND DATE_DIFF(s.purchase_date, c.birth_date, YEAR) BETWEEN 20 AND 30
)

SELECT 
  c.state, 
  COUNT(*) as total_sales
FROM RelevantPurchases rp
JOIN `de-07-dmytro-shpatakovskyi.gold.customers` c ON rp.client_id = c.client_id
GROUP BY c.state
ORDER BY total_sales DESC
LIMIT 1;
```

**Результат:**
- **Штат:** Idaho
- **Всього продаж:** 1432


## DAGs Overview

### Process Sales DAG
[![Process Sales DAG](Снимок экрана 2024-03-05 в 10.39.41.png)](https://files.oaiusercontent.com/file-lPxWkIgivD9KKiW0zvBf4D4L?se=2024-03-05T09%3A49%3A31Z&sp=r&sv=2021-08-06&sr=b&rscc=max-age%3D299%2C%20immutable&rscd=attachment%3B%20filename%3Dimage.png&sig=O8WtyLHEVBrkLh%2BSreyOymaZxdYJS7DeIfNsjk8l1sA%3D)

### Process Customers DAG
![Process Customers DAG](Снимок экрана 2024-03-05 в 10.40.45.png)

### Process User Profiles Pipeline
![Process User Profiles Pipeline](Снимок экрана 2024-03-05 в 10.43.22.png)

### Enrich Customers Gold Pipeline
![Enri![image](https://github.com/Dimitr119/final_task/assets/53143818/76bc8813-8557-4476-a295-0cb118858082)
ch Customers Gold Pipeline](Снимок экрана 2024-03-05 в 10.43.50.png)

Додаткову інформацію та завдання можна знайти в іншому [README.md](https://github.com/robot-dreams-code/Data-Engineering-Golovata/blob/main/final_project/README.md) на GitHub.
