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

### Process Sales Pipeline
![image](https://github.com/Dimitr119/final_task/assets/53143818/ef60efc3-e6d1-485a-ba33-df6a76f9e516)


### Process Customers Pipeline
![image](https://github.com/Dimitr119/final_task/assets/53143818/66d68706-5974-4519-b2a3-69d8504b5b91)


### Process User Profiles Pipeline
![image](https://github.com/Dimitr119/final_task/assets/53143818/191677c0-bc25-48cc-9535-c3683fae4ba2)


### Enrich Customers Gold Pipeline
![image](https://github.com/Dimitr119/final_task/assets/53143818/9635a7aa-161b-41d3-bf77-55856df64e56)


Додаткову інформацію та завдання можна знайти в іншому [README.md](https://github.com/robot-dreams-code/Data-Engineering-Golovata/blob/main/final_project/README.md) на GitHub.
