SELECT
	*
FROM prod.customers
LIMIT 100;

SELECT
	MIN(transaction_date)
	,MAX(transaction_date)
FROM prod.transactions;