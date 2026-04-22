WITH order_payment AS (
    SELECT 
        order_id,
        installments,
        SUM(payment_value) AS total_payment
    FROM raw.payments
    GROUP BY order_id, installments
)

SELECT 
    installments,
    AVG(total_payment) AS avg_payment_per_order
FROM order_payment
GROUP BY installments
ORDER BY avg_payment_per_order DESC;