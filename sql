CREATE VIEW `shaped-clarity-445113-s1.cohort_dataset.transformed_data_view` AS
WITH cohort_data AS (
    SELECT
        DATE_TRUNC(subscription_started, MONTH) AS cohort_month,
        subscription_started,
        CASE
            WHEN subscription_plan_type IN ('Lifetime', 'Single Course') THEN TIMESTAMP(DATE_ADD(DATE(subscription_started), INTERVAL 24 MONTH))
            WHEN subscription_plan_type = 'Monthly' THEN TIMESTAMP(DATE_ADD(DATE(subscription_started), INTERVAL 1 MONTH))
            WHEN subscription_plan_type = 'Quarterly' THEN TIMESTAMP(DATE_ADD(DATE(subscription_started), INTERVAL 3 MONTH))
            WHEN subscription_plan_type = 'Semi-Annual' THEN TIMESTAMP(DATE_ADD(DATE(subscription_started), INTERVAL 6 MONTH))
            WHEN subscription_plan_type = 'Annual' THEN TIMESTAMP(DATE_ADD(DATE(subscription_started), INTERVAL 12 MONTH))
            WHEN subscription_plan_type = 'Attachment-Bootcamp' THEN TIMESTAMP(DATE_ADD(DATE(subscription_started), INTERVAL 1 MONTH))
        END AS subscription_end
    FROM
        cohort_dataset.transformed_data
),
active_users AS (
    SELECT
        cohort_month,
        DATE_TRUNC(subscription_started, MONTH) AS start_month,
        DATE_TRUNC(subscription_end, MONTH) AS end_month,
        GENERATE_DATE_ARRAY(DATE(subscription_started), DATE(subscription_end), INTERVAL 1 MONTH) AS active_months
    FROM
        cohort_data
),
flattened_data AS (
    SELECT
        cohort_month,
        DATE_TRUNC(month, MONTH) AS active_month
    FROM
        active_users, UNNEST(active_months) AS month
),
retention_matrix AS (
    SELECT
        cohort_month,
        DATE_DIFF(DATE(active_month), DATE(cohort_month), MONTH) + 1 AS retention_month,
        COUNT(*) AS active_users
    FROM
        flattened_data
    GROUP BY
        cohort_month, retention_month
)
SELECT
    FORMAT_TIMESTAMP("%Y/%m", cohort_month) AS subscription_started,
    MAX(CASE WHEN retention_month = 1 THEN active_users ELSE 0 END) AS Month_1,
    MAX(CASE WHEN retention_month = 2 THEN active_users ELSE 0 END) AS Month_2,
    MAX(CASE WHEN retention_month = 3 THEN active_users ELSE 0 END) AS Month_3,
    MAX(CASE WHEN retention_month = 4 THEN active_users ELSE 0 END) AS Month_4,
    MAX(CASE WHEN retention_month = 12 THEN active_users ELSE 0 END) AS Month_12
FROM
    retention_matrix
GROUP BY
    cohort_month
ORDER BY
    cohort_month;
