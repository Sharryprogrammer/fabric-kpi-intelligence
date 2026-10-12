CREATE OR ALTER VIEW dbo.vw_RegionalKpi
AS
SELECT
    region,
    completed_orders,
    units_sold,
    revenue,
    gross_margin,
    gross_margin_rate
FROM dbo.gold_region_kpis;
GO

CREATE OR ALTER VIEW dbo.vw_DailyKpi
AS
SELECT
    order_date,
    completed_orders,
    units_sold,
    revenue,
    gross_margin,
    average_order_value
FROM dbo.gold_daily_kpis;
GO
