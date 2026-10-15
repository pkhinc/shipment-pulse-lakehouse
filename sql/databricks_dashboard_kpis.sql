-- Shipment Pulse Lakehouse | Databricks AI/BI Dashboard
-- Sources: managed Delta tables imported from local Gold Parquet files.

-- KPI 1: Total Shipments
SELECT
    COUNT(DISTINCT shipment_id) AS total_shipments
FROM shipment_platform.gold.shipment_summary;

-- KPI 2: Total Cargo Weight (kg)
SELECT
    ROUND(SUM(cargo_weight_kg), 2) AS total_cargo_weight_kg
FROM shipment_platform.gold.shipment_summary;

-- KPI 3: Total Routes
SELECT
    COUNT(*) AS total_routes
FROM (
    SELECT DISTINCT
        origin_port,
        destination_port
    FROM shipment_platform.gold.route_summary
) AS routes;
