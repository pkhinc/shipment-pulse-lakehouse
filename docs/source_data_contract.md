# Shipment Pulse - Source Data Contract

## Purpose 

This document defines the structure and meaning of the source data ingested by the Shipment Pulse Lakehouse 

All data is synthetic and does not represent real customers or shipments 

## Landing-zone structure 

```text
landing/
├── ports/
│   └── ports.csv
├── shipments_cdc/
│   └── batch_001.jsonl
└── shipment_events/
    └── batch_001.jsonl

1. Port reference data

File pattern:

ports/*.csv
Column	Type	Required	Description
port_code	string	Yes	UN/LOCODE-style port identifier
port_name	string	Yes	Human-readable port name
country_code	string	Yes	ISO-style country code
region	string	Yes	Geographical region

This is small reference data used to enrich shipment records.

2. Shipment CDC records

File pattern:
shipments_cdc/*.jsonl

| Column                 | Type      | Required | Description                              |
| ---------------------- | --------- | -------: | ---------------------------------------- |
| shipment_id            | string    |      Yes | Unique shipment identifier               |
| booking_reference      | string    |      Yes | Booking reference from the source system |
| origin_port            | string    |      Yes | Port where transportation begins         |
| destination_port       | string    |      Yes | Planned destination port                 |
| planned_departure_time | timestamp |      Yes | Planned departure in UTC                 |
| planned_arrival_time   | timestamp |      Yes | Planned arrival in UTC                   |
| container_count        | integer   |      Yes | Number of containers                     |
| cargo_weight_kg        | decimal   |      Yes | Total cargo weight                       |
| customer_tier          | string    |      Yes | STANDARD, PREMIUM or STRATEGIC           |
| status                 | string    |      Yes | Current shipment status                  |
| operation              | string    |      Yes | INSERT, UPDATE or DELETE                 |
| operation_timestamp    | timestamp |      Yes | Time of the source-system change         |
| source_system          | string    |      Yes | System that produced the record          |

The combination of shipment_id and operation_timestamp will be used to determine
the latest version of a shipment.

3. Shipment events

File pattern:

shipment_events/*.jsonl
Column	Type	Required	Description
event_id	string	Yes	Unique event identifier
shipment_id	string	Yes	Shipment associated with the event
event_type	string	Yes	Type of logistics event
event_time	timestamp	Yes	Time when the event actually happened
received_at	timestamp	Yes	Time when the platform received the event
location_code	string	Yes	Port where the event happened
source_system	string	Yes	System that produced the event
payload_version	integer	Yes	Version of the event structure

event_time and received_at are intentionally separate. Their difference allows
the pipeline to identify late-arriving data.

Data quality rules
Every shipment_id must be present.
Every event_id should be unique.
Origin and destination ports must be different.
Port codes should exist in the port reference data.
Planned arrival must be later than planned departure.
Bronze preserves the original input.
Silver validates, deduplicates and quarantines invalid records.
Timestamps use ISO 8601 format with a UTC offset.
Each JSONL line contains one complete JSON object.