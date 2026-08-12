from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from faker import Faker

PORTS = (
    {
        "port_code": "PLGDN",
        "port_name": "Gdansk",
        "country_code": "PL",
        "region": "Europe",
    },
    {
        "port_code": "DEHAM",
        "port_name": "Hamburg",
        "country_code": "DE",
        "region": "Europe",
    },
    {
        "port_code": "NLRTM",
        "port_name": "Rotterdam",
        "country_code": "NL",
        "region": "Europe",
    },
    {
        "port_code": "BEANR",
        "port_name": "Antwerp",
        "country_code": "BE",
        "region": "Europe",
    },
    {
        "port_code": "GBFXT",
        "port_name": "Felixstowe",
        "country_code": "GB",
        "region": "Europe",
    },
    {
        "port_code": "SGSIN",
        "port_name": "Singapore",
        "country_code": "SG",
        "region": "Asia",
    },
)


def _write_json_lines(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8") as output_file:
        for record in records:
            output_file.write(json.dumps(record, ensure_ascii=False) + "\n")


def _write_ports(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=list(PORTS[0].keys()))
        writer.writeheader()
        writer.writerows(PORTS)


def generate_dataset(
    output_directory: Path,
    shipment_count: int,
    seed: int,
) -> dict[str, int]:
    if shipment_count < 1:
        raise ValueError("shipment_count must be greater than zero")

    random_generator = random.Random(seed)
    fake = Faker("en_US")
    fake.seed_instance(seed)

    base_time = datetime(2026, 8, 1, 8, 0, tzinfo=UTC)
    shipments: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []

    for index in range(1, shipment_count + 1):
        shipment_id = f"SHP-{index:05d}"
        origin, destination = random_generator.sample(PORTS, 2)

        departure_time = base_time + timedelta(
            hours=random_generator.randint(24, 24 * 30)
        )
        arrival_time = departure_time + timedelta(
            hours=random_generator.randint(48, 240)
        )
        operation_time = departure_time - timedelta(
            days=random_generator.randint(7, 30)
        )
        received_at = operation_time + timedelta(
            minutes=random_generator.randint(1, 180)
        )

        shipments.append(
            {
                "shipment_id": shipment_id,
                "booking_reference": fake.bothify(text="BKG-########"),
                "origin_port": origin["port_code"],
                "destination_port": destination["port_code"],
                "planned_departure_time": departure_time.isoformat(),
                "planned_arrival_time": arrival_time.isoformat(),
                "container_count": random_generator.randint(1, 5),
                "cargo_weight_kg": round(
                    random_generator.uniform(5_000, 80_000),
                    2,
                ),
                "customer_tier": random_generator.choice(
                    ["STANDARD", "PREMIUM", "STRATEGIC"]
                ),
                "status": "BOOKED",
                "operation": "INSERT",
                "operation_timestamp": operation_time.isoformat(),
                "source_system": "TMS",
            }
        )

        events.append(
            {
                "event_id": f"EVT-{index:06d}",
                "shipment_id": shipment_id,
                "event_type": "BOOKED",
                "event_time": operation_time.isoformat(),
                "received_at": received_at.isoformat(),
                "location_code": origin["port_code"],
                "source_system": "EDI_GATEWAY",
                "payload_version": 1,
            }
        )

    _write_ports(output_directory / "ports" / "ports.csv")
    _write_json_lines(
        output_directory / "shipments_cdc" / "batch_001.jsonl",
        shipments,
    )
    _write_json_lines(
        output_directory / "shipment_events" / "batch_001.jsonl",
        events,
    )

    return {
        "ports": len(PORTS),
        "shipments": len(shipments),
        "events": len(events),
    }


def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic Shipment Pulse source data."
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--shipments", type=int, default=20)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> None:
    arguments = _parse_arguments()
    summary = generate_dataset(
        output_directory=arguments.output,
        shipment_count=arguments.shipments,
        seed=arguments.seed,
    )

    print(
        f"Generated {summary['ports']} ports, "
        f"{summary['shipments']} shipments and "
        f"{summary['events']} events."
    )


if __name__ == "__main__":
    main()
