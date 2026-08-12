import json
from pathlib import Path

from shipment_pulse.generator import generate_dataset


def _read_json_lines(path: Path) -> list[dict[str, object]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
    ]

def test_generate_dataset_creates_expected_records(tmp_path: Path) -> None:
    summary = generate_dataset(
        output_directory=tmp_path,
        shipment_count=5,
        seed=42,
    )

    shipments_path = tmp_path / "shipments_cdc" / "batch_001.jsonl"
    events_path = tmp_path / "shipment_events" / "batch_001.jsonl"
    ports_path = tmp_path / "ports" / "ports.csv"

    shipments = _read_json_lines(shipments_path)
    events = _read_json_lines(events_path)

    assert summary == {"ports": 6, "shipments": 5, "events": 5}
    assert ports_path.exists()
    assert len(shipments) == 5
    assert len(events) == 5
    assert all(record["operation"] == "INSERT" for record in shipments)
    assert len({record["event_id"] for record in events}) == 5