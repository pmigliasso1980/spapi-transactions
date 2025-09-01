import pathlib, subprocess, csv, time, pytest

@pytest.mark.integration
def test_cli_end_to_end_with_docker(tmp_path: pathlib.Path):
    outdir = tmp_path / "out"
    outdir.mkdir()
    ts = int(time.time())
    csv_name = f"summary_cli_{ts}.csv"

    cmd = [
        "docker","compose","run","--rm",
        "-v", f"{outdir}:/out",
        "-e","PG_DSN=postgresql://app:app@db:5432/spapi",
        "-e","MOCK_MODE=1",
        "-e","MOCK_FILE=/app/sample_data/rich_payload.json",
        "app","python","main.py",
        "--posted-after","2025-08-01T00:00:00Z",
        "--summary-csv", f"/out/{csv_name}",
    ]
    subprocess.run(cmd, check=True)

    csv_path = outdir / csv_name
    assert csv_path.exists()

    with open(csv_path) as fh:
        rows = list(csv.reader(fh))
    assert rows[0] == ["sku","total_amount"]
    assert any(r[0].startswith("SKU-RICH-") for r in rows[1:])
