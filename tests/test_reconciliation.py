from app import BASE_DIR, create_app
from reconciliation import reconcile_data


SAMPLE_A = BASE_DIR / "data" / "sample_security_master_bloomberg.csv"
SAMPLE_B = BASE_DIR / "data" / "sample_security_master_ice.csv"


def test_reconcile_data_returns_expected_summary(tmp_path):
    report_path = tmp_path / "reconciliation_report.xlsx"

    result = reconcile_data(
        source_a_path=SAMPLE_A,
        source_b_path=SAMPLE_B,
        report_path=report_path,
        source_a_label="Bloomberg Sample",
        source_b_label="ICE Sample",
    )

    assert result["summary"]["matched_records"] == 4
    assert result["summary"]["discrepancy_count"] == 3
    assert result["summary"]["field_mismatch_count"] == 3
    assert result["summary"]["severity_breakdown"]["High"] == 1
    assert result["summary"]["severity_breakdown"]["Medium"] == 2
    assert report_path.exists()


def test_demo_endpoint_returns_dashboard_payload():
    app = create_app()
    client = app.test_client()

    response = client.post("/api/demo")
    payload = response.get_json()

    assert response.status_code == 200
    assert payload["summary"]["source_a_label"] == "Bloomberg Sample"
    assert payload["summary"]["source_b_label"] == "ICE Sample"
    assert payload["summary"]["discrepancy_count"] == 3
    assert payload["report_url"] == "/download-report"


def test_reconcile_endpoint_requires_both_files():
    app = create_app()
    client = app.test_client()

    response = client.post("/api/reconcile", data={}, content_type="multipart/form-data")
    payload = response.get_json()

    assert response.status_code == 400
    assert "Source A" in payload["error"]


def test_reconcile_endpoint_accepts_uploaded_files():
    app = create_app()
    client = app.test_client()

    with SAMPLE_A.open("rb") as source_a_handle, SAMPLE_B.open("rb") as source_b_handle:
        response = client.post(
            "/api/reconcile",
            data={
                "source_a": (source_a_handle, "sample_security_master_bloomberg.csv"),
                "source_b": (source_b_handle, "sample_security_master_ice.csv"),
            },
            content_type="multipart/form-data",
        )

    payload = response.get_json()

    assert response.status_code == 200
    assert payload["summary"]["discrepancy_count"] == 3
    assert len(payload["discrepancies"]) == 3


def test_download_report_after_demo_run():
    app = create_app()
    client = app.test_client()

    demo_response = client.post("/api/demo")
    assert demo_response.status_code == 200

    download_response = client.get("/download-report")

    assert download_response.status_code == 200
    assert download_response.headers["Content-Disposition"].startswith("attachment;")




