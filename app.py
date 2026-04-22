from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, render_template, request, send_file, url_for
from werkzeug.utils import secure_filename

from reconciliation import ReconciliationError, reconcile_data


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = BASE_DIR / "uploads"
REPORT_DIR = BASE_DIR / "reports"
REPORT_PATH = REPORT_DIR / "reconciliation_report.xlsx"
SAMPLE_SOURCE_A = DATA_DIR / "sample_security_master_bloomberg.csv"
SAMPLE_SOURCE_B = DATA_DIR / "sample_security_master_ice.csv"


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["UPLOAD_FOLDER"] = str(UPLOAD_DIR)
    app.config["REPORT_FOLDER"] = str(REPORT_DIR)

    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    @app.route("/")
    def index() -> str:
        return render_template("index.html")

    @app.route("/api/reconcile", methods=["POST"])
    def reconcile_upload() -> tuple[dict, int] | tuple[object, int] | object:
        source_a_file = request.files.get("source_a")
        source_b_file = request.files.get("source_b")

        if not source_a_file or not source_a_file.filename:
            return jsonify({"error": "Please upload a Source A CSV file."}), 400
        if not source_b_file or not source_b_file.filename:
            return jsonify({"error": "Please upload a Source B CSV file."}), 400

        try:
            source_a_path = _save_uploaded_file(source_a_file, "source_a")
            source_b_path = _save_uploaded_file(source_b_file, "source_b")
        except ReconciliationError as exc:
            return jsonify({"error": str(exc)}), 400

        payload, status_code = _run_reconciliation_response(
            source_a_path=source_a_path,
            source_b_path=source_b_path,
            source_a_label="Uploaded Source A",
            source_b_label="Uploaded Source B",
        )
        return jsonify(payload), status_code

    @app.route("/api/demo", methods=["POST"])
    def reconcile_demo() -> tuple[object, int] | object:
        payload, status_code = _run_reconciliation_response(
            source_a_path=SAMPLE_SOURCE_A,
            source_b_path=SAMPLE_SOURCE_B,
            source_a_label="Bloomberg Sample",
            source_b_label="ICE Sample",
        )
        return jsonify(payload), status_code

    @app.route("/download-report")
    def download_report() -> object:
        if not REPORT_PATH.exists():
            return jsonify({"error": "No report is available yet. Run a reconciliation first."}), 404
        return send_file(REPORT_PATH, as_attachment=True, download_name=REPORT_PATH.name)

    return app


def _save_uploaded_file(file_storage, prefix: str) -> Path:
    filename = secure_filename(file_storage.filename or "dataset.csv")
    if not filename.lower().endswith(".csv"):
        raise ReconciliationError("Only CSV files are supported for reconciliation.")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    file_path = UPLOAD_DIR / f"{prefix}_{timestamp}_{filename}"
    file_storage.save(file_path)
    return file_path


def _run_reconciliation_response(
    *,
    source_a_path: Path,
    source_b_path: Path,
    source_a_label: str,
    source_b_label: str,
) -> tuple[dict, int]:
    try:
        result = reconcile_data(
            source_a_path=source_a_path,
            source_b_path=source_b_path,
            report_path=REPORT_PATH,
            source_a_label=source_a_label,
            source_b_label=source_b_label,
        )
    except ReconciliationError as exc:
        return {"error": str(exc)}, 400
    except Exception as exc:  # pragma: no cover - last-resort guard for runtime issues
        return {"error": f"Unexpected reconciliation error: {exc}"}, 500

    result["report_url"] = url_for("download_report")
    result["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return result, 200


app = create_app()


if __name__ == '__main__':
    app.run(debug=True)
