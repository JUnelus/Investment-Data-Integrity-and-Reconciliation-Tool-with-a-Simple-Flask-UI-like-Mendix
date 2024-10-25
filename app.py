from flask import Flask, render_template, request, redirect, url_for, send_file
import pandas as pd
import os

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads/'
app.config['REPORT_FOLDER'] = 'reports/'

if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])
if not os.path.exists(app.config['REPORT_FOLDER']):
    os.makedirs(app.config['REPORT_FOLDER'])

# Route for the main page
@app.route('/')
def index():
    return render_template('index.html')

# Route to handle file upload and reconciliation
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return 'No file part'
    file = request.files['file']
    if file.filename == '':
        return 'No selected file'
    if file:
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
        file.save(filepath)
        # Run reconciliation process here (call your script)
        discrepancies = run_reconciliation(filepath)
        return render_template('index.html', discrepancies=discrepancies)

# Route to download the report
@app.route('/download-report')
def download_report():
    report_path = os.path.join(app.config['REPORT_FOLDER'], 'reconciliation_report.xlsx')
    return send_file(report_path, as_attachment=True)

def run_reconciliation(filepath):
    # Load the CSV file into a DataFrame
    df = pd.read_csv(filepath)

    # Perform reconciliation based on 'Market Price'
    discrepancies = df[df['Market Price'] < 100]  # Example condition for discrepancies

    # Save the discrepancies report
    report_path = os.path.join('reports', 'reconciliation_report.xlsx')
    discrepancies.to_excel(report_path, index=False)

    return discrepancies.to_dict(orient='records')

if __name__ == '__main__':
    app.run(debug=True)
