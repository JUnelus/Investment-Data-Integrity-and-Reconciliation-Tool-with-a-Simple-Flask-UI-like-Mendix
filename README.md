# Investment Data Integrity and-Reconciliation Tool with Flask UI like Mendix
This project simulates a data integrity and reconciliation process for investment portfolios, leveraging security master data from multiple sources (e.g., Bloomberg, ICE) to detect and resolve discrepancies. The project automates the reconciliation process, provides data integrity reports, and suggests improvements for managing data accuracy. 

## Project Overview

This project is a web-based tool developed using Python, Flask, and Bootstrap. It allows users to upload security master data (in CSV format) from different sources, reconcile them to find discrepancies, and generate downloadable reports in Excel format. The UI is simple and easy to use, offering features similar to a Mendix-like UI, making it ideal for investment data management.

## Features

- Upload and compare **two security master data files** (CSV)
- Run a **bundled demo reconciliation** using the included Bloomberg and ICE sample files
- Automated reconciliation engine with:
  - required-column validation
  - duplicate CUSIP detection
  - record coverage checks (missing in Source A / Source B)
  - field-level comparison across market price, outstanding balance, coupon rate, exchange, and more
- Dynamic dashboard UI with:
  - KPI summary cards
  - issue and severity charts
  - searchable and sortable discrepancy workbench
  - no full-page reload after processing
- Download a multi-sheet Excel reconciliation report
- Built using Flask, Pandas, Bootstrap, DataTables, Chart.js, and custom CSS styling

## Technologies Used

- Python
- Flask
- Pandas
- Bootstrap
- HTML/CSS
- Excel for report generation

## Project Structure

```bash
flask_ui_project/
├── app.py              # Main Flask application
├── templates/
│   └── index.html      # Main UI template (HTML with Bootstrap)
├── static/
│   └── style.css       # Custom CSS styling
├── uploads/            # Folder for storing uploaded CSV files
├── reports/            # Folder for storing generated reports
├── reconciliation.py   # Python script for reconciliation logic
├── README.md           # Project documentation (this file)
```

## Installation and Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/JUnelus/Investment-Data-Integrity-and-Reconciliation-Tool-with-a-Simple-Flask-UI-like-Mendix.git
   ```

2. **Create a virtual environment (optional but recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```

3. **Install the required dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Run the Flask application:**
   ```bash
   python app.py
   ```

5. **Access the application:**
   Open your web browser and go to `http://127.0.0.1:5000` to use the tool.
![img_1.png](img_1.png)

## Usage

1. Start the Flask app.
2. Upload both a **Source A** and **Source B** security master CSV file, or click **Run bundled demo**.
3. Review the live reconciliation dashboard for:
   - match rate
   - source-only records
   - issue distribution
   - detailed discrepancy rows
4. Download the generated Excel report for audit and offline analysis.

## Testing

Run the lightweight automated tests with:

```bash
pytest
```

## Example CSV Structure

Here is an example of how the CSV file should be structured for security master data:

```csv
CUSIP,Security Name,Asset Class,Issue Date,Maturity Date,Coupon Rate,Outstanding Balance,Market Price,Currency,Country,Exchange
123456789,ABC Corp Bond,Bond,2020-01-01,2030-01-01,3.50%,1000000,100.50,USD,USA,NYSE
987654321,XYZ Corp Stock,Equity,2019-05-15,N/A,N/A,N/A,150.75,USD,USA,NASDAQ
456123789,DEF Mortgage Backed,MBS,2021-03-01,2035-03-01,4.00%,500000,98.75,USD,USA,NYSE
654789123,GHI Treasury Bond,Bond,2018-07-01,2028-07-01,2.00%,250000,102.25,USD,USA,NYSE
```
![img_2.png](img_2.png)