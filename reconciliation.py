import pandas as pd


def reconcile_data(file_path):
    # Read in the data (assuming a CSV for now)
    df = pd.read_csv(file_path)

    # Example: Find discrepancies where Value is below a threshold
    discrepancies = df[df['Value'] < 100]

    # Save discrepancies report to Excel
    report_path = 'reports/reconciliation_report.xlsx'
    discrepancies.to_excel(report_path, index=False)

    return discrepancies.to_dict(orient='records')
