import pandas as pd
import gspread
import logging
from google.oauth2.service_account import Credentials
from sklearn.preprocessing import StandardScaler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler("log.txt"),
    logging.StreamHandler()
])
logger = logging.getLogger()

def authorize_google_sheets(json_keyfile):
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    creds = Credentials.from_service_account_file(json_keyfile, scopes=scope)
    client = gspread.authorize(creds)
    return client


def read_data_from_google_sheets(sheet_name, worksheet_name, json_keyfile):
    client = authorize_google_sheets(json_keyfile)

    spreadsheet = client.open(sheet_name)
    worksheet = spreadsheet.worksheet(worksheet_name)

    data = worksheet.get_all_values()
    headers = data[0]
    rows = data[1:]
    df = pd.DataFrame(rows, columns=headers)

    df['Wiek'] = pd.to_numeric(df['Wiek'], errors='coerce')
    df['Średnie Zarobki'] = pd.to_numeric(df['Średnie Zarobki'].str.replace(',', '.'), errors='coerce')

    return df


def clean_data(df):
    logger.info("Rozpoczynanie czyszczenia danych...")

    initial_rows = len(df)

    initial_missing = df.isna().sum().sum()
    logger.info(f"Liczba brakujących wartości przed czyszczeniem: {initial_missing}")

    df = df.dropna(thresh=len(df.columns) // 2, axis=0)

    df = df[(df['Wiek'] != 0) & (df['Średnie Zarobki'] != 0)]

    for column in df.select_dtypes(include=['object']).columns:
        df = df[df[column] != ""]

    for column in df.select_dtypes(include=['float64', 'int64']).columns:
        median_value = df[column].median()
        df[column] = df[column].fillna(median_value)

    final_missing = df.isna().sum().sum()
    logger.info(f"Liczba brakujących wartości po czyszczeniu: {final_missing}")

    changed_values = initial_missing - final_missing
    changed_percentage = (changed_values / initial_missing) * 100 if initial_missing != 0 else 0
    logger.info(f"Procent zmienionych danych (uzupełnione braki): {changed_percentage:.2f}%")

    removed_rows = initial_rows - len(df)
    removed_percentage = (removed_rows / initial_rows) * 100

    logger.info(f"Liczba usuniętych wierszy: {removed_rows}, co stanowi {removed_percentage:.2f}% wszystkich danych.")

    return df, removed_percentage, changed_percentage


def standardize_data(df):
    logger.info("Rozpoczynanie standaryzacji danych...")
    num_columns = df.select_dtypes(include=['float64', 'int64']).columns
    scaler = StandardScaler()
    df[num_columns] = scaler.fit_transform(df[num_columns])
    logger.info("Zakończono standaryzację danych.")
    return df


def generate_report(changed_percentage, removed_percentage, report_file="report.txt"):
    with open(report_file, "w") as file:
        file.write("Raport z czyszczenia i standaryzacji danych:\n")
        file.write(f"Procent zmienionych danych: {changed_percentage:.2f}%\n")
        file.write(f"Procent usuniętych danych: {removed_percentage:.2f}%\n")

    logger.info(f"Raport został zapisany w pliku: {report_file}")


def save_to_google_sheets(df, sheet_name, worksheet_name, json_keyfile):
    client = authorize_google_sheets(json_keyfile)
    spreadsheet = client.open(sheet_name)
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
        worksheet.clear()
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=len(df.columns))

    df = df.replace([pd.NA, pd.NaT, None], '').replace([float('inf'), float('-inf'), float('nan')], 0)

    worksheet.update([df.columns.values.tolist()] + df.values.tolist())
    logger.info(f"Dane zostały zapisane do arkusza Google: {worksheet_name}")

if __name__ == "__main__":
    json_keyfile = 'lab2-439408-876ac3b2eb7f.json'
    sheet_name = 'Lab2'
    worksheet_name = 'Arkusz1'
    worksheet_name_cleaned = 'Arkusz2'
    worksheet_name_standardized = 'Arkusz3'
    df = read_data_from_google_sheets(sheet_name, worksheet_name, json_keyfile)

    print("Dane przed czyszczeniem:")
    print(df.head())

    df_cleaned, removed_percentage, changed_percentage = clean_data(df)

    print("\nDane po czyszczeniu:")
    print(df_cleaned.head())
    save_to_google_sheets(df_cleaned, sheet_name, worksheet_name_cleaned, json_keyfile)
    print(f"Dane po czyszczeniu zostały zapisane do '{worksheet_name_cleaned}'")

    df_standardized = standardize_data(df_cleaned)

    print("\nDane po standaryzacji:")
    print(df_standardized.head())
    save_to_google_sheets(df_standardized, sheet_name, worksheet_name_standardized, json_keyfile)
    print(f"Dane po standaryzacji zostały zapisane do '{worksheet_name_standardized}'")

    generate_report(changed_percentage=changed_percentage, removed_percentage=removed_percentage)
