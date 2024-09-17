import tabula
import pandas as pd
import csv
import multiprocessing
from functools import partial


def digit_only(str_digit):
    return ''.join(ch for ch in str_digit if ch.isdigit())


def read_pdf_page(pdf_path, page_number):
    """
    Reads a specific page from a PDF and returns a DataFrame.
    """
    try:
        df = tabula.read_pdf(pdf_path, pages=page_number)[0]
        return df
    except Exception as e:
        print(f"An error occurred while reading the PDF: {e}")
        return None


def clean_dataframe(df):
    """
    Cleans the DataFrame by setting headers and removing unnecessary rows.
    """
    df_cleaned = df.iloc[3:].reset_index(drop=True)
    df_cleaned.columns = ['Date', 'Debit',
                          'Credit', 'Balance', 'Transaction Detail']
    df = df_cleaned.drop(0).reset_index(drop=True)
    return df


def extract_entries(df):
    """
    Extracts entries from the DataFrame by grouping rows between dates.
    """
    entries = []
    current_entry = []

    for idx, row in df.iterrows():
        if pd.notna(row['Date']) and '/' in str(row['Date']):
            if current_entry:
                entries.append(current_entry)
            current_entry = [row.tolist()]
        else:
            if current_entry:
                current_entry.append(row.tolist())

    if current_entry:
        entries.append(current_entry)

    return entries


def process_entries(entries):
    """
    Processes the entries to construct a list of dictionaries for CSV output.
    """
    final_output = []

    for entry in entries:
        if not entry:
            continue

        record = {}
        record["Date"] = entry[0][0] if entry[0] else None

        record["TransactionId"] = digit_only(
            entry[2][0]) if len(entry) > 2 else None

        record["Amount"] = entry[1][2] if len(entry) > 1 else None

        content = " ".join(row[4] for row in entry if pd.notna(row[4]))
        record["Content"] = content.strip()

        final_output.append(record)

    return final_output


def write_csv(data, csv_file_path, page_number):
    """
    Appends data to an existing CSV file.
    """
    try:
        fieldnames = ["Date", "TransactionId", "Amount", "Content"]

        # Check if the file already exists and if it contains data
        file_exists = False
        try:
            with open(csv_file_path, mode='r', encoding='utf-8') as file:
                file_exists = True
        except FileNotFoundError:
            file_exists = False

        # Open the file in append mode
        with open(csv_file_path, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)

            # Write header only if the file is new or empty
            if not file_exists:
                writer.writeheader()

            # Append rows
            writer.writerows(data)

        print(f"Page {page_number} was successfully appended to {csv_file_path}")

    except Exception as e:
        print(f"An error occurred while writing to CSV: {e}")


# Initial Version
# def process_pdf_to_csv(pdf_path, csv_file_path, total_number):
#     """
#     Full process to read a PDF, clean data, extract entries, process data, and write to a CSV file.
#     """
#     # TODO: Batch Processing - reading the PDF by chunks (maybe 100 pages per ingestion) to process data more efficiently,
#     for page_number in range(2, total_number+1):
#         df = read_pdf_page(pdf_path, page_number)
#         if df is not None:
#             df = clean_dataframe(df)
#             entries = extract_entries(df)
#             final_output = process_entries(entries)
#             write_csv(final_output, csv_file_path, page_number)

# Optimized version
def process_pdf_page_range(pdf_path, csv_file_path, page_range):
    """
    Processes a range of pages and appends data to the CSV file.
    """
    for page_number in page_range:
        df = read_pdf_page(pdf_path, page_number)
        if df is not None:
            df = clean_dataframe(df)
            entries = extract_entries(df)
            final_output = process_entries(entries)
            write_csv(final_output, csv_file_path, page_number)


def process_pdf_to_csv(pdf_path, csv_file_path, total_number, batch_size=100, num_workers=4):
    """
    Processes the PDF in chunks of pages, allowing for faster ingestion with multiprocessing.
    """
    page_ranges = [range(i, min(i + batch_size, total_number+1))
                   for i in range(2, total_number+1, batch_size)]

    # Use multiprocessing to process different page ranges in parallel
    with multiprocessing.Pool(processes=num_workers) as pool:
        func = partial(process_pdf_page_range, pdf_path, csv_file_path)
        pool.map(func, page_ranges)

    print(f"Processing of {total_number} pages is complete.")


if __name__ == '__main__':
    pdf_path = "thong_tin_ung_ho_qua_tsk_vcb_0011001932418_tu_01_09_den10_09_2024.pdf"
    csv_file_path = "final_output.csv"
    total_pages = 12028
    process_pdf_to_csv(pdf_path, csv_file_path, total_pages)
