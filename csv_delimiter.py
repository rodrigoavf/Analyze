import os
import chardet
import codecs

def csv_check(csv_file):
    file_extension = os.path.splitext(csv_file)[1]

    # Check if the file extension is a common CSV extension (e.g., '.csv', '.tsv', '.txt').
    csv_extensions = ['.csv', '.tsv', '.txt']  # Add more if needed.
    is_csv = file_extension.lower() in csv_extensions
    if not is_csv:
        return print(f"The file '{csv_file}' does not appear to be a CSV file.")

    def detect_encoding(csv_file):
        with open(csv_file, 'rb') as f:
            result = chardet.detect(f.read())
            return result['encoding']

    # Detect the file's encoding.
    file_encoding = detect_encoding(csv_file) or 'utf-8'

    # Read the first few lines of the CSV file to analyze the delimiters.
    num_lines_to_read = 5  # You can adjust this as needed.

    with open(csv_file, 'r', encoding=file_encoding) as file:
        # Read the first few lines of the file.
        first_lines = [next(file) for _ in range(num_lines_to_read)]
    
    # Read the first few lines of the CSV file to analyze the delimiters.
    num_lines_to_read = 5  # You can adjust this as needed.

    with codecs.open(csv_file, 'r') as file:
        # Read the first few lines of the file.
        first_lines = [next(file) for _ in range(num_lines_to_read)]

    # Initialize a dictionary to store delimiter counts.
    delimiter_counts = {}

    # Iterate over potential delimiters (comma, semicolon, tab, etc.).
    potential_delimiters = [',', ';', '\t', '|']  # Add more if needed.
    for delimiter in potential_delimiters:
        counts = []

        # Count the occurrences of the delimiter in each line.
        for line in first_lines:
            count = line.count(delimiter)
            counts.append(count)

        # Store the delimiter and its counts in the dictionary.
        delimiter_counts[delimiter] = counts

    # Determine the delimiter with the highest count.
    most_common_delimiter = max(delimiter_counts, key=lambda d: sum(delimiter_counts[d]))

    return most_common_delimiter