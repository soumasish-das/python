# Input/Output file information
fwf = input("Enter the absolute path of the fixed width file: ")
csv_file = input("Enter the absolute path of the CSV file to be written to: ")

# Input column widths
width_list = input("Enter comma-separated column widths: ")
width_list = list(map(int, width_list.split(',')))

with open(fwf) as file, open(csv_file, 'w') as csv:
    for line in file:
        if line == '\n':  # Ignore empty lines
            continue
        comma_sep_line = ''
        start_pos = 0
        for width in width_list:
            final_pos = start_pos + width
            comma_sep_line += '"' + (line[start_pos:final_pos]).rstrip() + '",'
            start_pos = final_pos
        comma_sep_line = comma_sep_line[:-1]    # Remove the last comma at the end of the line
        csv.write(comma_sep_line + '\n')

print("\nFixed Width File converted to CSV file successfully.")
