import argparse
import pandas as pd
import shutil
import os
import sendmail

parser = argparse.ArgumentParser(description='Resource Tracker Automated Email: '
                                             'Script to generate and send email for resource tracking.')

# Required arguments
required_args = parser.add_argument_group('required arguments')
required_args.add_argument("-l", "--excel-location", help="Location of the Resource Tracker excel file", required=True)
required_args.add_argument("-o", "--output-dir", help="Output directory to generate HTML file for email", required=True)
required_args.add_argument("-tn", "--to-list-names", help="To-list names to be used in the email body", required=True)
required_args.add_argument("-sn", "--sender-name", help="Sender's name to be used in the email body", required=True)
required_args.add_argument("-s", "--subject", help="Subject of the email", required=True)
required_args.add_argument("-te", "--to-list-addresses", help="Comma separated to-list email addresses", required=True)
required_args.add_argument("-se", "--sender-address", help="Sender's email address", required=True)

# Optional arguments
parser.add_argument("-ce", "--cc-list-addresses", help="Comma separated cc-list email addresses")
parser.add_argument("-a", "--attachment", help="File attachment to be added to the email")

args = parser.parse_args()

# Read the Excel file contents and get the tab name
data = pd.read_excel(args.excel_location)
tab_name = pd.ExcelFile(args.excel_location).sheet_names[0]

# Replace all NaN with '-'
data = data.fillna('-')

# Set CSS style
html = '<html>\n<head>\n<style>\n'
html += 'table, th, td {border: 1px solid black; border-collapse: collapse; padding: 8px; font-family: Calibri,' \
        'sans-serif}\n'
html += 'tr {background-color: #f0f8ff}\n'
html += 'p {font-family: Calibri,sans-serif}\n'
html += '</style>\n</head>\n'

# Begin HTML and email body
html += '<body style="font-size: 11pt">\n'
html += '<p>\n'
html += 'Hi {},<br><br>\n'.format(args.to_list_names)
html += 'Please see the EOD task status of each resource below:<br>\n</p>\n'
html += '<table>\n'

# Set the table headers
html += '<tr style="background-color: #0051a2; color: white">\n'
columns = list(data.columns)
for column in columns:
    html += '<th>' + column + '</th>\n'
html += '</tr>\n'

# Populate table data
for i in range(len(data)):
    html += '<tr>\n'
    for j in range(len(columns)):
        html += '<td>' + str(data.iloc[i, j]).replace("\n", "<br>") + '</td>\n'
    html += '</tr>\n'
html += '</table>\n'

# End of mail body and HTML
html += '<p>\nBest regards,<br>{}\n</p>\n'.format(args.sender_name)
html += '</body>\n</html>'

mail_file = args.output_dir + "\\Mail.html"

# Write HTML contents to file
with open(mail_file, "w") as file:
    file.write(html)

# Send email
sendmail.email(to_list=args.to_list_addresses,
               sender=args.sender_address,
               subject=args.subject,
               cc_list=args.cc_list_addresses,
               mail_html_file=mail_file,
               attachment=args.attachment
               )

# Create a backup copy of the original Excel file before clearing contents from its columns
shutil.copyfile(args.excel_location, os.path.join(args.output_dir, 'Backup_' + os.path.basename(args.excel_location)))

# Replace all column values with blank, except for the first column
data[columns[1:]] = ''

# Replace the original Excel file
writer = pd.ExcelWriter(args.excel_location, engine='xlsxwriter')
workbook = writer.book
worksheet = workbook.add_worksheet(tab_name)
writer.sheets[tab_name] = worksheet

header_cell_format = workbook.add_format({'bold': True, 'font_color': 'black', 'align': 'center',
                                          'valign': 'vcenter', 'border': 1, 'border_color': 'black',
                                          'bg_color': '#8DB5E2'})
data_cell_format = workbook.add_format({'border': 1, 'font_color': 'black', 'border_color': 'black',
                                        'bg_color': 'white'})

for i in range(len(data)):
    for j in range(len(columns)):
        if i == 0:
            # Write both header and data for the first iteration
            worksheet.write(0, j, columns[i], header_cell_format)
            worksheet.write(i + 1, j, data.iloc[i, j], data_cell_format)
        else:
            worksheet.write(i + 1, j, data.iloc[i, j], data_cell_format)
writer.save()

print("\nProgram completed successfully.")
