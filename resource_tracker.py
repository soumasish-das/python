import pandas as pd
# import os

import sendmail

# resource_tracker = "C:\\Users\\Vicky\\Minnie\\Resource_tracker.xlsx"
resource_tracker = input("Enter the path of the resource tracker excel file: ")
data = pd.read_excel(resource_tracker)

# Replace all NaN with blanks
data = data.fillna('')

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
html += 'Hi,<br><br>\n'
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
html += '<p>\nBest regards,<br>Shruti Jayaswal\n</p>\n'
html += '</body>\n</html>'

# Write HTML contents to file
with open("Mail.html", "w") as file:
    file.write(html)

# Open the HTML file in the default browser
# os.startfile("Mail.html")

sendmail.email(to_list='soumasishdas@gmail.com,jayaswal.shruti@gmail.com',
               sender='soumasishdas@gmail.com',
               subject='Resource Tracker Test automated email',
               cc_list='soumasishdas@gmail.com,jayaswal.shruti@gmail.com',
               mail_html_file='Mail.html',
               attachment=resource_tracker
               )
