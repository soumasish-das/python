import os
import sys
from os.path import basename

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import smtplib


def email(to_list, sender, subject, cc_list='', msg_body=None, mail_html_file=None, attachment=None):
    # Use the variable below only if connecting to Gmail or other non-Outlook based emails
    sender_pass = 'GMAIL PASSWORD'

    body = ''
    if body:
        body = msg_body
    if mail_html_file:
        with open(mail_html_file) as file:
            for line in file:
                body += line

    # Set up the MIME. To and Cc are expected to be comma-separated email addresses
    message = MIMEMultipart('alternative')
    message['From'] = sender
    message['To'] = to_list
    message['Cc'] = cc_list
    message['Subject'] = subject

    # Attach message body to MIME
    message.attach(MIMEText(body, 'html'))

    # Message file attachments
    if attachment:
        if not os.path.exists(attachment):
            print("The attachment file {} doesn't exist! Mail not sent.".format(attachment))
            sys.exit(1)
        else:
            file_attachment = MIMEApplication(open(attachment, "rb").read())
            file_attachment.add_header('Content-Disposition', 'attachment; filename={}'.format(basename(attachment)))
            message.attach(file_attachment)

    # Create SMTP session and send the mail
    try:
        # Replace the server and port accordingly
        session = smtplib.SMTP('smtp.gmail.com', 587)
        session.starttls()

        # Use the statement below only if connecting to Gmail or other non-Outlook based emails
        session.login(sender, sender_pass)

        if cc_list:
            session.sendmail(message['From'], to_list.split(",") + cc_list.split(","), message.as_string())
        else:
            session.sendmail(message['From'], to_list.split(","), message.as_string())

        session.quit()
        print('Email Sent successfully.')
    except Exception as e:
        print("ERROR:\n" + str(e))
