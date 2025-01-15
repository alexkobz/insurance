import os
import smtplib, ssl
from datetime import datetime as dt
from dotenv import load_dotenv
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from functions.get_date import last_day_month


def main(*filenames):
    # load_dotenv('.venv/.env')
    sender_email = os.environ["LOGIN_EMAIL"]
    receiver_email = os.environ["LOGIN_EMAIL"]
    subject = ' '.join(filenames)
    password = os.environ["PASSWORD_EMAIL"]
    
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email 
    message["Subject"] = subject
    
    body = ""
    message.attach(MIMEText(body, "plain"))
    
    for filename in filenames:
        with open("./data/Output/" + filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())
            encoders.encode_base64(part)
            part.add_header(
                        "Content-Disposition",
                        # insert report date into filename
                        f"attachment; filename= {filename[:filename.find('.')]}_{last_day_month.strftime('%Y%m%d')}{filename[filename.find('.'):]}",
                    )
            message.attach(part)
            
    text = message.as_string()
    
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, text)
