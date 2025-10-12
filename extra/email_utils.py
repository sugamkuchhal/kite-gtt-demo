import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging

# Configure your SMTP server and sender details here
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "sugamkuchhal@gmail.com"
SMTP_PASSWORD = "hn9t6PTPSsunki*4814"  # Use app password or environment variable for security
EMAIL_FROM = SMTP_USER
EMAIL_TO = ["sugamkuchhal2023@gmail.com", "sugamkuchhal@gmail.com"]  # List of recipients

def send_summary_email(subject, body):
    try:
        msg = MIMEMultipart()
        msg["From"] = EMAIL_FROM
        msg["To"] = ", ".join(EMAIL_TO)
        msg["Subject"] = subject

        msg.attach(MIMEText(body, "plain"))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(EMAIL_FROM, EMAIL_TO, msg.as_string())
        server.quit()
        logging.info("📧 Summary email sent successfully.")
    except Exception as e:
        logging.error(f"❌ Failed to send summary email: {e}")
