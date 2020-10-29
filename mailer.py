import pandas as pd
import smtplib, os, time, sys
from email.mime.multipart import MIMEMultipart 
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from getpass import getpass
from datetime import datetime

def check_credentials(email: str, password: str):
    """
    Checks if email and password is valid
    """
    print(f"Attempting ehlo...")
    time.sleep(2)
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()
    
    try:
        server.login(email, password)
    except Exception as f:
        print("Someting bad happended.\nError encountered on login is: ", f)
        print(f"Ensure you have enabled Less Secure apps in your email.")
        return False
    server.quit()
    return True


def send_mail(from_, pass_w, body_=None):
    toaddr = 'netbraus@gmail.com'

    msg = MIMEMultipart()
    msg['To'] = toaddr
    msg['Subject'] = '2NYP Data Collation'
    if body_:
        body = body_
    else:
        body = "Submitting Streamed Twitter Data."
    msg.attach(MIMEText(body, 'plain'))
    
    #Find and attach all excel files
    xlsx_files = [line for line in os.listdir() if line.endswith('.xlsx')]
    if not xlsx_files:
        print("We could not find any Excel files.\nQuitting in 2 seconds.")
        time.sleep(2)
        sys.exit()

    print('Attaching files...')
    time.sleep(2)
    for line in xlsx_files:
        attach_file=MIMEApplication(open(line,"rb").read())
        attach_file.add_header('Content-Disposition', 'attachment', filename=line)
        msg.attach(attach_file)

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.ehlo()
    server.starttls()

    server.login(from_, pass_w)

    text = msg.as_string()

    print("\n\nSending Email....\n\n")
    server.sendmail(from_, toaddr, text)
    server.quit()

    print("Mail successfully sent to Paschal")
    time.sleep(3)

    print("Cleanup time...")

#Announcement
print(f"Mailer for the 2NYP DS Project v1.")
time.sleep(3)

#Create new folder in current directory
path = os.getcwd()
path_ = path + '\\Sent_Files\\'
if not os.path.exists(path_):
    print("Creating folder 'Sent_Files' in current directory.")
    time.sleep(3)
    os.mkdir('Sent_Files')
    path_ = path + '\\Sent_Files\\'
else:
    print("Folder 'Sent_Files' already exists")


#Get creds and see if they are correct
print(f"Please enter your email address:")
sender = input('>>> ')
print(f"Please enter your email address:")
password = getpass('>>> ')

knocking = check_credentials(sender, password)
if not knocking:
    print(f"We got the above error while checking your email creds.\nExiting")
    time.sleep(3)
    sys.exit()

else:
    send_mail(sender, password)


#Move sent files
print(f"Moving file(s) to Sent_Files folder...")
time.sleep(3)

#Find and attach all excel files
xlsx_files = [line for line in os.listdir() if line.endswith('.xlsx')]
now = datetime.now()
for line in xlsx_files:
    namer = line[:-5]
    name_marker = datetime.strftime(now, '%Y_%m_%d_%H_%M') 
    path_now = os.getcwd() + '\\'
    os.replace(path_now + line, path_now + 'Sent_Files\\' + namer + '_' + name_marker + '.xlsx')

print("All done!")