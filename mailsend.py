#!/usr/bin/python3.4

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# from email.MIMEBase import MIMEBase
from email.mime.base import MIMEBase
from email import encoders

fromaddr = "pcstatsdcr@gmail.com"
MAILTO = ["vinay.unni@dcrworkforce.com", "neeshu.kalamgi@launchship.com", "neeshu.vasu@dcrworkforce.com"]

msg = MIMEMultipart()

msg['From'] = fromaddr
# msg['To'] = MAILTO
msg['To'] = ", ".join(MAILTO)
msg['Subject'] = "Statitics about Prompt Cloud data"

body = "Weekly statitics of Prompt Cloud data"

msg.attach(MIMEText(body, 'plain'))

filename = "pcdataanalysisresults.ods"
attachment = open("/mnt/nlpdata/pcdataanalysisresults.ods", "rb")

part = MIMEBase('application', 'octet-stream')
part.set_payload((attachment).read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

msg.attach(part)

server = smtplib.SMTP('smtp.gmail.com', 587)
server.starttls()
server.login(fromaddr, "pcstatsdcr@123$")
text = msg.as_string()
server.sendmail(fromaddr, MAILTO, text)
server.quit()
