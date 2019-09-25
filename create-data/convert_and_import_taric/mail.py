# import the smtplib module. It should be included in Python by default
import smtplib, time

# set up the SMTP server
server = smtplib.SMTP(host='smtp.gmail.com', port=587)
server.starttls()
server.login('matthew.lavis@gmail.com', 'Gonzibar98$')

server.ehlo()
print ('server working fine')
time.sleep(5)
sender = "<matthew.lavis@gmail.com>"
receivers = ["<matt.lavis@digital.trade.gov.uk>"]
subject = "SMTP e-mail Test" 
msg = "This is an automated message being sent by Python. Python is the mastermind behind this."
server.sendmail(sender, receivers, subject, msg)
print ('sending email to outlook')
server.quit()
