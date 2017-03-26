import smtplib
import datetime


def SendEmail(text):
    s=smtplib.SMTP("smtp.office365.com", 587)
    s.ehlo()
    s.starttls()
    s.login("deependra@directmailers.com", "Royan@100")

    from_addr = 'deependra@directmailers.com'
    to_addr = 'deependra@directmailers.com'

    subj = "SOM DATA NOTIFICATION"
    date = datetime.datetime.now().strftime( "%d/%m/%Y %H:%M" )

    message_text = text

    msg = "From: %s\nTo: %s\nSubject: %s\nDate: %s\n\n%s" % ( from_addr, to_addr, subj, date, message_text )

    s.sendmail(from_addr, to_addr, msg)
    s.quit()
    return;