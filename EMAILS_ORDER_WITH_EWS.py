#!/usr/bin/python
# -*- coding: cp1250  -*-
__author__ = 'Fekete Andr‡s Demeter'

from exchangelib import Configuration, Account, Credentials, NTLM, FileAttachment, Build, Version
import time
import cx_Oracle
import logging
import socket, csv, shutil
from datetime import timedelta, datetime
from email import encoders
import os
from email import generator
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import timedelta

backup_path ="P://mails_backup"


#Test shared folder connection
def test_shared_folder():
    try:
        return os.path.isdir(backup_path)
    except:
        return False


# save email item to .eml
def EmailGen(item):
        cclist = ""
        tolist = ""

        if item.to_recipients is not None:

            for toit in item.to_recipients:
                tolist+=str(toit.email_address)+";"

        if item.cc_recipients is not None:

            for ccit in item.cc_recipients:
                cclist+=str(ccit.email_address)+";"

        msg = MIMEMultipart()

        if item.sender.email_address is not None:
            msg['From'] = item.sender.email_address

        msg['Subject'] = item.subject
        msg['To'] = tolist
        msg['CC'] = cclist

        html = item.body

        if item.body.body_type=='HTML':
            part = MIMEText(html+"\r\n", 'html')
            # if body type html
        else:
            part = MIMEText(html+"\r\n", 'plain')
            # if body type text

        msg.attach(part)

        for attachment in item.attachments:
            if isinstance(attachment, FileAttachment):
                    att = MIMEBase('application', "octet-stream")
                    att.set_payload(attachment.content,charset="utf-8")
                    encoders.encode_base64(att)
                    att.add_header('Content-Disposition', 'attachment', filename=str(attachment.name))
                    msg.attach(att)

        SaveToFile(msg,item.subject,str(item.sender.email_address),item)

def SaveToFile(msg,subject,from_,item):
    with open(backup_path+'/'+str(item.datetime_received+timedelta(2/24)).replace('/','').replace('\\','').replace(' ','_').replace(':','').replace('+','_').replace('-','')+'##'+from_+"##"+str(subject).replace(' ','_').replace(':','').replace('+','_').replace('-','').replace('/','').replace('\\','')+'.eml', 'w') as outfile:
        gen = generator.Generator(outfile)
        gen.flatten(msg)




# save attachments from email item
def save_attachment(attachment,item):
    if isinstance(attachment, FileAttachment):
        if attachment.name.find('\\'):
            attachment_name =  attachment.name.split('\\')[len(attachment.name.split('\\'))-1]
        else:
            attachment_name=attachment.name
        local_path = 'P:\\Riport_beerk\\'+attachment_name
        with open(local_path, 'wb') as f:
             f.write(attachment.content)
        try:
            logging.info('Saved attachment to: '+ local_path, extra=qq)
            if test_shared_folder():
                try:
                    EmailGen(item)
                    logging.info('Saved mail to backup successfull', extra=qq)
                    item.delete()
                except Exception as e:
                    item.move(another_folder)
                    logging.error("error: "+str(e), extra=qq)
        except:
            pass


# email sender address user level
def csop(zi):
    global output
    ip = 'host.com'
    port = 1111
    db='sid'
    user='user'
    pwd ='password'
    dsn_tns = cx_Oracle.makedsn(ip, port, db)
    conn = cx_Oracle.connect(user, pwd, dsn_tns)
    cur=conn.cursor()
    output=cur.var(cx_Oracle.STRING)
    cur.callproc("package.process_mail",(zi,output))
    conn.close()
    output=(output.getvalue())
    return output

# email sender is report sender ?
def rip_(zi):
    global output2
    ip = 'host.com'
    port = 1111
    db='sid'
    user='user'
    pwd ='password'
    dsn_tns = cx_Oracle.makedsn(ip, port, db)
    conn = cx_Oracle.connect(user, pwd, dsn_tns)
    cur=conn.cursor()
    output2=cur.var(cx_Oracle.STRING)
    cur.callproc("package.process_mail_2",(zi,output2))
    conn.close()
    output2=(output2.getvalue())
    return output2

# send allert sms from database
def SMS_allert_LOG(input_):
    ip = 'host.com'
    port = 1111
    db='sid'
    user='user'
    pwd ='password'
    dsn_tns = cx_Oracle.makedsn(ip, port, db)
    conn = cx_Oracle.connect(user, pwd, dsn_tns)
    cur=conn.cursor()
    cur.callproc("package.process_MAIL_ALERT",(input_,))
    conn.close()

FORMAT = '%(asctime)-15s | %(levelname)s | %(clientip)s | %(user)-8s == %(message)s'
logging.basicConfig(filename='c:\\temp\\NaDiN_2.0_outlook_LOG_20'+str(time.strftime('%y_%m_%d'))+'.log',format=FORMAT)
logging.FileHandler.encoding='utf-8'
rootLogger = logging.getLogger('')
rootLogger.setLevel(logging.DEBUG)
rootLogger.setLevel(logging.WARNING)
rootLogger.setLevel(logging.ERROR)
rootLogger.setLevel(logging.INFO)

ipp = socket.gethostbyname(socket.gethostname())
qq = {'clientip': ipp, 'user': __author__}

try:
    log=(str(time.strftime('%y_%m_%d')))
    logging.info(log,extra=qq)
except:
    pass
global db_conn_test
global fileservice_conn_test
global fileservice_conn_test_1
global fileservice_conn_test_2

ttime=int(time.strftime('%H'))
ido_xx = time.strftime('%H:%M:%S')
time_hour2=0
print("run: "+str(ido_xx))

# exchange 2013
version = Version(build=Build(15, 0, 12, 34))

creds = Credentials( username='emailaddress@domain.hu', password='password')

config = Configuration( server='mail.host.com', credentials=creds,  auth_type=NTLM, version=version)

account = Account(
                                    primary_smtp_address='emailaddress@domain.hu',
                                    config=config,
                                    credentials=creds,
                                    autodiscover=False
                                 )
acc = None
acc = account.inbox
while 1:

    acc.refresh()
    try:
        another_folder_11 = account.root.get_folder_by_name('__ANOTHER_MAILS')
        another_folder = account.root.get_folder_by_name('reports')
        another_folder_1 = account.root.get_folder_by_name('__TASK_FROM_BOSS')
        another_folder_3 = account.root.get_folder_by_name('__TASK_FROM_DIRECTOR')
        another_folder_5 = account.root.get_folder_by_name('__TASK_FROM_OPERATION')
        another_folder_6 = account.root.get_folder_by_name('_Report_task_done')

        items = None
        items= acc.all().order_by('datetime_received')
        for item in items:

            logging.info("Mail form: "+str(item.sender.email_address), extra=qq)

            if str(item.subject) == "" and str(item.body) == "":
                item.move(another_folder_11)

            else:
                
                # Email textbody report examples
                
                if str(item.body)[:10] == 'agent_id;h':

                    yesterday = datetime.today() - timedelta(1)
                    datestr = yesterday.strftime('%Y%m%d')
                    datestr = str(datestr)
                    filename_csv_0 = datestr + '_email_textbody_report_export_1'
                    b = open("c:\\temp\\" + filename_csv_0 + ".csv", 'w', newline='')
                    a = csv.writer(b, delimiter=";")
                    filennname = str(filename_csv_0)
                    body_text_read = item.body.split('\r\n')
                    for body_text_item in body_text_read:
                        rowss = []
                        items_cells = body_text_item.split(';')
                        for item_cells in items_cells:
                            rowss.append(item_cells)
                        rowss.append(filennname)
                        a.writerows([rowss])
                    b.close()
                    time.sleep(5)
                    shutil.move("c:\\temp\\" + filename_csv_0 + ".csv", "P:\\Riport_beerk\\" + filename_csv_0 + ".csv")

                    if test_shared_folder():
                        try:
                            EmailGen(item)
                            logging.info('Saved mail to backup successfull', extra=qq)
                            item.delete()
                        except Exception as e:
                            item.move(another_folder)
                            logging.error("error: "+str(e), extra=qq)
            
                # Email textbody report examples if need title
                elif str(item.body)[:2] == 'XY' and str(item.subject)[:2] == 'YY' and str(item.sender.email_address).lower() == 'life@domain.hu':

                    yesterday = datetime.today() - timedelta(1)
                    datestr = yesterday.strftime('%Y%m%d')
                    datestr = str(datestr)
                    filename_csv_0 = datestr + '_email_textbody_report_export_2'
                    b = open("c:\\temp\\" + filename_csv_0 + ".csv", 'w', newline='')
                    a = csv.writer(b, delimiter=";")
                    a.writerow(['title1', 'title2', 'title3', 'title4', 'title5', 'title6', 'title6'])
                    body_text_data = item.body[6:]
                    body_text_read = body_text_data.split('\r\n')
                    for body_text_item in body_text_read:
                        rowss = []

                        items_cells = body_text_item.split('|')
                        for item_cells in items_cells:
                            rowss.append(item_cells)
                        if rowss != ['']:
                            a.writerows([rowss])
                    b.close()
                    time.sleep(5)
                    shutil.move("c:\\temp\\" + filename_csv_0 + ".csv", "P:\\Riport_beerk\\" + filename_csv_0 + ".csv")

                    if test_shared_folder():
                        try:
                            EmailGen(item)
                            logging.info('Saved mail to backup successfull', extra=qq)
                            item.delete()
                        except Exception as e:
                            item.move(another_folder)
                            logging.error("error: "+str(e), extra=qq)

                else:
                    
                    # check sender user level and type (report user or not)
                    text_subj = str(item.subject)
                    text_from = str(item.sender.email_address).lower()
                    try:
                        data2 = rip_(str(item.sender.email_address).lower())
                    except:
                        data2 = 'NO_USER'

                    if item.sender.email_address is None:
                        data2 = 'RIP_USER'

                    if text_from[:3] == "dws" or data2 == 'RIP_USER':
                        try:
                            for attachment in item.attachments:
                                # if report user save attachment
                                save_attachment(attachment,item)
                    
                        except Exception as e:
                            logging.error("error: "+str(e), extra=qq)
                        item.move(another_folder)

                    elif data2 == 'NO_USER':
                        
                        try:
                            u = len(str(item.sender.email_address))
                            data = csop(str(item.sender.email_address).lower())

                            if data == '__TASK_FROM_BOSS':
                                input_ = 'Email received from: ' + text_from
                                try:
                                    # send sms allert with sender user address
                                    SMS_allert_LOG(input_)
                                except:
                                    pass
                                item.move(another_folder_1)

                            elif data == '__TASK_FROM_DIRECTOR':
                                input_ = 'Email received from: ' + text_from
                                try:
                                    # send sms allert with sender user address
                                    SMS_allert_LOG(input_)
                                except:
                                    pass
                                item.move(another_folder_3)

                            elif data == '__TASK_FROM_OPERATION':
                                item.move(another_folder_5)

                            else:
                                item.move(another_folder_11)

                        except Exception as e:
                            logging.error("error: "+str(e), extra=qq)
                            time.sleep(30)

        time.sleep(30)
    except Exception as e:
        logging.error("error: "+str(e), extra=qq)
        time.sleep(30)
