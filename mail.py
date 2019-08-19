import os
import smtplib
from contextlib import closing
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

from config import host, username, password, send_tos


def loginToServer(host, user, password):
    try:
        server = smtplib.SMTP(host, 25)
        server.login(user, password)
        return server
    except smtplib.SMTPException:
        print("连接服务器失败")
        return


def getmsg(text, att_urls):
    msg = MIMEMultipart('mixed')
    if text:
        msg.attach(MIMEText(text, 'plain', 'gbk'))
    if att_urls:
        for att_url in att_urls:
            if os.path.exists(att_url):
                _, filename = os.path.split(att_url)
                att = MIMEText(open(att_url, 'rb').read(), "base64", "gbk")
                att["Content-Type"] = 'application/octet-stream'
                att.add_header('Content-Disposition', 'attachment', filename=('gbk', '', filename))
                msg.attach(att)
    return msg


def send_mail(send_tos, name, subject, text, att_urls=None):
    with closing(loginToServer(host, username, password)) as server:
        if server:
            msg = getmsg(text="你好：\n" + text, att_urls=att_urls)
            msg['From'] = formataddr([name, username])
            msg['Subject'] = Header(subject, "utf-8").encode()
            server.sendmail(username, send_tos, msg.as_string())
            print("邮件发送成功")
        else:
            print("邮件发送失败")
    return


if __name__ == "__main__":
    att_urls = ["result/20190808结果.xls"]
    send_mail(send_tos=send_tos, name="Simon Yang", subject="2019-08-08结果", text="请查收附件。", att_urls=att_urls)
