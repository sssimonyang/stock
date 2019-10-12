下载miniconda安装
python 路径 /root/miniconda3/bin/python
安装需要的库  
conda install requests xlrd xlwt openpyxl pandas aiohttp
服务器需解封25端口  
服务器设置定时任务  
service crond start  
crontab -e  
00 18 * * * /root/main.sh >> /home/log 2>&1 &
