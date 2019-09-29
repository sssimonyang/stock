安装 miniconda  
安装库  
conda install xlrd xlwt openpyxl pandas aiohttp requests  
服务器需解封25端口  
服务器设置定时任务  
service crond start  
crontab -e  
00 20 * * * /root/main.sh >> /home/log 2>&1 &
