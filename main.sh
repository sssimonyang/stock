#!/bin/bash
cd /home/python/stock
/root/miniconda3/bin/python main.py >> log 2>&1
cd /home/python/homepage
/root/miniconda3/bin/python yanghua.py >> log 2>&1
if [ $? == 1 ]
then
echo "重新运行" >> log
/root/miniconda3/bin/python yanghua.py >> log 2>&1
fi
