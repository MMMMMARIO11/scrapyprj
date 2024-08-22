# main.py (FastAPI应用)

from fastapi import FastAPI, BackgroundTasks
import subprocess
import os

app = FastAPI()

# 定义一个异步的后台任务，用于运行Scrapy爬虫
async def run_scrapy_spider():
    try:
        os.chdir(r'C:\Users\59158\scrapyprj')
        # 使用 subprocess 调用命令行来运行 Scrapy 爬虫
        subprocess.run(['scrapy', 'crawl', 'xxqg'])
    except Exception as e:
        print(f"Error running Scrapy spider: {e}")

@app.get("/run-scrapy-spider/")
async def trigger_scrapy_spider(background_tasks: BackgroundTasks):
    # 在后台任务中运行 Scrapy 爬虫
    background_tasks.add_task(run_scrapy_spider)
    return {"message": "Scrapy spider running in the background"}
