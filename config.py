# config.py
"""
爬虫配置文件
"""

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Chenyixian6453!",
    "database": "novel_analysis",
    "charset": "utf8mb4"
}

# 爬虫配置
CRAWLER_CONFIG = {
    # 请求配置
    "request_timeout": 30,
    "retry_times": 3,

    # 爬取限制
    "max_chapters_per_novel": 50,  # 每部小说最大爬取章节数
    "max_comment_pages_per_chapter": 5,  # 每章节最大评论页数

    # 延迟配置（秒）
    "delay_between_requests": {
        "min": 1,
        "max": 5
    },
    "delay_between_chapters": {
        "min": 3,
        "max": 8
    },
    "delay_between_novels": {
        "min": 15,
        "max": 30
    },

    # 用户代理轮换
    "user_agents": [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
    ],

    # 代理配置（如有需要）
    "proxies": None,  # 例如: {"http": "http://proxy:port", "https": "https://proxy:port"}
}

# 日志配置
LOG_CONFIG = {
    "level": "INFO",
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
    "log_file": "novel_crawler.log"
}