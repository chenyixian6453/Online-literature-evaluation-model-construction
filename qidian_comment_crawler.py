# qidian_comment_crawler.py
import pymysql
import requests
import time
import random
import json
import re
from bs4 import BeautifulSoup
import logging
from datetime import datetime
from typing import List, Dict, Optional

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('comment_crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Chenyixian6453!",
    "database": "novel_analysis",
    "charset": "utf8mb4"
}


class QidianCommentCrawler:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://www.qidian.com/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'X-Requested-With': 'XMLHttpRequest'
        })
        self.base_comment_url = "https://read.qidian.com/ajax/book/comment"

    def get_book_id_from_url(self, url: str) -> Optional[str]:
        """从作品URL提取bookId"""
        patterns = [
            r'book/(\d+)/',
            r'bid=(\d+)',
            r'id=(\d+)'
        ]

        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)

        # 尝试直接解析URL
        try:
            return url.split('/')[-1].split('?')[0]
        except:
            return None

    def fetch_comments_by_chapter(self, book_id: str, chapter_id: str, page: int = 1) -> Dict:
        """获取章节评论"""
        try:
            url = f"{self.base_comment_url}/chapter?bookId={book_id}&chapterId={chapter_id}&page={page}&pageSize=20"

            time.sleep(random.uniform(1, 3))
            response = self.session.get(url, timeout=15)
            response.encoding = 'utf-8'

            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"评论请求失败: {url}, 状态码: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"获取评论异常: {e}")
            return None

    def fetch_global_comments(self, book_id: str, page: int = 1) -> Dict:
        """获取全书评论（书评区）"""
        try:
            url = f"{self.base_comment_url}/list?bookId={book_id}&page={page}&pageSize=20&type=2"

            time.sleep(random.uniform(1, 3))
            response = self.session.get(url, timeout=15)
            response.encoding = 'utf-8'

            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"书评请求失败: {url}, 状态码: {response.status_code}")
                return None
        except Exception as e:
            logger.error(f"获取书评异常: {e}")
            return None

    def parse_comment_data(self, comment_json: Dict) -> List[Dict]:
        """解析评论JSON数据"""
        comments = []

        try:
            data = comment_json.get('data', {})
            posts = data.get('posts', [])

            for post in posts:
                try:
                    comment = {
                        'user_name': post.get('userName', '匿名用户'),
                        'comment_content': post.get('content', '').strip(),
                        'comment_time': post.get('createTime', ''),
                        'like_num': int(post.get('likeNum', 0)),
                        'floor_num': int(post.get('floorNum', 0)),
                        'chapter_id': post.get('chapterId', ''),
                        'chapter_name': post.get('chapterName', '')
                    }

                    # 清理HTML标签
                    if comment['comment_content']:
                        soup = BeautifulSoup(comment['comment_content'], 'html.parser')
                        comment['comment_content'] = soup.get_text().strip()

                    comments.append(comment)
                except Exception as e:
                    logger.warning(f"解析单条评论失败: {e}")
                    continue

        except Exception as e:
            logger.error(f"解析评论JSON失败: {e}")

        return comments

    def save_comments_to_db(self, work_id: int, comments: List[Dict]) -> int:
        """保存评论到数据库"""
        success_count = 0

        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()

            sql = """
            INSERT INTO novel_comments 
            (work_id, user_name, comment_content, comment_time, like_num, chapter_name)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                comment_content = VALUES(comment_content),
                like_num = VALUES(like_num)
            """

            for comment in comments:
                try:
                    cursor.execute(sql, (
                        work_id,
                        comment['user_name'],
                        comment['comment_content'],
                        comment['comment_time'],
                        comment['like_num'],
                        comment['chapter_name']
                    ))
                    success_count += 1
                except Exception as e:
                    logger.warning(f"保存评论失败: {e}")
                    continue

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"保存评论成功: {success_count}/{len(comments)} 条")

        except Exception as e:
            logger.error(f"数据库操作失败: {e}")

        return success_count

    def get_chapter_ids_from_db(self, work_id: int) -> List[str]:
        """从数据库获取章节ID列表"""
        chapter_ids = []

        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # 获取前10章用于测试
            cursor.execute("""
                SELECT chapter_id 
                FROM novel_chapters 
                WHERE work_id = %s 
                ORDER BY chapter_num 
                LIMIT 10
            """, (work_id,))

            results = cursor.fetchall()
            chapter_ids = [str(row[0]) for row in results]

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"获取章节ID失败: {e}")

        return chapter_ids

    def crawl_novel_comments(self, work_id: int, work_url: str, max_pages_per_chapter: int = 5) -> bool:
        """爬取小说评论"""
        logger.info(f"开始爬取评论: ID={work_id}, URL={work_url}")

        try:
            # 1. 提取book_id
            book_id = self.get_book_id_from_url(work_url)
            if not book_id:
                logger.error(f"无法从URL提取book_id: {work_url}")
                return False

            logger.info(f"提取到book_id: {book_id}")

            total_comments_saved = 0

            # 2. 先爬取书评区（全书评论）
            logger.info("开始爬取书评区...")
            for page in range(1, max_pages_per_chapter + 1):
                try:
                    logger.info(f"爬取书评第 {page} 页...")

                    comment_json = self.fetch_global_comments(book_id, page)
                    if not comment_json:
                        logger.warning(f"书评第 {page} 页获取失败")
                        break

                    comments = self.parse_comment_data(comment_json)
                    if not comments:
                        logger.info(f"书评第 {page} 页无数据，停止爬取")
                        break

                    saved_count = self.save_comments_to_db(work_id, comments)
                    total_comments_saved += saved_count

                    logger.info(f"书评第 {page} 页: 获取 {len(comments)} 条，保存 {saved_count} 条")

                    # 检查是否还有更多页
                    data = comment_json.get('data', {})
                    if not data.get('hasNext', True):
                        logger.info("书评区已无更多页")
                        break

                    time.sleep(random.uniform(2, 4))

                except Exception as e:
                    logger.error(f"爬取书评第 {page} 页失败: {e}")
                    continue

            # 3. 爬取章节评论
            logger.info("开始爬取章节评论...")
            chapter_ids = self.get_chapter_ids_from_db(work_id)

            if not chapter_ids:
                logger.warning("未找到章节ID，跳过章节评论爬取")
            else:
                for chapter_idx, chapter_id in enumerate(chapter_ids, 1):
                    try:
                        logger.info(f"爬取章节 {chapter_idx}/{len(chapter_ids)}: ID={chapter_id}")

                        for page in range(1, max_pages_per_chapter + 1):
                            try:
                                logger.info(f"  章节评论第 {page} 页...")

                                comment_json = self.fetch_comments_by_chapter(book_id, chapter_id, page)
                                if not comment_json:
                                    logger.warning(f"章节评论第 {page} 页获取失败")
                                    break

                                comments = self.parse_comment_data(comment_json)
                                if not comments:
                                    logger.info(f"章节评论第 {page} 页无数据，停止爬取")
                                    break

                                saved_count = self.save_comments_to_db(work_id, comments)
                                total_comments_saved += saved_count

                                logger.info(f"  第 {page} 页: 获取 {len(comments)} 条，保存 {saved_count} 条")

                                # 检查是否还有更多页
                                data = comment_json.get('data', {})
                                if not data.get('hasNext', True):
                                    logger.info("  本章节已无更多评论")
                                    break

                                time.sleep(random.uniform(1, 3))

                            except Exception as e:
                                logger.error(f"爬取章节评论第 {page} 页失败: {e}")
                                continue

                        # 章节间休息
                        time.sleep(random.uniform(2, 5))

                    except Exception as e:
                        logger.error(f"爬取章节 {chapter_id} 评论失败: {e}")
                        continue

            logger.info(f"评论爬取完成: ID={work_id}, 总计保存 {total_comments_saved} 条评论")
            return total_comments_saved > 0

        except Exception as e:
            logger.error(f"爬取评论失败: ID={work_id}, 错误: {e}")
            return False


def crawl_multiple_novels():
    """批量爬取多部小说的评论"""
    try:
        # 连接数据库
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 获取前3部有章节数据的小说
        cursor.execute("""
            SELECT DISTINCT b.work_id, b.work_name, b.work_url
            FROM novel_base_info b
            INNER JOIN novel_chapters c ON b.work_id = c.work_id
            WHERE b.platform_name = '起点小说'
            AND b.work_id NOT IN (
                SELECT DISTINCT work_id FROM novel_comments
            )
            ORDER BY b.work_id
            LIMIT 3
        """)

        novels = cursor.fetchall()
        cursor.close()
        conn.close()

        if not novels:
            logger.info("没有找到需要爬取评论的小说")
            return

        logger.info(f"找到 {len(novels)} 部需要爬取评论的小说")

        # 初始化爬虫
        crawler = QidianCommentCrawler()

        # 爬取每部小说
        for novel in novels:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"开始处理: {novel['work_name']} (ID: {novel['work_id']})")

            success = crawler.crawl_novel_comments(
                work_id=novel['work_id'],
                work_url=novel['work_url'],
                max_pages_per_chapter=3  # 每章节最多爬3页评论
            )

            if success:
                logger.info(f"✓ 评论爬取成功: {novel['work_name']}")
            else:
                logger.info(f"✗ 评论爬取失败: {novel['work_name']}")

            # 小说间休息
            time.sleep(random.uniform(10, 20))

        logger.info("\n" + "=" * 60)
        logger.info("批量爬取完成！")

    except Exception as e:
        logger.error(f"批量爬取失败: {e}")


if __name__ == "__main__":
    print("起点小说评论爬虫")
    print("=" * 50)

    # 测试单个小说
    test_work_id = 1036526469
    test_work_url = "https://www.qidian.com/book/1036526469/"

    crawler = QidianCommentCrawler()

    # 测试爬取
    success = crawler.crawl_novel_comments(
        work_id=test_work_id,
        work_url=test_work_url,
        max_pages_per_chapter=2
    )

    if success:
        print(f"✓ 测试爬取成功")
    else:
        print(f"✗ 测试爬取失败")