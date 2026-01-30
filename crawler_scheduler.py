# crawler_scheduler.py
import pymysql
import time
import random
import logging
from datetime import datetime
from simple_crawler import SimpleQidianCrawler
from qidian_comment_crawler import QidianCommentCrawler

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_scheduler.log', encoding='utf-8'),
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


class CrawlerScheduler:
    def __init__(self):
        self.chapter_crawler = SimpleQidianCrawler()
        self.comment_crawler = QidianCommentCrawler()

    def update_crawl_status(self, work_id: int, crawl_type: str, status: str,
                            count: int = 0, error_msg: str = ""):
        """更新爬取状态"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()

            sql = """
            INSERT INTO crawl_status 
            (work_id, crawl_type, status, crawl_count, error_message)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_crawl_time = CURRENT_TIMESTAMP,
                status = VALUES(status),
                crawl_count = VALUES(crawl_count),
                error_message = VALUES(error_message)
            """

            cursor.execute(sql, (work_id, crawl_type, status, count, error_msg))
            conn.commit()

            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"更新状态失败: {e}")

    def get_pending_novels(self, limit: int = 10) -> list:
        """获取待爬取的小说"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor(pymysql.cursors.DictCursor)

            # 优先爬取没有章节数据和评论数据的小说
            cursor.execute("""
                SELECT b.work_id, b.work_name, b.work_url
                FROM novel_base_info b
                WHERE b.platform_name = '起点小说'
                AND (
                    b.work_id NOT IN (SELECT DISTINCT work_id FROM novel_chapters)
                    OR b.work_id NOT IN (SELECT DISTINCT work_id FROM novel_comments)
                )
                ORDER BY 
                    CASE 
                        WHEN b.work_id NOT IN (SELECT DISTINCT work_id FROM novel_chapters) THEN 1
                        ELSE 2
                    END,
                    b.work_id
                LIMIT %s
            """, (limit,))

            novels = cursor.fetchall()
            cursor.close()
            conn.close()

            return novels

        except Exception as e:
            logger.error(f"获取待爬取小说失败: {e}")
            return []

    def crawl_single_novel_comprehensive(self, work_id: int, work_name: str, work_url: str):
        """完整爬取单部小说（章节+评论）"""
        logger.info(f"开始完整爬取: {work_name} (ID: {work_id})")

        # 1. 先爬取章节
        logger.info("第一阶段：爬取章节...")
        try:
            chapter_success = self.chapter_crawler.crawl_novel(
                work_id=work_id,
                work_url=work_url,
                max_chapters=20  # 每部小说爬20章
            )

            if chapter_success:
                self.update_crawl_status(work_id, "chapters", "success", 20)
                logger.info("✓ 章节爬取成功")
            else:
                self.update_crawl_status(work_id, "chapters", "failed", 0, "章节爬取失败")
                logger.warning("✗ 章节爬取失败")

        except Exception as e:
            self.update_crawl_status(work_id, "chapters", "failed", 0, str(e))
            logger.error(f"章节爬取异常: {e}")

        # 2. 等待一段时间后爬取评论
        time.sleep(random.uniform(15, 30))

        logger.info("第二阶段：爬取评论...")
        try:
            comment_success = self.comment_crawler.crawl_novel_comments(
                work_id=work_id,
                work_url=work_url,
                max_pages_per_chapter=3
            )

            if comment_success:
                self.update_crawl_status(work_id, "comments", "success")
                logger.info("✓ 评论爬取成功")
            else:
                self.update_crawl_status(work_id, "comments", "failed", 0, "评论爬取失败")
                logger.warning("✗ 评论爬取失败")

        except Exception as e:
            self.update_crawl_status(work_id, "comments", "failed", 0, str(e))
            logger.error(f"评论爬取异常: {e}")

        logger.info(f"完整爬取完成: {work_name}")
        return chapter_success or comment_success

    def run_batch_crawl(self, batch_size: int = 5, delay_between_novels: int = 30):
        """批量爬取多部小说"""
        logger.info(f"开始批量爬取，每批 {batch_size} 部小说")

        while True:
            # 获取待爬取的小说
            pending_novels = self.get_pending_novels(batch_size)

            if not pending_novels:
                logger.info("所有小说已爬取完成！")
                break

            logger.info(f"本批次将爬取 {len(pending_novels)} 部小说")

            # 爬取每部小说
            for idx, novel in enumerate(pending_novels, 1):
                logger.info(f"\n{'=' * 60}")
                logger.info(f"第 {idx}/{len(pending_novels)} 部: {novel['work_name']}")

                try:
                    success = self.crawl_single_novel_comprehensive(
                        work_id=novel['work_id'],
                        work_name=novel['work_name'],
                        work_url=novel['work_url']
                    )

                    if success:
                        logger.info(f"✓ 第 {idx} 部小说爬取完成")
                    else:
                        logger.warning(f"✗ 第 {idx} 部小说爬取失败")

                except Exception as e:
                    logger.error(f"第 {idx} 部小说爬取异常: {e}")

                # 小说间休息
                if idx < len(pending_novels):
                    logger.info(f"等待 {delay_between_novels} 秒后继续...")
                    time.sleep(delay_between_novels)

            # 批次间休息
            logger.info(f"\n本批次完成，等待 60 秒后检查下一批...")
            time.sleep(60)

        logger.info("批量爬取全部完成！")

    def run_incremental_crawl(self, check_interval: int = 3600):
        """增量爬取模式（定时检查）"""
        logger.info(f"启动增量爬取模式，检查间隔: {check_interval} 秒")

        while True:
            try:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"\n{'=' * 60}")
                logger.info(f"开始增量检查: {current_time}")

                # 获取需要更新的小说
                conn = pymysql.connect(**DB_CONFIG)
                cursor = conn.cursor(pymysql.cursors.DictCursor)

                # 查找最近3天内有更新但未爬取的小说
                cursor.execute("""
                    SELECT b.work_id, b.work_name, b.work_url
                    FROM novel_base_info b
                    LEFT JOIN crawl_status cs ON b.work_id = cs.work_id 
                    AND cs.crawl_type = 'chapters'
                    WHERE b.platform_name = '起点小说'
                    AND (
                        cs.last_crawl_time IS NULL 
                        OR cs.last_crawl_time < DATE_SUB(NOW(), INTERVAL 3 DAY)
                    )
                    ORDER BY cs.last_crawl_time ASC NULLS FIRST
                    LIMIT 3
                """)

                novels_to_update = cursor.fetchall()
                cursor.close()
                conn.close()

                if novels_to_update:
                    logger.info(f"找到 {len(novels_to_update)} 部需要更新的小说")

                    for novel in novels_to_update:
                        logger.info(f"更新: {novel['work_name']}")

                        try:
                            self.crawl_single_novel_comprehensive(
                                work_id=novel['work_id'],
                                work_name=novel['work_name'],
                                work_url=novel['work_url']
                            )
                        except Exception as e:
                            logger.error(f"更新失败: {novel['work_name']}, 错误: {e}")

                        time.sleep(random.uniform(20, 40))

                else:
                    logger.info("没有需要更新的小说")

                logger.info(f"等待 {check_interval} 秒后再次检查...")
                time.sleep(check_interval)

            except KeyboardInterrupt:
                logger.info("收到中断信号，停止增量爬取")
                break
            except Exception as e:
                logger.error(f"增量爬取异常: {e}")
                time.sleep(300)  # 异常后等待5分钟重试


def main():
    """主调度函数"""
    print("起点小说爬虫调度系统")
    print("=" * 50)
    print("1. 批量爬取模式")
    print("2. 增量爬取模式")
    print("3. 测试单部小说")
    print("4. 检查爬取状态")

    choice = input("\n请选择模式 (1-4): ").strip()

    scheduler = CrawlerScheduler()

    if choice == "1":
        batch_size = int(input("请输入每批爬取数量 (默认5): ") or "5")
        scheduler.run_batch_crawl(batch_size=batch_size)

    elif choice == "2":
        interval = int(input("请输入检查间隔(秒，默认3600): ") or "3600")
        scheduler.run_incremental_crawl(check_interval=interval)

    elif choice == "3":
        # 测试单部小说
        work_id = 1036526469
        work_name = "长生从炼丹宗师开始"
        work_url = "https://www.qidian.com/book/1036526469/"

        success = scheduler.crawl_single_novel_comprehensive(
            work_id=work_id,
            work_name=work_name,
            work_url=work_url
        )

        if success:
            print(f"✓ 测试爬取成功")
        else:
            print(f"✗ 测试爬取失败")

    elif choice == "4":
        # 检查状态
        check_crawl_status()

    else:
        print("无效选择")


def check_crawl_status():
    """检查爬取状态"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        print("\n=== 爬取状态统计 ===")

        # 总体统计
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT work_id) as total_novels,
                SUM(CASE WHEN cs.status = 'success' THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN cs.status = 'failed' THEN 1 ELSE 0 END) as failed_count,
                SUM(CASE WHEN cs.status IS NULL THEN 1 ELSE 0 END) as pending_count
            FROM novel_base_info b
            LEFT JOIN crawl_status cs ON b.work_id = cs.work_id 
            AND cs.crawl_type = 'chapters'
            WHERE b.platform_name = '起点小说'
        """)

        stats = cursor.fetchone()
        print(f"小说总数: {stats['total_novels']}")
        print(f"已成功爬取: {stats['success_count']}")
        print(f"爬取失败: {stats['failed_count']}")
        print(f"待爬取: {stats['pending_count']}")

        # 章节统计
        cursor.execute("SELECT COUNT(*) as count FROM novel_chapters")
        chapters_count = cursor.fetchone()['count']
        print(f"\n章节总数: {chapters_count}")

        cursor.execute("SELECT COUNT(DISTINCT work_id) as count FROM novel_chapters")
        novels_with_chapters = cursor.fetchone()['count']
        print(f"已爬取章节的小说数: {novels_with_chapters}")

        # 评论统计
        cursor.execute("SELECT COUNT(*) as count FROM novel_comments")
        comments_count = cursor.fetchone()['count']
        print(f"\n评论总数: {comments_count}")

        cursor.execute("SELECT COUNT(DISTINCT work_id) as count FROM novel_comments")
        novels_with_comments = cursor.fetchone()['count']
        print(f"已爬取评论的小说数: {novels_with_comments}")

        # 最近爬取记录
        print("\n=== 最近爬取记录 ===")
        cursor.execute("""
            SELECT 
                cs.work_id,
                b.work_name,
                cs.crawl_type,
                cs.status,
                cs.last_crawl_time,
                cs.crawl_count,
                cs.error_message
            FROM crawl_status cs
            JOIN novel_base_info b ON cs.work_id = b.work_id
            ORDER BY cs.last_crawl_time DESC
            LIMIT 10
        """)

        for record in cursor.fetchall():
            status_icon = "✓" if record['status'] == 'success' else "✗"
            print(f"{status_icon} {record['work_name']} - {record['crawl_type']} - {record['last_crawl_time']}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"检查状态失败: {e}")


if __name__ == "__main__":
    main()