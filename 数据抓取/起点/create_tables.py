# create_tables.py
import pymysql
from pymysql.err import ProgrammingError

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Chenyixian6453!",
    "database": "novel_analysis",
    "charset": "utf8mb4"
}


def create_tables():
    """创建缺失的数据库表"""

    # 章节表SQL
    chapters_table_sql = """
    CREATE TABLE IF NOT EXISTS novel_chapters (
        chapter_id INT AUTO_INCREMENT PRIMARY KEY COMMENT '章节ID',
        work_id BIGINT NOT NULL COMMENT '关联作品ID',
        chapter_title VARCHAR(255) NOT NULL COMMENT '章节标题',
        chapter_content LONGTEXT NOT NULL COMMENT '章节内容',
        chapter_num INT COMMENT '章节序号',
        update_time VARCHAR(50) COMMENT '章节更新时间',
        crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '爬取时间',
        FOREIGN KEY (work_id) REFERENCES novel_base_info(work_id),
        INDEX idx_work_id (work_id),
        INDEX idx_chapter_num (chapter_num)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='小说章节表'
    """

    # 评论表SQL
    comments_table_sql = """
    CREATE TABLE IF NOT EXISTS novel_comments (
        comment_id INT AUTO_INCREMENT PRIMARY KEY COMMENT '评论ID',
        work_id BIGINT NOT NULL COMMENT '关联作品ID',
        user_name VARCHAR(100) COMMENT '评论用户名',
        comment_content TEXT COMMENT '评论内容',
        comment_time VARCHAR(50) COMMENT '评论时间',
        like_num INT DEFAULT 0 COMMENT '点赞数',
        crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '爬取时间',
        FOREIGN KEY (work_id) REFERENCES novel_base_info(work_id),
        INDEX idx_work_id (work_id),
        INDEX idx_comment_time (comment_time(20))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='小说评论表'
    """

    # 爬取状态表SQL（可选）
    status_table_sql = """
    CREATE TABLE IF NOT EXISTS crawl_status (
        status_id INT AUTO_INCREMENT PRIMARY KEY COMMENT '状态ID',
        work_id BIGINT NOT NULL COMMENT '作品ID',
        crawl_type VARCHAR(20) COMMENT '爬取类型（chapters/comments）',
        last_crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '最后爬取时间',
        crawl_count INT DEFAULT 0 COMMENT '爬取数量',
        status VARCHAR(20) DEFAULT 'pending' COMMENT '状态（pending/success/failed）',
        error_message TEXT COMMENT '错误信息',
        FOREIGN KEY (work_id) REFERENCES novel_base_info(work_id),
        INDEX idx_work_id (work_id),
        INDEX idx_crawl_type (crawl_type),
        INDEX idx_status (status)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='爬取状态记录表'
    """

    try:
        # 连接数据库
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("开始创建数据库表...")

        # 检查并创建章节表
        print("1. 检查novel_chapters表...")
        try:
            cursor.execute("SELECT 1 FROM novel_chapters LIMIT 1")
            print("   ✓ novel_chapters表已存在")
        except ProgrammingError:
            print("   - novel_chapters表不存在，正在创建...")
            cursor.execute(chapters_table_sql)
            print("   ✓ novel_chapters表创建成功")

        # 检查并创建评论表
        print("2. 检查novel_comments表...")
        try:
            cursor.execute("SELECT 1 FROM novel_comments LIMIT 1")
            print("   ✓ novel_comments表已存在")
        except ProgrammingError:
            print("   - novel_comments表不存在，正在创建...")
            cursor.execute(comments_table_sql)
            print("   ✓ novel_comments表创建成功")

        # 检查并创建状态表（可选）
        print("3. 检查crawl_status表...")
        try:
            cursor.execute("SELECT 1 FROM crawl_status LIMIT 1")
            print("   ✓ crawl_status表已存在")
        except ProgrammingError:
            print("   - crawl_status表不存在，正在创建...")
            cursor.execute(status_table_sql)
            print("   ✓ crawl_status表创建成功")

        # 提交事务
        conn.commit()

        # 验证表结构
        print("\n4. 验证表结构...")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        print("  数据库中的所有表:")
        for table in tables:
            print(f"    - {table[0]}")

        # 检查novel_base_info表数据
        cursor.execute("SELECT COUNT(*) as count FROM novel_base_info")
        base_count = cursor.fetchone()[0]
        print(f"\n  小说基础信息表记录数: {base_count}")

        # 关闭连接
        cursor.close()
        conn.close()

        print("\n=== 数据库表创建完成 ===")
        print("可以开始运行爬虫了！")

    except Exception as e:
        print(f"创建表失败: {e}")
        raise


def check_database_schema():
    """检查数据库架构"""
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()

        print("=== 数据库架构检查 ===")

        # 检查所有表
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()

        table_list = [table[0] for table in tables]
        print(f"现有表: {', '.join(table_list)}")

        # 检查每个表的结构
        for table in table_list:
            print(f"\n{table} 表结构:")
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            for column in columns:
                print(f"  {column[0]:20} {column[1]:20} {column[2]}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"检查失败: {e}")


if __name__ == "__main__":
    # 先检查现有架构
    check_database_schema()

    # 询问是否创建表
    choice = input("\n是否要创建缺失的表？(y/n): ")
    if choice.lower() == 'y':
        create_tables()
    else:
        print("已取消创建")