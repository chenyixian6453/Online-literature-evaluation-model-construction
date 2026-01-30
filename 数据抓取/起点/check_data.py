# check_data.py
import pymysql
from pymysql.cursors import DictCursor

# 数据库配置（使用你的配置）
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Chenyixian6453!",
    "database": "novel_analysis",
    "charset": "utf8mb4"
}


def check_database_status():
    """检查数据库状态"""
    print("=== 数据库状态检查 ===")

    try:
        # 连接数据库
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(DictCursor)

        # 1. 检查novel_base_info表
        print("1. 检查novel_base_info表...")
        cursor.execute("SELECT COUNT(*) as count FROM novel_base_info")
        base_count = cursor.fetchone()['count']
        print(f"  小说基本信息记录数: {base_count}")

        # 显示一些示例数据
        cursor.execute("SELECT work_id, work_name, work_url FROM novel_base_info LIMIT 5")
        samples = cursor.fetchall()
        print("  示例数据（前5条）:")
        for sample in samples:
            print(f"    ID:{sample['work_id']} - {sample['work_name']}")

        # 2. 检查novel_chapters表
        print("\n2. 检查novel_chapters表...")
        cursor.execute("SELECT COUNT(*) as count FROM novel_chapters")
        chapters_count = cursor.fetchone()['count']
        print(f"  章节记录数: {chapters_count}")

        # 3. 检查novel_comments表
        print("\n3. 检查novel_comments表...")
        cursor.execute("SELECT COUNT(*) as count FROM novel_comments")
        comments_count = cursor.fetchone()['count']
        print(f"  评论记录数: {comments_count}")

        # 4. 统计各表数据分布
        if chapters_count > 0:
            cursor.execute("""
                SELECT work_id, COUNT(*) as chapter_count 
                FROM novel_chapters 
                GROUP BY work_id 
                LIMIT 10
            """)
            print("\n  各小说章节数量（前10）:")
            for row in cursor.fetchall():
                print(f"    作品ID {row['work_id']}: {row['chapter_count']} 章")

        # 5. 检查未爬取的小说
        print("\n4. 检查待爬取的小说...")
        cursor.execute("""
            SELECT work_id, work_name, work_url 
            FROM novel_base_info 
            WHERE work_id NOT IN (SELECT DISTINCT work_id FROM novel_chapters)
            LIMIT 10
        """)
        uncrawled = cursor.fetchall()
        print(f"  未爬取章节的小说数量（前10）: {len(uncrawled)}")
        for novel in uncrawled:
            print(f"    ID:{novel['work_id']} - {novel['work_name']}")

        # 6. 检查表结构
        print("\n5. 检查表结构...")
        cursor.execute("DESCRIBE novel_base_info")
        print("  novel_base_info表结构:")
        for column in cursor.fetchall():
            print(f"    {column['Field']}: {column['Type']}")

        # 关闭连接
        cursor.close()
        conn.close()

        print("\n=== 检查完成 ===")
        return True

    except Exception as e:
        print(f"检查失败: {e}")
        return False


def check_novel_base_info_quality():
    """检查novel_base_info表数据质量"""
    print("\n=== novel_base_info表数据质量检查 ===")

    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(DictCursor)

        # 检查空值
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN work_name IS NULL OR work_name = '' THEN 1 ELSE 0 END) as empty_names,
                SUM(CASE WHEN work_url IS NULL OR work_url = '' THEN 1 ELSE 0 END) as empty_urls,
                SUM(CASE WHEN author_name IS NULL OR author_name = '' THEN 1 ELSE 0 END) as empty_authors
            FROM novel_base_info
        """)
        empty_stats = cursor.fetchone()
        print(f"  空值统计:")
        print(f"    空作品名: {empty_stats['empty_names']}")
        print(f"    空URL: {empty_stats['empty_urls']}")
        print(f"    空作者名: {empty_stats['empty_authors']}")

        # 检查完结状态分布
        cursor.execute("""
            SELECT completion_status, COUNT(*) as count 
            FROM novel_base_info 
            GROUP BY completion_status
        """)
        print(f"  完结状态分布:")
        for row in cursor.fetchall():
            print(f"    {row['completion_status']}: {row['count']}")

        # 检查平台分布
        cursor.execute("""
            SELECT platform_name, COUNT(*) as count 
            FROM novel_base_info 
            GROUP BY platform_name
        """)
        print(f"  平台分布:")
        for row in cursor.fetchall():
            print(f"    {row['platform_name']}: {row['count']}")

        # 检查题材分类分布
        cursor.execute("""
            SELECT subject_category, COUNT(*) as count 
            FROM novel_base_info 
            GROUP BY subject_category 
            ORDER BY count DESC 
            LIMIT 10
        """)
        print(f"  热门题材分类（前10）:")
        for row in cursor.fetchall():
            print(f"    {row['subject_category']}: {row['count']}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"数据质量检查失败: {e}")


def check_url_pattern():
    """检查URL格式"""
    print("\n=== URL格式检查 ===")

    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(DictCursor)

        cursor.execute("SELECT work_id, work_url FROM novel_base_info LIMIT 10")
        print("  URL示例（前10）:")
        for row in cursor.fetchall():
            url = row['work_url']
            is_valid = url.startswith('http') and 'qidian.com' in url
            status = "✓" if is_valid else "✗"
            print(f"    {status} ID:{row['work_id']}: {url[:50]}...")

        # 统计无效URL
        cursor.execute("""
            SELECT COUNT(*) as invalid_count 
            FROM novel_base_info 
            WHERE work_url NOT LIKE 'http%qidian.com%'
        """)
        invalid_count = cursor.fetchone()['invalid_count']
        print(f"  无效URL数量: {invalid_count}")

        cursor.close()
        conn.close()

    except Exception as e:
        print(f"URL检查失败: {e}")


if __name__ == "__main__":
    # 执行所有检查
    check_database_status()
    check_novel_base_info_quality()
    check_url_pattern()

    print("\n" + "=" * 50)
    print("所有检查完成！")
    print("=" * 50)