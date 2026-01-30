# check_db_final.py
import pymysql

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Chenyixian6453!",
    "database": "novel_analysis",
    "charset": "utf8mb4"
}


def final_check():
    """最终检查"""
    print("=== 最终数据库检查 ===")

    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        # 1. 检查所有表
        cursor.execute("SHOW TABLES")
        tables = [row['Tables_in_novel_analysis'] for row in cursor.fetchall()]
        print(f"1. 数据库中的表: {tables}")

        # 2. 检查表数据量
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
            count = cursor.fetchone()['count']
            print(f"   {table}: {count} 条记录")

        # 3. 检查小说基础信息
        cursor.execute("""
            SELECT work_id, work_name, completion_status, reference_value 
            FROM novel_base_info 
            WHERE platform_name = '起点小说'
            LIMIT 5
        """)
        print("\n2. 小说示例（前5部）:")
        for row in cursor.fetchall():
            status = "已完结" if row['completion_status'] == '完结' else "连载中"
            print(f"   {row['work_id']} - {row['work_name']} ({status}) - 推荐: {row['reference_value']}")

        # 4. 检查章节和评论表结构
        if 'novel_chapters' in tables:
            cursor.execute("DESCRIBE novel_chapters")
            print("\n3. novel_chapters表结构:")
            for row in cursor.fetchall():
                print(f"   {row['Field']}: {row['Type']} {'(PK)' if row['Key'] == 'PRI' else ''}")

        if 'novel_comments' in tables:
            cursor.execute("DESCRIBE novel_comments")
            print("\n4. novel_comments表结构:")
            for row in cursor.fetchall():
                print(f"   {row['Field']}: {row['Type']} {'(PK)' if row['Key'] == 'PRI' else ''}")

        cursor.close()
        conn.close()

        print("\n=== 检查完成 ===")
        print("数据库准备就绪，可以开始爬取！")

    except Exception as e:
        print(f"检查失败: {e}")


if __name__ == "__main__":
    final_check()