from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time
import random
import pymysql
import logging
import os
import json
from datetime import datetime
import re
import sys

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'crawler_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 数据库配置
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Chenyixian6453!",
    "database": "novel_analysis",
    "charset": "utf8mb4"
}


class MobileQidianCrawler:
    def __init__(self, headless=False):
        """初始化爬虫"""
        self.headless = headless
        self.driver = None
        self.init_driver()
        self.setup_database()

    def setup_database(self):
        """重新创建适配的数据库表结构（补充chapter_url/content_length/crawl_time字段）"""
        try:
            # 先创建数据库（如果不存在）
            conn = pymysql.connect(
                host=DB_CONFIG['host'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                charset=DB_CONFIG['charset']
            )
            cursor = conn.cursor()
            cursor.execute(
                "CREATE DATABASE IF NOT EXISTS novel_analysis DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
            cursor.execute("USE novel_analysis;")

            # 创建小说基础信息表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS novel_base_info (
                    work_id BIGINT PRIMARY KEY COMMENT '作品唯一ID',
                    work_name VARCHAR(255) NOT NULL COMMENT '作品名称',
                    author_name VARCHAR(100) NOT NULL COMMENT '作者名',
                    platform_name VARCHAR(50) NOT NULL COMMENT '平台名称',
                    work_url VARCHAR(500) NOT NULL COMMENT '作品原始链接',
                    category VARCHAR(50) NOT NULL COMMENT '题材分类',
                    tags VARCHAR(200) NOT NULL COMMENT '筛选标签',
                    completion_status VARCHAR(20) NOT NULL COMMENT '完结状态',
                    reference_value VARCHAR(50) NOT NULL COMMENT '传统指标参考值',
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='小说基础信息表';
            """)

            # 创建小说章节表（补充chapter_url/content_length/crawl_time字段）
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS novel_chapters (
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    work_id BIGINT NOT NULL COMMENT '小说ID',
                    chapter_title VARCHAR(255) NOT NULL COMMENT '章节标题',
                    chapter_content TEXT COMMENT '章节内容',
                    chapter_num VARCHAR(50) COMMENT '章节序号（支持引子/序章等）',
                    update_time VARCHAR(50) COMMENT '更新时间',
                    chapter_url VARCHAR(500) COMMENT '章节链接',
                    content_length INT COMMENT '内容长度（字符数）',
                    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '爬取时间',
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
                    UNIQUE KEY idx_work_chapter (work_id, chapter_num) COMMENT '防重复'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # 创建小说评论表
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS novel_comments (
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    work_id BIGINT NOT NULL COMMENT '小说ID',
                    user_name VARCHAR(100) COMMENT '用户名',
                    comment_content TEXT COMMENT '评论内容',
                    comment_time VARCHAR(50) COMMENT '评论时间',
                    like_num INT DEFAULT 0 COMMENT '点赞数',
                    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '入库时间',
                    KEY idx_work_id (work_id) COMMENT '索引'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)

            # ========== 新增：创建爬取文件存储表 ==========
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS novel_crawl_files (
                    id INT AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
                    work_id BIGINT NOT NULL COMMENT '小说ID',
                    file_name VARCHAR(255) NOT NULL COMMENT '本地文件名',
                    file_content LONGTEXT COMMENT '文件完整内容',
                    file_size INT COMMENT '文件大小(字节)',
                    crawl_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '爬取时间',
                    KEY idx_work_id (work_id) COMMENT '索引'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='爬取文件内容存储表';
            """)

            conn.commit()
            cursor.close()
            conn.close()

            logger.info("数据库表结构已按要求创建/更新完成（含chapter_url/content_length/crawl_time）")

        except Exception as e:
            logger.error(f"数据库设置失败: {e}")
            raise

    def init_driver(self):
        """初始化Chrome驱动 - 模拟移动端"""
        try:
            chrome_options = Options()

            if self.headless:
                chrome_options.add_argument('--headless')

            # 模拟移动端设备
            mobile_emulation = {
                "deviceMetrics": {"width": 375, "height": 812, "pixelRatio": 3.0},
                "userAgent": "Mozilla/5.0 (iPhone; CPU iPhone OS 14_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1"
            }
            chrome_options.add_experimental_option("mobileEmulation", mobile_emulation)

            # 基础配置
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')

            # 隐藏自动化特征
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')

            # 创建驱动
            self.driver = webdriver.Chrome(options=chrome_options)

            # 执行隐藏脚本
            hide_script = """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """
            self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
                'source': hide_script
            })

            self.driver.set_page_load_timeout(20)

            logger.info("Chrome驱动初始化成功（移动端模拟）")

        except Exception as e:
            logger.error(f"初始化Chrome驱动失败: {e}")
            raise

    def fix_url_to_mobile(self, url):
        """将PC URL转换为移动端URL"""
        if 'www.qidian.com' in url:
            return url.replace('www.qidian.com', 'm.qidian.com')
        elif 'm.qidian.com' in url:
            return url
        else:
            match = re.search(r'/chapter/(\d+)/(\d+)/', url)
            if match:
                book_id = match.group(1)
                chapter_id = match.group(2)
                return f'https://m.qidian.com/chapter/{book_id}/{chapter_id}/'
        return url

    def _clean_chapter_title(self, title):
        """清洗章节标题"""
        if not title:
            return ""
        title = re.sub(r'_小说在线阅读 - 起点中文网手机版', '', title)
        title = re.sub(r'\d{4}-\d{2}-\d{2}\s*\d{2}:\d{2}:\d{2}', '', title)
        redundant_words = ['作家入驻', '即更即看', '还有番外', '免费', 'VIP']
        for word in redundant_words:
            title = title.replace(word, '')
        title = re.sub(r'\s+', ' ', title).strip()
        title = re.sub(r'·+', '', title).strip()
        return title

    def _extract_chapter_number(self, title):
        """提取真实章节号（适配VARCHAR字段）"""
        if not title:
            return "未知章节"
        chapter_pattern = re.compile(r'第(\d+)章')
        match = chapter_pattern.search(title)
        if match:
            return f"第{match.group(1)}章"
        special_chapters = ['引子', '序章', '终章', '番外', '后记', '楔子']
        for chap in special_chapters:
            if chap in title:
                return chap
        return title[:10] if len(title) > 10 else title

    def _extract_novel_base_info(self, book_id):
        """从目录页提取小说基础信息"""
        try:
            catalog_url = f"https://m.qidian.com/book/{book_id}/catalog"
            self.driver.get(catalog_url)
            time.sleep(3)

            # 提取小说名称
            work_name = "未知小说"
            name_elems = self.driver.find_elements(By.CSS_SELECTOR, '.book-name, h1, .title')
            if name_elems:
                work_name = name_elems[0].text.strip()

            # 提取作者名
            author_name = "未知作者"
            author_elems = self.driver.find_elements(By.CSS_SELECTOR, '.author, .writer, .book-author')
            if author_elems:
                author_name = author_elems[0].text.strip()

            # 其他基础信息（默认值，可后续从详情页补充）
            platform_name = "起点中文网"
            work_url = f"https://m.qidian.com/book/{book_id}/"
            category = "未知分类"
            tags = "未知标签"
            completion_status = "连载中"
            reference_value = "0"

            return {
                "work_id": book_id,
                "work_name": work_name,
                "author_name": author_name,
                "platform_name": platform_name,
                "work_url": work_url,
                "category": category,
                "tags": tags,
                "completion_status": completion_status,
                "reference_value": reference_value
            }
        except Exception as e:
            logger.error(f"提取小说基础信息失败: {e}")
            # 返回默认值
            return {
                "work_id": book_id,
                "work_name": f"未知小说_{book_id}",
                "author_name": "未知作者",
                "platform_name": "起点中文网",
                "work_url": f"https://m.qidian.com/book/{book_id}/",
                "category": "未知分类",
                "tags": "未知标签",
                "completion_status": "连载中",
                "reference_value": "0"
            }

    def _save_novel_base_info(self, base_info):
        """写入小说基础信息到novel_base_info表"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # 先查询是否存在，不存在则插入，存在则更新
            cursor.execute("SELECT work_id FROM novel_base_info WHERE work_id = %s", (base_info['work_id'],))
            if cursor.fetchone():
                # 更新
                sql = """
                    UPDATE novel_base_info 
                    SET work_name=%s, author_name=%s, platform_name=%s, work_url=%s,
                        category=%s, tags=%s, completion_status=%s, reference_value=%s
                    WHERE work_id=%s
                """
                cursor.execute(sql, (
                    base_info['work_name'], base_info['author_name'], base_info['platform_name'],
                    base_info['work_url'], base_info['category'], base_info['tags'],
                    base_info['completion_status'], base_info['reference_value'],
                    base_info['work_id']
                ))
                logger.info(f"更新小说基础信息: {base_info['work_id']} - {base_info['work_name']}")
            else:
                # 插入
                sql = """
                    INSERT INTO novel_base_info 
                    (work_id, work_name, author_name, platform_name, work_url,
                     category, tags, completion_status, reference_value)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    base_info['work_id'], base_info['work_name'], base_info['author_name'],
                    base_info['platform_name'], base_info['work_url'], base_info['category'],
                    base_info['tags'], base_info['completion_status'], base_info['reference_value']
                ))
                logger.info(f"插入小说基础信息: {base_info['work_id']} - {base_info['work_name']}")

            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"写入小说基础信息失败: {e}")

    # ========== 新增：将爬取文件内容存入数据库 ==========
    def _save_crawl_file_to_db(self, book_id, file_path):
        """将爬取生成的txt文件内容存入数据库"""
        try:
            # 读取本地文件内容
            with open(file_path, 'r', encoding='utf-8') as f:
                file_content = f.read()
            file_size = os.path.getsize(file_path)  # 获取文件大小
            file_name = os.path.basename(file_path)  # 获取文件名

            # 写入数据库
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # 防重复插入
            cursor.execute(
                "SELECT id FROM novel_crawl_files WHERE work_id = %s AND file_name = %s",
                (book_id, file_name)
            )
            if not cursor.fetchone():
                sql = """
                    INSERT INTO novel_crawl_files 
                    (work_id, file_name, file_content, file_size)
                    VALUES (%s, %s, %s, %s)
                """
                cursor.execute(sql, (book_id, file_name, file_content, file_size))
                conn.commit()
                logger.info(f"文件内容已存入数据库: {file_name}")
            else:
                logger.info(f"文件已存在，跳过写入: {file_name}")

            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"写入文件内容到数据库失败: {e}")

    def _save_chapter_to_db(self, book_id, chapter_data):
        """写入章节内容到novel_chapters表（补充chapter_url/content_length/crawl_time）"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # 提取章节信息（补充缺失字段）
            chapter_title = chapter_data['title']
            chapter_content = chapter_data['data']['content']
            chapter_num = chapter_data['chapter_num']
            update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            chapter_url = chapter_data['data']['url']  # 章节链接
            content_length = chapter_data['data']['content_length']  # 内容长度
            crawl_time = datetime.now()  # 爬取时间

            # 防重复插入
            cursor.execute(
                "SELECT id FROM novel_chapters WHERE work_id = %s AND chapter_num = %s",
                (book_id, chapter_num)
            )
            if not cursor.fetchone():
                sql = """
                    INSERT INTO novel_chapters 
                    (work_id, chapter_title, chapter_content, chapter_num, update_time,
                     chapter_url, content_length, crawl_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    book_id, chapter_title, chapter_content, chapter_num, update_time,
                    chapter_url, content_length, crawl_time
                ))
                conn.commit()
                logger.info(f"章节写入数据库成功: {book_id} - {chapter_num}（含URL/长度/爬取时间）")
            else:
                logger.info(f"章节已存在，跳过写入: {book_id} - {chapter_num}")

            cursor.close()
            conn.close()
        except Exception as e:
            logger.error(f"写入章节到数据库失败: {e}")

    def get_mobile_chapter_content(self, url, chapter_tag=None):
        """获取移动端章节内容"""
        try:
            mobile_url = self.fix_url_to_mobile(url)
            logger.info(f"访问移动端章节: {mobile_url}")

            self.driver.get(mobile_url)
            time.sleep(3)

            page_title = self.driver.title
            logger.info(f"移动端页面标题: {page_title}")

            # 处理登录弹窗
            page_source = self.driver.page_source
            if '登录' in page_source or 'login' in page_source:
                logger.warning("可能需要登录")
                try:
                    close_buttons = self.driver.find_elements(By.CSS_SELECTOR,
                                                              '.close, .ui-dialog-close, [aria-label="关闭"]')
                    for btn in close_buttons:
                        if btn.is_displayed():
                            btn.click()
                            time.sleep(1)
                except:
                    pass

            # 滚动加载内容
            for i in range(3):
                scroll_height = (i + 1) * 0.3
                self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_height});")
                time.sleep(1)

            # 提取内容
            content = self._extract_mobile_content()
            if not content or len(content) < 300:
                logger.info("标准提取方法失败，尝试备用方法")
                content = self._extract_mobile_content_fallback()

            # 清理内容
            if content:
                content = self._clean_mobile_content(content)
                logger.info(f"提取到内容: {len(content)} 字符")
            else:
                logger.warning("未能提取到内容")

            # 判断VIP
            is_vip = False
            try:
                if chapter_tag:
                    if chapter_tag == 'VIP':
                        is_vip = True
                    elif chapter_tag == '免费':
                        is_vip = False
                else:
                    tag_selectors = ['.chapter-tag', '.vip-tag', '.free-tag', '.read-tag']
                    tag_text = ''
                    for selector in tag_selectors:
                        tags = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        for tag in tags:
                            tag_text = tag.text.strip()
                            if tag_text == 'VIP':
                                is_vip = True
                                break
                            elif tag_text == '免费':
                                is_vip = False
                                break
                        if tag_text:
                            break

                    if not tag_text and '免费' in page_title:
                        is_vip = False

                    if not tag_text and not is_vip:
                        content_area = self.driver.find_elements(By.CSS_SELECTOR,
                                                                 'div.read-content, div.chapter-content, .content')
                        content_source = content_area[0].get_attribute('innerHTML') if content_area else ''
                        vip_keywords = ['本章需订阅', 'VIP会员专享', 'VIP章节', '订阅本章']
                        is_vip = any(keyword in content_source for keyword in vip_keywords)
            except Exception as e:
                logger.warning(f"VIP标识判断错误: {e}")
                content_area = self.driver.find_elements(By.CSS_SELECTOR,
                                                         'div.read-content, div.chapter-content, .content')
                content_source = content_area[0].get_attribute('innerHTML') if content_area else page_source
                is_vip = '本章需订阅' in content_source or 'VIP会员专享' in content_source

            clean_title = self._clean_chapter_title(page_title)

            return {
                'title': clean_title,
                'content': content or "",
                'url': self.driver.current_url,  # 章节实际URL
                'is_vip': is_vip,
                'needs_login': False,
                'content_length': len(content) if content else 0  # 内容长度
            }

        except Exception as e:
            logger.error(f"获取移动端章节内容失败: {e}")
            return None

    def _extract_mobile_content(self):
        """提取移动端内容"""
        mobile_selectors = [
            'div.read-content',
            'div.chapter-content',
            'div.read-section',
            '.read-content',
            '.chapter-content',
            '.read-section',
            '.chapter-entity',
            '.chapter-text',
            '#chapterContent',
            '.content'
        ]

        for selector in mobile_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    try:
                        if element.is_displayed():
                            text = element.text.strip()
                            if text and len(text) > 300:
                                logger.info(f"使用选择器 {selector} 找到内容")
                                return text
                    except:
                        continue
            except:
                continue

        return ""

    def _extract_mobile_content_fallback(self):
        """备用提取方法"""
        try:
            js_script = """
            function extractMobileContent() {
                var selectors = ['div.read-content', 'div.chapter-content', '.read-section', '.chapter-entity', '.chapter-text'];
                for (var i = 0; i < selectors.length; i++) {
                    var elements = document.querySelectorAll(selectors[i]);
                    for (var j = 0; j < elements.length; j++) {
                        var text = elements[j].textContent || elements[j].innerText;
                        if (text && text.trim().length > 500) {
                            return text.trim();
                        }
                    }
                }

                var allDivs = document.querySelectorAll('div');
                var bestDiv = null;
                var bestLength = 0;

                for (var k = 0; k < allDivs.length; k++) {
                    var text = allDivs[k].textContent || allDivs[k].innerText;
                    var trimmed = text.trim();

                    if (trimmed.length > bestLength && trimmed.length < 20000) {
                        var hasChinese = /[\\u4e00-\\u9fff]/.test(trimmed);
                        var hasPunctuation = /[。！？，；：]/.test(trimmed);
                        var hasParagraphs = trimmed.includes('\\n\\n') || trimmed.split('\\n').length > 5;

                        if (hasChinese && hasPunctuation && hasParagraphs) {
                            bestLength = trimmed.length;
                            bestDiv = allDivs[k];
                        }
                    }
                }

                return bestDiv ? (bestDiv.textContent || bestDiv.innerText).trim() : '';
            }
            return extractMobileContent();
            """

            content = self.driver.execute_script(js_script)
            if content and len(content) > 300:
                logger.info("JavaScript提取到移动端内容")
                return content

            # 段落过滤
            try:
                body = self.driver.find_element(By.TAG_NAME, 'body')
                all_text = body.text
                paragraphs = [p.strip() for p in all_text.split('\n') if p.strip()]
                content_paragraphs = []
                for para in paragraphs:
                    if 100 < len(para) < 3000 and re.search(r'[\u4e00-\u9fff]', para):
                        if not any(keyword in para for keyword in ['上一章', '下一章', '目录', '登录', '广告']):
                            content_paragraphs.append(para)
                if content_paragraphs:
                    content = '\n'.join(content_paragraphs)
                    logger.info(f"段落过滤提取到内容: {len(content)} 字符")
                    return content
            except:
                pass

            return ""

        except Exception as e:
            logger.error(f"备用提取方法失败: {e}")
            return ""

    def _clean_mobile_content(self, content):
        """清理内容"""
        if not content:
            return ""

        lines = content.split('\n')
        cleaned_lines = []
        hard_filters = ['广告ADVERTISEMENT', '立即登录', 'VIP会员专享', '本章需订阅', '举报', '指南', '旧版', '反馈']

        for line in lines:
            line = line.strip()
            if not line:
                continue

            should_skip = False
            for filter_text in hard_filters:
                if filter_text in line:
                    should_skip = True
                    break

            if len(line) < 10 and not re.search(r'[\u4e00-\u9fff]', line):
                should_skip = True

            if not should_skip:
                cleaned_lines.append(line)

        result = '\n'.join(cleaned_lines)
        while '\n\n\n' in result:
            result = result.replace('\n\n\n', '\n\n')

        return result

    def find_chapters_from_mobile(self, book_id):
        """查找移动端章节"""
        try:
            mobile_catalog_url = f"https://m.qidian.com/book/{book_id}/catalog"
            logger.info(f"访问移动端目录页: {mobile_catalog_url}")

            self.driver.get(mobile_catalog_url)
            time.sleep(3)

            # 滚动加载
            for i in range(3):
                self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {0.3 * (i + 1)});")
                time.sleep(1)

            # JS提取章节
            js_script = """
            function findMobileChapters() {
                var chapters = [];
                var allLinks = document.querySelectorAll('a');

                for (var i = 0; i < allLinks.length; i++) {
                    var link = allLinks[i];
                    var href = link.getAttribute('href') || '';
                    var text = link.textContent.trim();

                    if (!href || !text || text.length < 2) continue;

                    var isChapter = href.includes('/chapter/') || href.includes('/book/') || href.includes('read.qidian.com');
                    var isNotNav = !text.includes('上一章') && !text.includes('下一章') && !text.includes('目录') && !text.includes('开始阅读');

                    if (isChapter && isNotNav) {
                        if (href.startsWith('//')) {
                            href = 'https:' + href;
                        } else if (href.startsWith('/')) {
                            href = 'https://m.qidian.com' + href;
                        } else if (!href.startsWith('http')) {
                            href = 'https://m.qidian.com' + href;
                        }

                        var tag = '';
                        var parent = link.parentElement;
                        var tagEle = parent.querySelector('.chapter-tag, .vip-tag, .free-tag, span.tag, .tag-vip, .tag-free');
                        if (tagEle) {
                            tag = tagEle.textContent.trim();
                        }
                        if (!tag) {
                            var siblingTags = link.parentNode.querySelectorAll('.chapter-tag, .vip-tag, .free-tag');
                            if (siblingTags.length > 0) {
                                tag = siblingTags[0].textContent.trim();
                            }
                        }

                        chapters.push({
                            href: href,
                            text: text,
                            tag: tag
                        });
                    }
                }

                var uniqueChapters = [];
                var seen = {};
                for (var j = 0; j < chapters.length; j++) {
                    var chap = chapters[j];
                    if (!seen[chap.href]) {
                        seen[chap.href] = true;
                        uniqueChapters.push(chap);
                    }
                }

                return uniqueChapters.slice(0, 30);
            }
            return findMobileChapters();
            """

            chapters = self.driver.execute_script(js_script)
            if not chapters:
                chapters = self._find_mobile_chapters_backup()

            logger.info(f"从移动端找到 {len(chapters)} 个章节")
            for i, chap in enumerate(chapters[:3]):
                tag_info = f" (标签: {chap.get('tag', '无')})" if chap.get('tag') else ""
                logger.info(f"  第{i + 1}章: {chap['text']}{tag_info}")

            return chapters

        except Exception as e:
            logger.error(f"查找移动端章节失败: {e}")
            return []

    def _find_mobile_chapters_backup(self):
        """备用章节查找"""
        try:
            chapters = []
            all_links = self.driver.find_elements(By.TAG_NAME, 'a')

            for link in all_links:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.text.strip()

                    if not href or not text:
                        continue

                    if href.startswith('//'):
                        href = 'https:' + href
                    elif href.startswith('/'):
                        href = 'https://m.qidian.com' + href

                    if ('/chapter/' in href or '/book/' in href) and text:
                        if text not in ['上一章', '下一章', '目录', '开始阅读']:
                            tag = ''
                            try:
                                parent = link.parentElement
                                tagEle = parent.find_element(By.CSS_SELECTOR,
                                                             '.chapter-tag, .vip-tag, .free-tag, span.tag')
                                tag = tagEle.text.strip()
                            except:
                                pass

                            chapters.append({
                                'href': href,
                                'text': text,
                                'tag': tag
                            })
                except:
                    continue

            return chapters[:30]
        except:
            return []

    def crawl_mobile_novel(self, book_id, max_chapters=3):
        """爬取移动端小说（核心方法）"""
        logger.info(f"开始爬取移动端小说: ID={book_id}")
        start_time = time.time()

        try:
            # 1. 提取并保存小说基础信息
            base_info = self._extract_novel_base_info(book_id)
            self._save_novel_base_info(base_info)

            # 2. 查找章节
            logger.info("查找移动端章节...")
            chapters = self.find_chapters_from_mobile(book_id)
            if not chapters:
                logger.error("未找到任何章节")
                return False

            # 3. 爬取章节内容
            chapters_to_crawl = chapters[:max_chapters]
            logger.info(f"开始爬取章节，共 {len(chapters_to_crawl)} 章")

            success_count = 0
            results = []

            for idx, chapter in enumerate(chapters_to_crawl, 1):
                try:
                    clean_chapter_title = self._clean_chapter_title(chapter['text'])
                    real_chapter_num = self._extract_chapter_number(clean_chapter_title)
                    logger.info(f"[{idx}/{len(chapters_to_crawl)}] 爬取: {clean_chapter_title}")

                    # 获取章节内容
                    chapter_data = self.get_mobile_chapter_content(chapter['href'], chapter.get('tag'))

                    if chapter_data and chapter_data['content'] and len(chapter_data['content']) > 200:
                        success_count += 1
                        result = {
                            'chapter_num': real_chapter_num,
                            'title': clean_chapter_title,
                            'data': chapter_data
                        }
                        results.append(result)

                        # 写入数据库
                        self._save_chapter_to_db(book_id, result)

                        status = "VIP" if chapter_data.get('is_vip') else "免费"
                        logger.info(f"✓ {real_chapter_num}提取成功 ({status}, {len(chapter_data['content'])} 字符)")

                        # 预览
                        preview = chapter_data['content'][:200]
                        logger.info(f"  预览: {preview}...")
                    else:
                        content_len = len(chapter_data['content']) if chapter_data and chapter_data['content'] else 0
                        logger.warning(f"{real_chapter_num}内容无效或太短 ({content_len} 字符)")

                    # 反爬等待
                    if idx < len(chapters_to_crawl):
                        time.sleep(random.uniform(2, 4))

                except Exception as e:
                    logger.error(f"爬取{real_chapter_num}失败: {e}")
                    continue

            # 4. 保存到本地文件
            if results:
                self._save_crawl_results(book_id, results)

                # 统计信息
                elapsed_time = time.time() - start_time
                success_rate = (success_count / len(chapters_to_crawl) * 100) if chapters_to_crawl else 0

                logger.info(f"爬取完成: 成功 {success_count}/{len(chapters_to_crawl)} 章，成功率: {success_rate:.1f}%")
                logger.info(f"总耗时: {elapsed_time:.1f} 秒")

                total_chars = sum(r['data']['content_length'] for r in results)
                avg_chars = total_chars / len(results) if results else 0
                logger.info(f"总字符数: {total_chars} | 平均每章: {avg_chars:.0f} 字符")

                return True
            else:
                logger.error("没有成功提取到任何内容")
                return False

        except Exception as e:
            logger.error(f"爬取小说失败: {e}")
            return False

    def _save_crawl_results(self, book_id, results):
        """保存爬取结果到本地文件，并同步存入数据库"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"mobile_crawl_{book_id}_{timestamp}.txt"
            file_path = os.path.abspath(filename)  # ========== 新增：获取文件绝对路径 ==========

            with open(filename, 'w', encoding='utf-8') as f:
                f.write(f"移动端爬取结果报告\n")
                f.write(f"小说ID: {book_id}\n")
                f.write(f"爬取时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"成功章节: {len(results)}\n")
                f.write(f"\n{'=' * 60}\n\n")

                for result in results:
                    f.write(f"章节: {result['title']}\n")
                    f.write(f"内容长度: {result['data']['content_length']} 字符\n")
                    f.write(f"URL: {result['data']['url']}\n")
                    f.write(f"是否VIP: {'是' if result['data'].get('is_vip') else '否'}\n")

                    f.write(f"\n内容:\n")
                    f.write(result['data']['content'])
                    f.write(f"\n{'=' * 60}\n\n")

            logger.info(f"爬取结果已保存到: {filename}")

            # ========== 新增：调用文件写入数据库方法 ==========
            self._save_crawl_file_to_db(book_id, file_path)

        except Exception as e:
            logger.error(f"保存结果失败: {e}")

    def test_mobile_extraction(self, url):
        """测试移动端内容提取"""
        print(f"\n测试移动端内容提取")
        print(f"原始URL: {url}")

        mobile_url = self.fix_url_to_mobile(url)
        print(f"移动端URL: {mobile_url}")

        chapter_data = self.get_mobile_chapter_content(url)

        if chapter_data:
            print(f"\n提取结果:")
            print(f"标题: {chapter_data['title']}")
            print(f"内容长度: {chapter_data['content_length']} 字符")
            print(f"是否为VIP: {chapter_data.get('is_vip', False)}")
            print(f"当前URL: {chapter_data['url']}")

            if chapter_data['content']:
                print(f"\n内容预览 (前300字符):")
                print("-" * 50)
                print(chapter_data['content'][:300])
                print("-" * 50)

                with open('mobile_test.txt', 'w', encoding='utf-8') as f:
                    f.write(chapter_data['content'])
                print(f"\n完整内容已保存到: mobile_test.txt")
        else:
            print("\n⚠ 未能提取到内容")

    def close(self):
        """关闭浏览器"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("浏览器已关闭")
            except:
                pass


def main():
    """主函数"""
    print("起点中文网移动端小说爬虫（适配数据库版）")
    print("=" * 60)

    test_novels = [
        {"id": 1036526469, "name": "长生从炼丹宗师开始"},
        {"id": 1044750637, "name": "北望江山"},
        {"id": 1035805445, "name": "宿命之环"},
    ]

    print("可用小说列表:")
    for i, novel in enumerate(test_novels, 1):
        print(f"{i}. {novel['name']} (ID: {novel['id']})")

    print("\n请选择模式:")
    print("1. 爬取移动端小说（自动写入数据库）")
    print("2. 测试移动端内容提取")
    print("3. 对比PC和移动端")

    choice = input("\n请选择 (1-3): ").strip()

    crawler = None
    try:
        crawler = MobileQidianCrawler(headless=False)

        if choice == '1':
            print("\n选择要爬取的小说:")
            for i, novel in enumerate(test_novels, 1):
                print(f"{i}. {novel['name']}")

            novel_choice = input(f"请选择 (1-{len(test_novels)}): ").strip()
            try:
                idx = int(novel_choice) - 1
                if 0 <= idx < len(test_novels):
                    book_id = test_novels[idx]['id']
                    book_name = test_novels[idx]['name']

                    max_chapters = input("请输入要爬取的章节数 (默认3): ").strip()
                    max_chapters = int(max_chapters) if max_chapters.isdigit() else 3

                    print(f"\n开始爬取移动端: {book_name} (ID: {book_id})")
                    print(f"爬取章节数: {max_chapters} | 数据会自动写入数据库（含爬取文件）")
                    print("=" * 60)

                    success = crawler.crawl_mobile_novel(book_id, max_chapters)

                    if success:
                        print(f"\n✓ {book_name} 移动端爬取成功，数据已写入数据库")
                    else:
                        print(f"\n✗ {book_name} 移动端爬取失败")
                else:
                    print("无效选择")
            except:
                print("输入错误")

        elif choice == '2':
            test_url = "https://www.qidian.com/chapter/1044750637/877453469/"
            user_url = input(f"请输入测试URL (默认: {test_url}): ").strip()
            if not user_url:
                user_url = test_url
            crawler.test_mobile_extraction(user_url)

        elif choice == '3':
            print("\n对比PC和移动端")
            test_url = "https://www.qidian.com/chapter/1044750637/877453469/"
            print(f"测试URL: {test_url}")
            print(f"移动端URL: {crawler.fix_url_to_mobile(test_url)}")

            print("\n1. 测试移动端:")
            crawler.test_mobile_extraction(test_url)

            print("\n2. PC端测试:")
            print("请手动在浏览器中访问以上URL，比较内容差异")
            print("移动端通常更容易获取完整内容")

        else:
            print("无效选择")

    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        print(f"程序错误: {e}")
        import traceback
        traceback.print_exc()

    finally:
        if crawler:
            crawler.close()
            print("\n程序结束")


if __name__ == "__main__":
    main()