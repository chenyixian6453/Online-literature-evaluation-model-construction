# qidian_crawler_content_fixed.py
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


class ContentFixedCrawler:
    def __init__(self, headless=False):
        """初始化爬虫"""
        self.headless = headless
        self.driver = None
        self.init_driver()
        self.setup_database()

    def setup_database(self):
        """设置数据库表结构"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS novel_works (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    work_id BIGINT NOT NULL UNIQUE,
                    title VARCHAR(255),
                    author VARCHAR(100),
                    description TEXT,
                    tags VARCHAR(255),
                    url VARCHAR(500),
                    crawl_time DATETIME,
                    status VARCHAR(20) DEFAULT 'active'
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS novel_chapters (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    work_id BIGINT NOT NULL,
                    chapter_title VARCHAR(500),
                    chapter_content MEDIUMTEXT,
                    chapter_num INT,
                    chapter_url VARCHAR(500),
                    is_vip BOOLEAN DEFAULT FALSE,
                    needs_login BOOLEAN DEFAULT FALSE,
                    content_length INT,
                    crawl_time DATETIME,
                    UNIQUE KEY unique_chapter (work_id, chapter_num),
                    INDEX idx_work_id (work_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """)

            conn.commit()
            cursor.close()
            conn.close()

            logger.info("数据库表结构已创建")

        except Exception as e:
            logger.error(f"数据库设置失败: {e}")

    def init_driver(self):
        """初始化Chrome驱动"""
        try:
            chrome_options = Options()

            if self.headless:
                chrome_options.add_argument('--headless')

            # 基础配置
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')

            # 设置User-Agent
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            chrome_options.add_argument(f'user-agent={user_agent}')

            # 隐藏自动化特征
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)

            # 启用JavaScript但禁用图片
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.javascript": 1
            }
            chrome_options.add_experimental_option("prefs", prefs)

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

            # 设置窗口大小和超时
            self.driver.set_window_size(1400, 900)
            self.driver.set_page_load_timeout(15)
            self.driver.implicitly_wait(5)

            logger.info("Chrome驱动初始化成功")

        except Exception as e:
            logger.error(f"初始化Chrome驱动失败: {e}")
            raise

    def fix_url(self, url):
        """修复URL"""
        if not url:
            return ""

        url = url.strip()

        if url.startswith('//'):
            return 'https:' + url
        elif url.startswith('/'):
            return 'https://www.qidian.com' + url
        elif not url.startswith('http'):
            if url.startswith('www.'):
                return 'https://' + url
            else:
                return 'https://www.qidian.com/' + url

        return url

    def handle_popups_and_scroll(self):
        """处理弹窗并滚动"""
        try:
            # 按ESC键
            try:
                body = self.driver.find_element(By.TAG_NAME, 'body')
                body.send_keys(Keys.ESCAPE)
                time.sleep(0.5)
            except:
                pass

            # 滚动页面
            for i in range(3):
                scroll_pos = (i + 1) * 0.25
                self.driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_pos});")
                time.sleep(0.5)

            # 使用JavaScript处理弹窗
            close_js = """
            // 关闭弹窗
            var closeButtons = document.querySelectorAll('.ui-dialog-close, .close, [aria-label="关闭"]');
            closeButtons.forEach(function(btn) {
                try { btn.click(); } catch(e) {}
            });

            // 隐藏弹窗
            var dialogs = document.querySelectorAll('.ui-dialog, .modal, .popup');
            dialogs.forEach(function(dialog) {
                dialog.style.display = 'none';
            });

            // 返回页面是否还有弹窗
            return document.querySelectorAll('.ui-dialog[style*="display: block"], .modal[style*="display: block"]').length === 0;
            """

            result = self.driver.execute_script(close_js)
            if not result:
                logger.warning("可能还有弹窗未关闭")

            time.sleep(1)
            return True
        except Exception as e:
            logger.error(f"处理弹窗失败: {e}")
            return False

    def get_novel_info(self, book_id):
        """获取小说基本信息"""
        try:
            url = f"https://www.qidian.com/book/{book_id}/"
            logger.info(f"访问小说主页: {url}")

            self.driver.get(url)
            time.sleep(2)

            self.handle_popups_and_scroll()

            page_title = self.driver.title
            novel_title = page_title
            match = re.search(r'^(.*?)[\(\-【_]', page_title)
            if match:
                novel_title = match.group(1).strip()

            author = "未知作者"
            try:
                js_script = """
                var author = "未知作者";
                var selectors = [
                    'a[href*="author"]',
                    '.writer',
                    '.author-name',
                    '.book-info .author'
                ];

                for (var i = 0; i < selectors.length; i++) {
                    var elements = document.querySelectorAll(selectors[i]);
                    for (var j = 0; j < elements.length; j++) {
                        var text = elements[j].textContent.trim();
                        if (text && text.length > 0 && text.length < 20) {
                            return text;
                        }
                    }
                }
                return author;
                """
                author = self.driver.execute_script(js_script)
            except:
                pass

            logger.info(f"小说: {novel_title} - 作者: {author}")

            return {
                'book_id': book_id,
                'title': novel_title,
                'author': author,
                'description': "",
                'tags': "",
                'url': url
            }

        except Exception as e:
            logger.error(f"获取小说信息失败: {e}")
            return None

    def find_chapters(self, book_id):
        """查找章节"""
        try:
            catalog_url = f"https://www.qidian.com/book/{book_id}/catalog/"
            logger.info(f"访问目录页: {catalog_url}")

            self.driver.get(catalog_url)
            time.sleep(3)

            self.handle_popups_and_scroll()

            # 使用可靠的JavaScript查找章节
            js_script = """
            function findChapters() {
                var chapters = [];
                var allLinks = document.getElementsByTagName('a');

                for (var i = 0; i < allLinks.length; i++) {
                    var link = allLinks[i];
                    var href = link.getAttribute('href') || '';
                    var text = link.textContent.trim();

                    if (!href || !text || text.length < 2) continue;

                    // 检查是否是章节链接
                    var isChapter = href.includes('/chapter/') || href.includes('read.qidian.com');
                    var isNotNav = !text.includes('上一章') && 
                                  !text.includes('下一章') && 
                                  !text.includes('目录') &&
                                  !text.includes('开始阅读');

                    if (isChapter && isNotNav) {
                        // 修复URL
                        if (href.startsWith('//')) {
                            href = 'https:' + href;
                        } else if (href.startsWith('/')) {
                            href = 'https://www.qidian.com' + href;
                        }

                        chapters.push({
                            href: href,
                            text: text,
                            index: i
                        });
                    }
                }

                // 去重并限制数量
                var uniqueChapters = [];
                var seenHrefs = {};

                for (var j = 0; j < chapters.length; j++) {
                    var chap = chapters[j];
                    if (!seenHrefs[chap.href]) {
                        seenHrefs[chap.href] = true;
                        uniqueChapters.push(chap);
                    }
                }

                return uniqueChapters.slice(0, 30);
            }

            return findChapters();
            """

            chapters = self.driver.execute_script(js_script)

            if not chapters or len(chapters) == 0:
                chapters = self._find_chapters_backup()

            logger.info(f"找到 {len(chapters)} 个章节")

            for i, chap in enumerate(chapters[:3]):
                logger.info(f"  第{i + 1}章: {chap['text']}")

            return chapters

        except Exception as e:
            logger.error(f"查找章节失败: {e}")
            return []

    def _find_chapters_backup(self):
        """备用方法查找章节"""
        try:
            chapters = []
            all_links = self.driver.find_elements(By.TAG_NAME, 'a')

            for link in all_links:
                try:
                    href = link.get_attribute('href') or ''
                    text = link.text.strip()

                    if not href or not text:
                        continue

                    href = self.fix_url(href)

                    if ('/chapter/' in href or 'read.qidian.com' in href) and text:
                        if text not in ['上一章', '下一章', '目录', '开始阅读']:
                            chapters.append({
                                'href': href,
                                'text': text
                            })
                except:
                    continue

            return chapters[:30]
        except:
            return []

    def extract_chapter_content_debug(self, url):
        """调试版章节内容提取"""
        try:
            fixed_url = self.fix_url(url)
            logger.info(f"访问章节: {fixed_url}")

            self.driver.get(fixed_url)
            time.sleep(3)

            # 处理弹窗并滚动
            self.handle_popups_and_scroll()

            # 获取页面标题
            page_title = self.driver.title
            logger.info(f"页面标题: {page_title}")

            # 检查页面状态
            current_url = self.driver.current_url
            logger.info(f"当前URL: {current_url}")

            # 保存页面源码用于调试
            try:
                page_source = self.driver.page_source
                with open('debug_page.html', 'w', encoding='utf-8') as f:
                    f.write(page_source)
                logger.info("页面源码已保存: debug_page.html")
            except:
                pass

            # 尝试多种内容提取方法
            extraction_results = []

            # 方法1: 直接查找元素
            logger.info("尝试方法1: 直接查找元素")
            content1 = self._extract_direct_elements()
            if content1:
                extraction_results.append(("直接查找", len(content1), content1[:100]))

            # 方法2: JavaScript提取
            logger.info("尝试方法2: JavaScript提取")
            content2 = self._extract_with_javascript()
            if content2:
                extraction_results.append(("JavaScript", len(content2), content2[:100]))

            # 方法3: 获取body文本
            logger.info("尝试方法3: 获取body文本")
            content3 = self._extract_body_text()
            if content3:
                extraction_results.append(("Body文本", len(content3), content3[:100]))

            # 方法4: 查找所有文本
            logger.info("尝试方法4: 查找所有文本")
            content4 = self._extract_all_text()
            if content4:
                extraction_results.append(("所有文本", len(content4), content4[:100]))

            # 打印提取结果
            logger.info("提取结果:")
            for method, length, preview in extraction_results:
                logger.info(f"  {method}: {length} 字符, 预览: {preview}...")

            # 选择最长的内容
            all_contents = [c for m, l, c in extraction_results]
            best_content = max(all_contents, key=len) if all_contents else ""

            if best_content:
                logger.info(f"选择最佳内容: {len(best_content)} 字符")
                best_content = self._clean_content(best_content)

            # 检查页面状态
            page_source = self.driver.page_source[:2000]
            needs_login = any(keyword in page_source for keyword in ['登录', 'login'])
            is_vip = any(keyword in page_source.lower() for keyword in ['vip', '订阅'])

            if needs_login:
                logger.warning("页面需要登录")
            if is_vip:
                logger.warning("VIP章节")

            return {
                'title': page_title,
                'content': best_content,
                'url': current_url,
                'is_vip': is_vip,
                'needs_login': needs_login,
                'content_length': len(best_content) if best_content else 0
            }

        except Exception as e:
            logger.error(f"提取章节失败: {e}")
            return None

    def _extract_direct_elements(self):
        """直接查找元素"""
        selectors = [
            'div.read-content',
            'div.chapter-content',
            'div.j_readContent',
            '.read-content',
            '.chapter-content',
            'div.content',
            'article',
            '.text-wrap',
            '.main-text-wrap'
        ]

        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    try:
                        if element.is_displayed():
                            text = element.text
                            if text and len(text.strip()) > 300:
                                logger.info(f"直接找到元素: {selector}")
                                return text.strip()
                    except:
                        continue
            except:
                continue

        return ""

    def _extract_with_javascript(self):
        """使用JavaScript提取"""
        js_scripts = [
            # 脚本1: 查找常见内容容器
            """
            function extract1() {
                var selectors = [
                    'div.read-content',
                    'div.chapter-content',
                    'div.j_readContent',
                    '.read-content',
                    '.chapter-content',
                    'div.content'
                ];

                for (var i = 0; i < selectors.length; i++) {
                    var elements = document.querySelectorAll(selectors[i]);
                    for (var j = 0; j < elements.length; j++) {
                        var text = elements[j].textContent || elements[j].innerText;
                        if (text && text.trim().length > 500) {
                            return text.trim();
                        }
                    }
                }
                return '';
            }
            return extract1();
            """,

            # 脚本2: 查找包含最多文本的元素
            """
            function extract2() {
                var allElements = document.querySelectorAll('div, article, section');
                var bestElement = null;
                var bestLength = 0;

                for (var i = 0; i < allElements.length; i++) {
                    var element = allElements[i];
                    var text = element.textContent || element.innerText;
                    var trimmed = text.trim();

                    if (trimmed.length > bestLength && trimmed.length < 30000) {
                        // 检查是否包含中文
                        var chineseChars = trimmed.match(/[\\u4e00-\\u9fff]/g);
                        if (chineseChars && chineseChars.length > trimmed.length * 0.3) {
                            bestLength = trimmed.length;
                            bestElement = element;
                        }
                    }
                }

                return bestElement ? (bestElement.textContent || bestElement.innerText).trim() : '';
            }
            return extract2();
            """,

            # 脚本3: 获取整个文档的文本
            """
            function extract3() {
                return document.body.textContent || document.body.innerText || '';
            }
            return extract3();
            """
        ]

        for script in js_scripts:
            try:
                content = self.driver.execute_script(script)
                if content and len(content.strip()) > 300:
                    logger.info(f"JavaScript脚本提取到内容: {len(content)} 字符")
                    return content.strip()
            except Exception as e:
                logger.debug(f"JavaScript脚本执行失败: {e}")
                continue

        return ""

    def _extract_body_text(self):
        """获取body文本"""
        try:
            body = self.driver.find_element(By.TAG_NAME, 'body')
            text = body.text
            if text and len(text.strip()) > 300:
                logger.info(f"获取到body文本: {len(text)} 字符")
                return text.strip()
        except:
            pass
        return ""

    def _extract_all_text(self):
        """获取所有文本"""
        try:
            js_script = """
            function getAllText() {
                var allText = '';
                var allElements = document.querySelectorAll('*');

                for (var i = 0; i < allElements.length; i++) {
                    var element = allElements[i];
                    var text = element.textContent || element.innerText;
                    if (text && text.trim()) {
                        allText += text.trim() + '\\n';
                    }
                }

                return allText;
            }
            return getAllText();
            """
            content = self.driver.execute_script(js_script)
            if content and len(content.strip()) > 300:
                logger.info(f"获取到所有文本: {len(content)} 字符")
                return content.strip()
        except:
            pass
        return ""

    def _clean_content(self, content):
        """清理内容"""
        if not content:
            return ""

        # 分割行并清理
        lines = content.split('\n')
        cleaned_lines = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # 过滤广告和非正文内容
            skip_patterns = [
                '广告', 'ADVERTISEMENT', '登录', 'login', '立即登录',
                'VIP', 'vip', '订阅', '会员', '上一章', '下一章',
                '目录', '返回', '本章完', '未完待续', '起点中文网',
                '创世中文网', 'QQ阅读', '加入书架', '推荐票', '月票'
            ]

            should_skip = False
            for pattern in skip_patterns:
                if pattern.lower() in line.lower():
                    should_skip = True
                    break

            # 跳过太短的行
            if len(line) < 10:
                should_skip = True

            if not should_skip:
                cleaned_lines.append(line)

        result = '\n'.join(cleaned_lines)
        logger.info(f"清理后内容长度: {len(result)} 字符")
        return result

    def save_novel_info(self, novel_info):
        """保存小说信息"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()

            sql = """
            INSERT INTO novel_works 
            (work_id, title, author, description, tags, url, crawl_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                title = VALUES(title),
                author = VALUES(author),
                crawl_time = VALUES(crawl_time)
            """

            cursor.execute(sql, (
                novel_info['book_id'],
                novel_info['title'],
                novel_info['author'],
                novel_info['description'],
                novel_info['tags'],
                novel_info['url'],
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info("保存小说信息成功")
            return True

        except Exception as e:
            logger.error(f"保存小说信息失败: {e}")
            return False

    def save_chapter(self, work_id, chapter_info, chapter_num):
        """保存章节"""
        try:
            conn = pymysql.connect(**DB_CONFIG)
            cursor = conn.cursor()

            sql = """
            INSERT INTO novel_chapters 
            (work_id, chapter_title, chapter_content, chapter_num, chapter_url, 
             is_vip, needs_login, content_length, crawl_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                chapter_content = VALUES(chapter_content),
                chapter_url = VALUES(chapter_url),
                crawl_time = VALUES(crawl_time)
            """

            cursor.execute(sql, (
                work_id,
                chapter_info['title'][:200],
                chapter_info['content'],
                chapter_num,
                chapter_info['url'][:400],
                chapter_info.get('is_vip', False),
                chapter_info.get('needs_login', False),
                chapter_info.get('content_length', 0),
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))

            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"保存章节成功: 第{chapter_num}章")
            return True

        except Exception as e:
            logger.error(f"保存章节失败: {e}")
            return False

    def crawl_novel_debug(self, book_id, max_chapters=2):
        """调试版爬取小说"""
        logger.info(f"开始调试爬取小说: ID={book_id}")
        start_time = time.time()

        try:
            # 1. 获取小说信息
            logger.info("获取小说基本信息...")
            novel_info = self.get_novel_info(book_id)

            if not novel_info:
                logger.error("无法获取小说信息")
                return False

            self.save_novel_info(novel_info)

            # 2. 查找章节
            logger.info("查找章节...")
            chapters = self.find_chapters(book_id)

            if not chapters:
                logger.error("未找到任何章节")
                return False

            # 3. 爬取章节（详细调试）
            chapters_to_crawl = chapters[:max_chapters]
            logger.info(f"开始爬取章节，共 {len(chapters_to_crawl)} 章")

            success_count = 0

            for idx, chapter in enumerate(chapters_to_crawl, 1):
                try:
                    logger.info(f"[{idx}/{len(chapters_to_crawl)}] 爬取: {chapter['text']}")

                    # 使用调试版提取
                    chapter_data = self.extract_chapter_content_debug(chapter['href'])

                    if chapter_data and chapter_data['content'] and len(chapter_data['content']) > 100:
                        # 保存章节
                        if self.save_chapter(book_id, chapter_data, idx):
                            success_count += 1
                            logger.info(f"✓ 第{idx}章保存成功 ({len(chapter_data['content'])} 字符)")

                            # 显示内容预览
                            preview = chapter_data['content'][:200]
                            logger.info(f"内容预览: {preview}...")
                        else:
                            logger.warning(f"✗ 第{idx}章保存失败")
                    else:
                        content_len = len(chapter_data['content']) if chapter_data and chapter_data['content'] else 0
                        logger.warning(f"第{idx}章内容无效或太短 ({content_len} 字符)")

                    # 等待
                    if idx < len(chapters_to_crawl):
                        time.sleep(2)

                except Exception as e:
                    logger.error(f"爬取第{idx}章失败: {e}")
                    continue

            # 4. 结果
            elapsed_time = time.time() - start_time
            success_rate = (success_count / len(chapters_to_crawl) * 100) if chapters_to_crawl else 0

            logger.info(f"爬取完成: 成功 {success_count}/{len(chapters_to_crawl)} 章，成功率: {success_rate:.1f}%")
            logger.info(f"总耗时: {elapsed_time:.1f} 秒")

            return success_count > 0

        except Exception as e:
            logger.error(f"爬取小说失败: {e}")
            return False

    def test_extraction(self, url):
        """测试内容提取"""
        print(f"\n测试内容提取: {url}")
        print("=" * 50)

        chapter_data = self.extract_chapter_content_debug(url)

        if chapter_data:
            print(f"标题: {chapter_data['title']}")
            print(f"URL: {chapter_data['url']}")
            print(f"内容长度: {chapter_data['content_length']} 字符")
            print(f"是否需要登录: {chapter_data.get('needs_login', False)}")
            print(f"是否为VIP: {chapter_data.get('is_vip', False)}")

            if chapter_data['content']:
                print(f"\n内容预览:")
                print("-" * 50)
                print(chapter_data['content'][:500])
                print("-" * 50)

                # 保存到文件
                with open('extraction_test.txt', 'w', encoding='utf-8') as f:
                    f.write(chapter_data['content'])
                print(f"\n完整内容已保存到: extraction_test.txt")
            else:
                print("\n⚠ 未能提取到内容")
        else:
            print("提取失败")

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
    print("起点中文网小说爬虫 - 内容提取修复版")
    print("=" * 50)

    test_novels = [
        {"id": 1036526469, "name": "长生从炼丹宗师开始"},
        {"id": 1044750637, "name": "北望江山"},
        {"id": 1035805445, "name": "宿命之环"},
    ]

    print("可用小说列表:")
    for i, novel in enumerate(test_novels, 1):
        print(f"{i}. {novel['name']} (ID: {novel['id']})")

    print("\n请选择模式:")
    print("1. 调试爬取小说")
    print("2. 测试内容提取")

    choice = input("\n请选择 (1-2): ").strip()

    crawler = None
    try:
        crawler = ContentFixedCrawler(headless=False)

        if choice == '1':
            # 调试爬取
            print("\n选择要爬取的小说:")
            for i, novel in enumerate(test_novels, 1):
                print(f"{i}. {novel['name']}")

            novel_choice = input(f"请选择 (1-{len(test_novels)}): ").strip()
            try:
                idx = int(novel_choice) - 1
                if 0 <= idx < len(test_novels):
                    book_id = test_novels[idx]['id']
                    book_name = test_novels[idx]['name']

                    max_chapters = input("请输入要爬取的章节数 (默认2): ").strip()
                    max_chapters = int(max_chapters) if max_chapters.isdigit() else 2

                    print(f"\n开始调试爬取: {book_name} (ID: {book_id})")
                    print(f"爬取章节数: {max_chapters}")
                    print("=" * 50)

                    success = crawler.crawl_novel_debug(book_id, max_chapters)

                    if success:
                        print(f"\n✓ {book_name} 爬取成功")
                    else:
                        print(f"\n✗ {book_name} 爬取失败")
                else:
                    print("无效选择")
            except:
                print("输入错误")

        elif choice == '2':
            # 测试内容提取
            test_url = "https://www.qidian.com/chapter/1044750637/877453469/"
            user_url = input(f"请输入测试URL (默认: {test_url}): ").strip()
            if not user_url:
                user_url = test_url

            crawler.test_extraction(user_url)

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