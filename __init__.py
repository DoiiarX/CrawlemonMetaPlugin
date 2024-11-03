import json
from urllib.request import Request, urlopen
from urllib.error import URLError
from urllib.parse import urlencode, quote
from calibre.ebooks.metadata.sources.base import Source, Option
from calibre.ebooks.metadata import MetaInformation
from datetime import datetime


class CrawlemonMetaPlugin(Source):
    name = 'Crawlemon 元数据插件'
    description = '使用 Crawlemon API 获取图书元数据的 Calibre 插件。'
    supported_platforms = ['windows', 'osx', 'linux']
    version = (3, 1, 2)
    author = 'Doiiars (Modified)'

    capabilities = frozenset(['identify'])
    touched_fields = frozenset([
        'title', 'authors', 'identifier:isbn', 'publisher', 'pubdate', 'comments', 'tags'
    ])

    options = (
        Option('api_key', 'string', '', 'API Key',
               'Crawlemon API 密钥，用于访问服务'),
        Option('api_base_url', 'string', 'https://crawlemon.fsotool.com/api/v1', 'API Base URL',
               'Crawlemon API 的基础 URL'),
        Option('request_url', 'string',
               'https://pdc.capub.cn/search.html#/quick?type=%E5%9B%BE%E4%B9%A6&search={isbn} {title} {author}',
               'Request URL',
               '用于请求的特定 URL，可以包含 {isbn}、{title} 和 {author} 占位符'),
        Option('max_items', 'number', -1, 'Max Items',
               '最大返回项目数。-1 表示无限制。'),
        Option('scroll_to_bottom', 'bool', False, 'Scroll to Bottom',
               '是否滚动到页面底部'),
        Option('actions', 'string', '[]', 'Custom Actions',
               '自定义动作配置，使用 JSON 数组格式。例如：[{"type": "click", "textMatchType": "contains", "textMatchValue": ""}]'),
        Option('cmd', 'string', '获取书籍元数据，作者名字中不应该带有"著"字样，pubdate以YYYY-MM-DD格式为准', 'Command',
               '可选的命令字符串，用于 API 请求')
    )

    def __init__(self, *args, **kwargs):
        Source.__init__(self, *args, **kwargs)

    def create_session(self, log):
        log.info("开始创建会话")
        api_url = f"{self.prefs['api_base_url']}/create_session"
        data = json.dumps({"api_key": self.prefs['api_key']}).encode('utf-8')
        headers = {"Content-Type": "application/json"}
        req = Request(api_url, data=data, headers=headers, method='POST')
        try:
            with urlopen(req) as response:
                session_data = json.loads(response.read().decode('utf-8'))
                log.info(f"会话创建成功，session_id: {session_data['session_id']}")
                return session_data['session_id']
        except URLError as e:
            log.error(f"创建会话时发生错误: {str(e)}")
            return None

    def retrieve_data(self, session_id, query, fields, url, log):
        log.info(f"开始检索数据，URL: {url}")
        api_url = f"{self.prefs['api_base_url']}/retrieve"

        # 解析 actions 配置
        try:
            actions = json.loads(self.prefs['actions'])
        except json.JSONDecodeError:
            log.warn("Actions 配置解析失败，将使用空列表")
            actions = []

        data = json.dumps({
            "cmd": self.prefs['cmd'] or query,
            "url": url,
            "fields": fields,
            "session_id": session_id,
            "scroll_to_bottom": self.prefs['scroll_to_bottom'],
            "max_items": self.prefs['max_items'],
            "local": True,
            "actions": actions
        }).encode('utf-8')

        headers = {"Content-Type": "application/json"}
        req = Request(api_url, data=data, headers=headers, method='POST')
        try:
            with urlopen(req) as response:
                result = json.loads(response.read().decode('utf-8'))
                log.info(f"API 返回的原始数据: {json.dumps(result, ensure_ascii=False, indent=2)}")
                result = result.get('result', [])
                log.info(f"数据检索成功，获取到 {len(result)} 条记录")
                return result
        except URLError as e:
            log.error(f"HTTP 检索请求失败: {str(e)}")
            return None

    def identify(self, log, result_queue, abort, title=None, authors=None, identifiers={}, timeout=30):
        log.info("开始识别过程")
        session_id = self.create_session(log)
        if not session_id:
            log.error("创建会话失败，终止识别过程")
            return

        isbn = identifiers.get('isbn', '')
        log.info(f"ISBN: {isbn}, 标题: {title}, 作者: {authors}")

        # 初始化并填充 format_params
        format_params = dict.fromkeys(('isbn', 'title', 'author'), '')
        if isbn:
            format_params['isbn'] = quote(isbn)
        if title:
            format_params['title'] = quote(title)
        if authors and authors[0]:
            format_params['author'] = quote(authors[0])

        # 格式化 URL
        try:
            formatted_url = self.prefs['request_url'].format(**format_params)
            log.info(f"格式化后的 URL: {formatted_url}")
        except KeyError as e:
            log.error(f"URL 格式化错误: 缺少键 {e}")
            return

        query = isbn or title
        if not query:
            log.error("未提供 ISBN、标题或作者")
            return

        log.info(f"使用查询参数: {query}")

        fields = ["title", "authors", "isbn", "publisher", "pubdate", "comments", "tags"]
        data = self.retrieve_data(session_id, query, fields, formatted_url, log)

        if not data:
            log.error("从 API 检索数据失败")
            return

        for item in data:
            if not isinstance(item, dict):
                log.info(f"无效的条目: {item}")
                continue
            log.info(f"处理项目: {item}")
            authors = item.get('authors', [])
            if isinstance(authors, str):
                if "," in authors or " " in authors or "，" in authors:
                    authors = authors.split(",")
                else:
                    authors = [authors]
            mi = MetaInformation(item.get('title', ''), authors)
            mi.isbn = item.get('isbn', '')
            mi.publisher = item.get('publisher', '')

            # Handle pubdate conversion to ISO format
            pubdate_str = item.get('pubdate', '').strip()  # Remove leading/trailing whitespace
            log.info(f"原始出版日期字符串: '{pubdate_str}'")

            if pubdate_str:
                try:
                    # Try parsing with different format strings
                    for fmt in ('%Y-%m-%d', '%Y年%m月%d日', '%Y年%m月', '%Y/%m/%d', '%Y.%m.%d'):
                        try:
                            pubdate = datetime.strptime(pubdate_str, fmt)
                            mi.pubdate = pubdate
                            log.info(f"成功解析出版日期: {mi.pubdate}, 使用格式: {fmt}")
                            break
                        except ValueError:
                            continue
                    else:
                        log.warn(f"无法解析出版日期: '{pubdate_str}'，尝试了所有已知格式")
                        mi.pubdate = None
                except Exception as e:
                    log.error(f"解析出版日期时发生错误: {str(e)}")
                    mi.pubdate = None
            else:
                log.info("未提供出版日期")
                mi.pubdate = None

            mi.comments = item.get('comments', '')
            tags = item.get('tags', [])
            if isinstance(tags, str):
                if "," in tags or " " in tags or "，" in tags:
                    tags = tags.split(",")
                else:
                    tags = [tags]
            mi.tags = tags

            if mi.isbn:
                mi.set_identifier('isbn', mi.isbn)

            result_queue.put(mi)
            log.info(f"已将元数据添加到结果队列: {mi.title}")

        log.info("识别过程完成")

    def download_cover(self, log, result_queue, abort, title=None, authors=None, identifiers={}, timeout=30,
                       get_best_cover=False):
        log.info("下载封面功能尚未实现")
        pass  # 这个 API 目前不提供封面下载功能