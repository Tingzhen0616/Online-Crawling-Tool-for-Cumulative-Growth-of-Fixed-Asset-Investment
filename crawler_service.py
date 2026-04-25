import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import re
import json
from datetime import datetime
from typing import List, Dict, Tuple, Optional

BASE_URL = "https://stats.gd.gov.cn/cztzqkb/"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
REQUEST_DELAY_MIN = 3
REQUEST_DELAY_MAX = 5

DEFAULT_INDUSTRIES = [
    "电力、热力、燃气及水生产和供应业",
    "水利、环境和公共设施管理",
    "石油及化学",
    "钢铁冶炼及加工",
    "建筑材料",
    "纺织业",
    "食品制造业",
    "采矿业",
    "医药制造业"
]

DEFAULT_KEYWORDS = {
    "电力、热力、燃气及水生产和供应业": ["电力、燃气及水的生产和供应业"],
    "水利、环境和公共设施管理": [""],
    "石油及化学": [""],
    "钢铁冶炼及加工": [""],
    "建筑材料": [""],
    "纺织业": ["纺织服装"],
    "食品制造业": ["食品饮料"],
    "采矿业": [""],
    "医药制造业": [""]
}

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

CONFIG_FILE = os.path.join(os.path.dirname(__file__), "web_config.json")


class CrawlerService:
    def __init__(self):
        self.logs = []
        self.matched_records = []
        self.industries = DEFAULT_INDUSTRIES.copy()
        self.keywords = DEFAULT_KEYWORDS.copy()
        self.crawl_lock = False
        self.base_url = BASE_URL
        self.start_year = 2024
        self.start_month = 1
        self.end_year = datetime.now().year
        self.end_month = datetime.now().month
        self.load_user_config()

    def log(self, message: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.logs.append(log_entry)

    def clear_logs(self):
        self.logs = []

    def get_logs(self) -> List[str]:
        return self.logs

    def set_industries(self, industries: List[str]):
        self.industries = industries

    def get_industries(self) -> List[str]:
        return self.industries

    def set_keywords(self, keywords: Dict[str, List[str]]):
        self.keywords = keywords

    def get_keywords(self) -> Dict[str, List[str]]:
        return self.keywords

    def add_industry(self, industry: str) -> bool:
        if industry and industry not in self.industries:
            self.industries.append(industry)
            if industry not in self.keywords:
                self.keywords[industry] = []
            return True
        return False

    def delete_industry(self, industry: str) -> bool:
        if industry in DEFAULT_INDUSTRIES:
            return False
        if industry in self.industries:
            self.industries.remove(industry)
            if industry in self.keywords:
                del self.keywords[industry]
            return True
        return False

    def add_keyword(self, industry: str, keyword: str) -> bool:
        if industry not in self.keywords:
            self.keywords[industry] = []
        if keyword and keyword not in self.keywords[industry]:
            self.keywords[industry].append(keyword)
            return True
        return False

    def delete_keyword(self, industry: str, keyword: str) -> bool:
        if industry in self.keywords and keyword in self.keywords[industry]:
            self.keywords[industry].remove(keyword)
            return True
        return False

    def reset_to_default(self):
        self.industries = DEFAULT_INDUSTRIES.copy()
        self.keywords = DEFAULT_KEYWORDS.copy()
        self.base_url = BASE_URL
        self.start_year = 2024
        self.start_month = 1
        self.end_year = datetime.now().year
        self.end_month = datetime.now().month

    def set_time_range(self, start_year: int, start_month: int, end_year: int, end_month: int):
        self.start_year = start_year
        self.start_month = start_month
        self.end_year = end_year
        self.end_month = end_month

    def get_time_range(self) -> Dict:
        return {
            'start_year': self.start_year,
            'start_month': self.start_month,
            'end_year': self.end_year,
            'end_month': self.end_month
        }

    def set_base_url(self, url: str):
        self.base_url = url

    def get_base_url(self) -> str:
        return self.base_url

    def save_user_config(self) -> str:
        config = {
            'base_url': self.base_url,
            'start_year': self.start_year,
            'start_month': self.start_month,
            'end_year': self.end_year,
            'end_month': self.end_month,
            'industries': self.industries,
            'keywords': self.keywords,
            'logs': self.logs
        }
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        return CONFIG_FILE

    def load_user_config(self):
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                self.base_url = config.get('base_url', BASE_URL)
                self.start_year = config.get('start_year', 2024)
                self.start_month = config.get('start_month', 1)
                self.end_year = config.get('end_year', datetime.now().year)
                self.end_month = config.get('end_month', datetime.now().month)
                self.industries = config.get('industries', DEFAULT_INDUSTRIES.copy())
                self.keywords = config.get('keywords', DEFAULT_KEYWORDS.copy())
                self.logs = config.get('logs', [])
            except:
                pass

    def export_config(self) -> Dict:
        return {
            'base_url': self.base_url,
            'start_year': self.start_year,
            'start_month': self.start_month,
            'end_year': self.end_year,
            'end_month': self.end_month,
            'industries': self.industries,
            'keywords': self.keywords,
            'logs': self.logs
        }

    def import_config(self, config: Dict):
        if 'base_url' in config:
            self.base_url = config['base_url']
        if 'start_year' in config:
            self.start_year = config['start_year']
        if 'start_month' in config:
            self.start_month = config['start_month']
        if 'end_year' in config:
            self.end_year = config['end_year']
        if 'end_month' in config:
            self.end_month = config['end_month']
        if 'industries' in config:
            self.industries = config['industries']
        if 'keywords' in config:
            self.keywords = config['keywords']
        if 'logs' in config:
            self.logs = config['logs']

    def get_session(self):
        session = requests.Session()
        session.headers.update({'User-Agent': USER_AGENT})
        return session

    def sleep(self):
        delay = REQUEST_DELAY_MIN + (REQUEST_DELAY_MAX - REQUEST_DELAY_MIN) * (time.time() % 1)
        time.sleep(delay)

    def fetch_page_list(self, base_url: str) -> List[Dict]:
        self.log("开始获取分页列表...")
        session = self.get_session()
        page_num = 1
        all_links = []
        base_url = base_url.rstrip('/') + '/'

        while True:
            if page_num == 1:
                url = f"{base_url}index.html"
            else:
                url = f"{base_url}index_{page_num}.html"

            self.log(f"正在请求第 {page_num} 页: {url}")

            try:
                response = session.get(url, timeout=30)
                response.encoding = 'utf-8'

                if response.status_code != 200:
                    self.log(f"页面不存在或无法访问，停止翻页")
                    break

                soup = BeautifulSoup(response.text, 'lxml')
                links = self.extract_detail_links(soup, base_url)

                if not links:
                    self.log(f"第 {page_num} 页未找到相关链接，停止翻页")
                    break

                all_links.extend(links)
                self.log(f"第 {page_num} 页找到 {len(links)} 个链接")

                page_num += 1
                self.sleep()

            except Exception as e:
                self.log(f"请求出错: {str(e)}")
                break

        self.log(f"共找到 {len(all_links)} 个详情链接")
        return all_links

    def extract_detail_links(self, soup: BeautifulSoup, base_url: str) -> List[Dict]:
        links = []
        pattern = re.compile(r'(\d{4})\s*年\s*(?:(\d{1,2})-(\d{1,2})月|(\d{1,2})月).*固定资产投资完成情况')

        for a_tag in soup.find_all('a', href=True):
            text = a_tag.get_text(strip=True)
            match = pattern.search(text)

            if match:
                year = int(match.group(1))
                if match.group(2) and match.group(3):
                    month = int(match.group(3))
                else:
                    month = int(match.group(4))

                href = a_tag['href']
                if not href.startswith('http'):
                    if href.startswith('/'):
                        href = f"https://stats.gd.gov.cn{href}"
                    else:
                        href = f"{base_url}{href}"

                links.append({
                    'title': text,
                    'url': href,
                    'year': year,
                    'month': month
                })

        return links

    def filter_by_time_range(self, links: List[Dict], start_year: int, start_month: int, end_year: int, end_month: int) -> List[Dict]:
        self.log(f"筛选时间范围: {start_year}年{start_month}月 至 {end_year}年{end_month}月")
        filtered = []

        for link in links:
            link_date = (link['year'], link['month'])
            start_date = (start_year, start_month)
            end_date = (end_year, end_month)

            if start_date <= link_date <= end_date:
                filtered.append(link)

        self.log(f"筛选后剩余 {len(filtered)} 个链接")
        return filtered

    def parse_detail_page(self, url: str, year: int, month: int) -> Dict:
        self.log(f"正在解析 {year}年{month}月 数据: {url}")
        session = self.get_session()
        result = {'year': year, 'month': month, 'data': {}}

        try:
            response = session.get(url, timeout=30)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'lxml')

            tables = soup.find_all('table')
            self.log(f"找到 {len(tables)} 个表格")

            for table in tables:
                table_data = self.parse_table(table)
                if table_data:
                    result['data'].update(table_data)

            self.log(f"{year}年{month}月 提取到 {len(result['data'])} 条行业数据")

        except Exception as e:
            self.log(f"解析出错: {str(e)}")

        return result

    def parse_table(self, table) -> Dict:
        result = {}
        rows = table.find_all('tr')

        if len(rows) < 2:
            return result

        start_row = 0
        headers = []

        for i, row in enumerate(rows):
            cells = row.find_all(['th', 'td'])
            cell_texts = [cell.get_text(strip=True) for cell in cells]

            if '单位' in cell_texts[0] or '%' in cell_texts[0]:
                continue

            if '指标' in cell_texts[0] or len(cell_texts) >= 2:
                start_row = i
                headers = cell_texts
                break

        if not headers:
            headers = [cell.get_text(strip=True) for cell in rows[0].find_all(['th', 'td'])]
            start_row = 0

        col_count = len(headers)

        if col_count == 2:
            for row in rows[start_row + 1:]:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    name = cells[0].get_text(strip=True)
                    value = cells[1].get_text(strip=True)
                    if name and value and '指标' not in name:
                        result[name] = value

        elif col_count >= 3:
            target_col_idx = -1
            for idx, header in enumerate(headers):
                if '固定资产' in header and ('同比' in header or '增长' in header or '%' in header):
                    target_col_idx = idx
                    break

            if target_col_idx == -1:
                for idx, header in enumerate(headers):
                    if '%' in header or '同比' in header or '增速' in header:
                        target_col_idx = idx
                        break

            if target_col_idx == -1 and col_count >= 2:
                target_col_idx = 1

            if target_col_idx != -1:
                for row in rows[start_row + 1:]:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) > target_col_idx:
                        name = cells[0].get_text(strip=True)
                        value = cells[target_col_idx].get_text(strip=True)
                        if name and value and '指标' not in name:
                            result[name] = value

        return result

    def match_industry(self, raw_name: str) -> Tuple[Optional[str], str, List[str]]:
        if raw_name in self.industries:
            return raw_name, "精确匹配", []

        for industry in self.industries:
            if industry in raw_name:
                return industry, "精确匹配", []

        for industry in self.industries:
            keywords = self.keywords.get(industry, [])
            for keyword in keywords:
                if keyword == raw_name:
                    return industry, "精确匹配", [keyword]

        for industry in self.industries:
            keywords = self.keywords.get(industry, [])
            for keyword in keywords:
                if keyword in raw_name:
                    return industry, "关键词匹配", [keyword]

        return None, "未匹配", []

    def process_all_data(self, all_page_data: List[Dict]) -> Tuple[Dict, List]:
        self.log("开始匹配行业数据...")
        self.matched_records = []
        monthly_data = {}

        for page_data in all_page_data:
            key = f"{page_data['year']}-{page_data['month']:02d}"
            monthly_data[key] = {}

            for raw_name, raw_value in page_data['data'].items():
                matched_industry, match_type, matched_keywords = self.match_industry(raw_name)

                if matched_industry:
                    clean_value = self.clean_value(raw_value)
                    monthly_data[key][matched_industry] = clean_value

                    record = {
                        'month': key,
                        'raw_name': raw_name,
                        'matched_industry': matched_industry,
                        'match_type': match_type,
                        'matched_keywords': matched_keywords,
                        'raw_value': raw_value,
                        'clean_value': clean_value
                    }
                    self.matched_records.append(record)

        self.log(f"完成行业匹配，共 {len(self.matched_records)} 条匹配记录")
        return monthly_data, self.matched_records

    def clean_value(self, value: str) -> str:
        value = value.strip()
        match = re.search(r'(-?\d+\.?\d*)', value)
        if match:
            num = match.group(1)
            return num
        return value

    def generate_csv(self, monthly_data: Dict, start_year: int, start_month: int, end_year: int, end_month: int) -> str:
        self.log("正在生成CSV文件...")
        months = sorted(monthly_data.keys(), reverse=True)

        def format_month(month_str):
            year, month = month_str.split('-')
            return f"{year}年{int(month)}月"

        formatted_months = [format_month(m) for m in months]
        rows = []
        rows.append(["数据库：月度数据"])
        rows.append([f"时间：{start_year}年{start_month}月-{end_year}年{end_month}月"])

        header = ["指标"] + formatted_months
        rows.append(header)

        for industry in self.industries:
            row = [industry]
            for month in months:
                value = monthly_data.get(month, {}).get(industry, "")
                row.append(value)
            rows.append(row)

        df = pd.DataFrame(rows)
        csv_path = os.path.join(OUTPUT_DIR, "固定资产投资月度数据.csv")
        df.to_csv(csv_path, index=False, header=False, encoding='utf-8-sig')

        self.log(f"CSV文件已生成: {csv_path}")
        return csv_path

    def generate_match_report(self) -> str:
        self.log("正在生成匹配说明文件...")
        report_path = os.path.join(OUTPUT_DIR, "行业匹配说明.txt")

        keyword_records = [r for r in self.matched_records if r['match_type'] == "关键词匹配"]
        merged = {}

        for record in keyword_records:
            key = (record['raw_name'], record['matched_industry'], record['matched_keywords'][0] if record['matched_keywords'] else "")
            if key not in merged:
                merged[key] = {
                    'raw_name': record['raw_name'],
                    'matched_industry': record['matched_industry'],
                    'matched_keyword': record['matched_keywords'][0] if record['matched_keywords'] else "",
                    'months': []
                }
            merged[key]['months'].append(record['month'])

        def format_month(month_str):
            year, month = month_str.split('-')
            return f"{year}年{int(month)}月"

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("行业匹配说明\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write("-" * 80 + "\n")
            f.write("匹配记录明细 (仅关键词匹配):\n")
            f.write("-" * 80 + "\n\n")

            for item in merged.values():
                f.write(f"原文名称: {item['raw_name']}\n")
                f.write(f"匹配行业: {item['matched_industry']}\n")
                f.write(f"匹配关键词: {item['matched_keyword']}\n")
                month_strs = [format_month(m) for m in item['months']]
                f.write(f"出现月份: {', '.join(month_strs)}\n")
                f.write("-" * 80 + "\n\n")

        self.log(f"匹配说明文件已生成: {report_path}")
        return report_path

    def get_match_report_html(self) -> str:
        if not self.matched_records:
            return ""

        keyword_records = [r for r in self.matched_records if r['match_type'] == "关键词匹配"]
        merged = {}

        for record in keyword_records:
            key = (record['raw_name'], record['matched_industry'], record['matched_keywords'][0] if record['matched_keywords'] else "")
            if key not in merged:
                merged[key] = {
                    'raw_name': record['raw_name'],
                    'matched_industry': record['matched_industry'],
                    'matched_keyword': record['matched_keywords'][0] if record['matched_keywords'] else "",
                    'months': []
                }
            merged[key]['months'].append(record['month'])

        def format_month(month_str):
            year, month = month_str.split('-')
            return f"{year}年{int(month)}月"

        html = "<h2>行业匹配说明</h2>"
        html += f"<p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>"
        html += "<h3>匹配记录明细 (仅关键词匹配):</h3>"

        for item in merged.values():
            html += "<div style='border: 1px solid #ccc; padding: 10px; margin: 10px 0;'>"
            html += f"<p><strong>原文名称:</strong> {item['raw_name']}</p>"
            html += f"<p><strong>匹配行业:</strong> {item['matched_industry']}</p>"
            html += f"<p><strong>匹配关键词:</strong> {item['matched_keyword']}</p>"
            month_strs = [format_month(m) for m in item['months']]
            html += f"<p><strong>出现月份:</strong> {', '.join(month_strs)}</p>"
            html += "</div>"

        return html

    def start_crawl(self, base_url: str, start_year: int, start_month: int, end_year: int, end_month: int) -> Dict:
        if self.crawl_lock:
            return {'success': False, 'message': '采集任务正在运行中，请稍后再试'}

        self.crawl_lock = True
        self.clear_logs()
        self.matched_records = []

        try:
            self.log("=" * 60)
            self.log("开始数据采集...")

            all_links = self.fetch_page_list(base_url)

            if not all_links:
                self.log("未找到任何链接，采集结束")
                return {'success': False, 'message': '未找到任何数据链接'}

            filtered_links = self.filter_by_time_range(all_links, start_year, start_month, end_year, end_month)

            if not filtered_links:
                self.log("时间范围内无数据，采集结束")
                return {'success': False, 'message': '所选时间范围内无数据'}

            all_page_data = []
            for link in filtered_links:
                page_data = self.parse_detail_page(link['url'], link['year'], link['month'])
                all_page_data.append(page_data)
                self.sleep()

            monthly_data, matched_records = self.process_all_data(all_page_data)

            csv_path = self.generate_csv(monthly_data, start_year, start_month, end_year, end_month)
            report_path = self.generate_match_report()

            self.log("=" * 60)
            self.log("采集完成！")

            return {
                'success': True,
                'message': '数据采集完成',
                'csv_path': csv_path,
                'report_path': report_path
            }

        except Exception as e:
            self.log(f"采集出错: {str(e)}")
            return {'success': False, 'message': f'采集过程出错: {str(e)}'}

        finally:
            self.crawl_lock = False

    def save_config(self, filepath: str, config: Dict):
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)

    def load_config(self, filepath: str) -> Dict:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)


crawler_service = CrawlerService()
