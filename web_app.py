from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
from crawler_service import crawler_service, BASE_URL
import os
from datetime import datetime
import json

app = Flask(__name__)


@app.route('/')
def index():
    current_year = datetime.now().year
    current_month = datetime.now().month
    time_range = crawler_service.get_time_range()
    return render_template('index.html',
                         base_url=crawler_service.get_base_url(),
                         start_year=time_range['start_year'],
                         start_month=time_range['start_month'],
                         end_year=time_range['end_year'],
                         end_month=time_range['end_month'],
                         current_year=current_year)


@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify({
        'base_url': crawler_service.get_base_url(),
        'start_year': crawler_service.start_year,
        'start_month': crawler_service.start_month,
        'end_year': crawler_service.end_year,
        'end_month': crawler_service.end_month,
        'industries': crawler_service.get_industries(),
        'keywords': crawler_service.get_keywords(),
        'logs': crawler_service.get_logs()
    })


@app.route('/api/config', methods=['POST'])
def save_config():
    data = request.json
    if 'base_url' in data:
        crawler_service.set_base_url(data['base_url'])
    if 'start_year' in data:
        crawler_service.start_year = data['start_year']
    if 'start_month' in data:
        crawler_service.start_month = data['start_month']
    if 'end_year' in data:
        crawler_service.end_year = data['end_year']
    if 'end_month' in data:
        crawler_service.end_month = data['end_month']
    if 'industries' in data:
        crawler_service.set_industries(data['industries'])
    if 'keywords' in data:
        crawler_service.set_keywords(data['keywords'])
    return jsonify({'success': True})


@app.route('/api/config/export', methods=['GET'])
def export_config():
    config = crawler_service.export_config()
    config_path = os.path.join(os.path.dirname(__file__), 'output', 'web_config_export.json')
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, ensure_ascii=False, indent=2)
    return send_file(config_path, as_attachment=True, download_name='web_config.json')


@app.route('/api/config/import', methods=['POST'])
def import_config():
    try:
        data = request.json
        crawler_service.import_config(data)
        return jsonify({
            'success': True,
            'message': '配置导入成功',
            'config': crawler_service.export_config()
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'导入失败: {str(e)}'})


@app.route('/api/config/reset', methods=['POST'])
def reset_config():
    crawler_service.reset_to_default()
    return jsonify({
        'success': True,
        'base_url': crawler_service.get_base_url(),
        'start_year': crawler_service.start_year,
        'start_month': crawler_service.start_month,
        'end_year': crawler_service.end_year,
        'end_month': crawler_service.end_month,
        'industries': crawler_service.get_industries(),
        'keywords': crawler_service.get_keywords()
    })


@app.route('/api/industries', methods=['GET'])
def get_industries():
    return jsonify({'industries': crawler_service.get_industries()})


@app.route('/api/industries', methods=['POST'])
def add_industry():
    data = request.json
    industry = data.get('industry', '')
    success = crawler_service.add_industry(industry)
    return jsonify({
        'success': success,
        'industries': crawler_service.get_industries()
    })


@app.route('/api/industries', methods=['DELETE'])
def delete_industry():
    data = request.json
    industry = data.get('industry', '')
    success = crawler_service.delete_industry(industry)
    return jsonify({
        'success': success,
        'industries': crawler_service.get_industries()
    })


@app.route('/api/keywords/<industry>', methods=['GET'])
def get_keywords(industry):
    keywords = crawler_service.get_keywords().get(industry, [])
    return jsonify({'keywords': keywords})


@app.route('/api/keywords', methods=['POST'])
def add_keyword():
    data = request.json
    industry = data.get('industry', '')
    keyword = data.get('keyword', '')
    success = crawler_service.add_keyword(industry, keyword)
    return jsonify({
        'success': success,
        'keywords': crawler_service.get_keywords().get(industry, [])
    })


@app.route('/api/keywords', methods=['DELETE'])
def delete_keyword():
    data = request.json
    industry = data.get('industry', '')
    keyword = data.get('keyword', '')
    success = crawler_service.delete_keyword(industry, keyword)
    return jsonify({
        'success': success,
        'keywords': crawler_service.get_keywords().get(industry, [])
    })


@app.route('/api/crawl', methods=['POST'])
def crawl():
    data = request.json
    base_url = data.get('base_url', BASE_URL)
    start_year = int(data.get('start_year', 2024))
    start_month = int(data.get('start_month', 1))
    end_year = int(data.get('end_year', datetime.now().year))
    end_month = int(data.get('end_month', datetime.now().month))

    result = crawler_service.start_crawl(base_url, start_year, start_month, end_year, end_month)
    return jsonify(result)


@app.route('/api/logs', methods=['GET'])
def get_logs():
    return jsonify({'logs': crawler_service.get_logs()})


@app.route('/api/download/csv', methods=['GET'])
def download_csv():
    csv_path = os.path.join(os.path.dirname(__file__), 'output', '固定资产投资月度数据.csv')
    if os.path.exists(csv_path):
        return send_file(csv_path, as_attachment=True, download_name='固定资产投资月度数据.csv')
    return jsonify({'success': False, 'message': 'CSV文件不存在'}), 404


@app.route('/api/download/report', methods=['GET'])
def download_report():
    report_path = os.path.join(os.path.dirname(__file__), 'output', '行业匹配说明.txt')
    if os.path.exists(report_path):
        return send_file(report_path, as_attachment=True, download_name='行业匹配说明.txt')
    return jsonify({'success': False, 'message': '匹配说明文件不存在'}), 404


@app.route('/api/report/html', methods=['GET'])
def get_report_html():
    return jsonify({'html': crawler_service.get_match_report_html()})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
