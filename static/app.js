let selectedIndustry = null;
let crawlInProgress = false;
let logPollInterval = null;

document.addEventListener('DOMContentLoaded', function() {
    loadConfig();
});

async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        const data = await response.json();
        renderIndustries(data.industries);
        if (data.base_url) {
            document.getElementById('baseUrl').value = data.base_url;
        }
        if (data.start_year) {
            document.getElementById('startYear').value = data.start_year;
        }
        if (data.start_month) {
            document.getElementById('startMonth').value = data.start_month;
        }
        if (data.end_year) {
            document.getElementById('endYear').value = data.end_year;
        }
        if (data.end_month) {
            document.getElementById('endMonth').value = data.end_month;
        }
        if (data.logs && data.logs.length > 0) {
            renderLogs(data.logs);
        }
    } catch (error) {
        console.error('加载配置失败:', error);
    }
}

function renderIndustries(industries) {
    const list = document.getElementById('industryList');
    list.innerHTML = '';
    industries.forEach(industry => {
        const li = document.createElement('li');
        li.onclick = () => selectIndustry(industry, li);

        const deleteBtn = document.createElement('button');
        deleteBtn.className = 'delete-btn';
        deleteBtn.textContent = '删除';
        deleteBtn.onclick = (e) => {
            e.stopPropagation();
            deleteIndustry(industry);
        };

        const span = document.createElement('span');
        span.textContent = industry;
        li.appendChild(span);
        li.appendChild(deleteBtn);

        list.appendChild(li);
    });
}

async function selectIndustry(industry, liElement) {
    document.querySelectorAll('#industryList li').forEach(li => li.classList.remove('selected'));
    liElement.classList.add('selected');
    selectedIndustry = industry;

    document.getElementById('selectedIndustryName').textContent = `当前行业：${industry}`;
    document.getElementById('newKeyword').disabled = false;
    document.getElementById('addKeywordBtn').disabled = false;

    try {
        const response = await fetch(`/api/keywords/${encodeURIComponent(industry)}`);
        const data = await response.json();
        renderKeywords(data.keywords);
    } catch (error) {
        console.error('加载关键词失败:', error);
    }
}

function renderKeywords(keywords) {
    const list = document.getElementById('keywordList');
    list.innerHTML = '';
    keywords.forEach(keyword => {
        const li = document.createElement('li');

        const span = document.createElement('span');
        span.textContent = keyword;
        li.appendChild(span);

        const deleteBtn = document.createElement('button');
        deleteBtn.className = 'delete-btn';
        deleteBtn.textContent = '删除';
        deleteBtn.onclick = () => deleteKeyword(keyword);
        li.appendChild(deleteBtn);

        list.appendChild(li);
    });
}

async function addIndustry() {
    const input = document.getElementById('newIndustry');
    const industry = input.value.trim();
    if (!industry) return;

    try {
        const response = await fetch('/api/industries', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ industry })
        });
        const data = await response.json();
        if (data.success) {
            renderIndustries(data.industries);
            input.value = '';
        } else {
            alert('添加失败，行业可能已存在');
        }
    } catch (error) {
        console.error('添加行业失败:', error);
    }
}

async function deleteIndustry(industry) {
    if (!confirm(`确定要删除行业"${industry}"吗？`)) return;

    try {
        const response = await fetch('/api/industries', {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ industry })
        });
        const data = await response.json();
        if (data.success) {
            renderIndustries(data.industries);
            if (selectedIndustry === industry) {
                selectedIndustry = null;
                document.getElementById('selectedIndustryName').textContent = '请先选择一个行业';
                document.getElementById('newKeyword').disabled = true;
                document.getElementById('addKeywordBtn').disabled = true;
                document.getElementById('keywordList').innerHTML = '';
            }
        } else {
            alert('删除失败，默认行业不可删除');
        }
    } catch (error) {
        console.error('删除行业失败:', error);
    }
}

async function addKeyword() {
    if (!selectedIndustry) return;
    const input = document.getElementById('newKeyword');
    const keyword = input.value.trim();
    if (!keyword) return;

    try {
        const response = await fetch('/api/keywords', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ industry: selectedIndustry, keyword })
        });
        const data = await response.json();
        if (data.success) {
            renderKeywords(data.keywords);
            input.value = '';
        } else {
            alert('添加失败，关键词可能已存在');
        }
    } catch (error) {
        console.error('添加关键词失败:', error);
    }
}

async function deleteKeyword(keyword) {
    if (!selectedIndustry) return;
    if (!confirm(`确定要删除关键词"${keyword}"吗？`)) return;

    try {
        const response = await fetch('/api/keywords', {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ industry: selectedIndustry, keyword })
        });
        const data = await response.json();
        if (data.success) {
            renderKeywords(data.keywords);
        }
    } catch (error) {
        console.error('删除关键词失败:', error);
    }
}

async function resetConfig() {
    if (!confirm('确定要重置为默认配置吗？')) return;

    try {
        const response = await fetch('/api/config/reset', { method: 'POST' });
        const data = await response.json();
        if (data.success) {
            renderIndustries(data.industries);
            selectedIndustry = null;
            document.getElementById('selectedIndustryName').textContent = '请先选择一个行业';
            document.getElementById('newKeyword').disabled = true;
            document.getElementById('addKeywordBtn').disabled = true;
            document.getElementById('keywordList').innerHTML = '';
        }
    } catch (error) {
        console.error('重置配置失败:', error);
    }
}

async function startCrawl() {
    if (crawlInProgress) return;

    const baseUrl = document.getElementById('baseUrl').value;
    const startYear = parseInt(document.getElementById('startYear').value);
    const startMonth = parseInt(document.getElementById('startMonth').value);
    const endYear = parseInt(document.getElementById('endYear').value);
    const endMonth = parseInt(document.getElementById('endMonth').value);

    crawlInProgress = true;
    document.getElementById('crawlBtn').disabled = true;
    document.getElementById('crawlBtn').textContent = '采集中...';

    try {
        const response = await fetch('/api/crawl', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ baseUrl, startYear, startMonth, endYear, endMonth })
        });

        logPollInterval = setInterval(pollLogs, 500);

        const data = await response.json();

        if (data.success) {
            document.getElementById('downloadCsvBtn').disabled = false;
            document.getElementById('downloadReportBtn').disabled = false;
            document.getElementById('viewReportBtn').disabled = false;
        } else {
            alert(data.message);
        }
    } catch (error) {
        console.error('采集失败:', error);
        alert('采集过程出错');
    } finally {
        clearInterval(logPollInterval);
        crawlInProgress = false;
        document.getElementById('crawlBtn').disabled = false;
        document.getElementById('crawlBtn').textContent = '开始采集';
        await pollLogs();
    }
}

async function pollLogs() {
    try {
        const response = await fetch('/api/logs');
        const data = await response.json();
        renderLogs(data.logs);
    } catch (error) {
        console.error('获取日志失败:', error);
    }
}

function renderLogs(logs) {
    const logArea = document.getElementById('logArea');
    logArea.innerHTML = logs.map(log => `<p>${log}</p>`).join('');
    logArea.scrollTop = logArea.scrollHeight;
}

function clearLogs() {
    document.getElementById('logArea').innerHTML = '';
}

function downloadCsv() {
    window.location.href = '/api/download/csv';
}

function downloadReport() {
    window.location.href = '/api/download/report';
}

async function viewMatchReport() {
    try {
        const response = await fetch('/api/report/html');
        const data = await response.json();
        if (data.html) {
            document.getElementById('reportContent').innerHTML = data.html;
            document.getElementById('reportModal').style.display = 'block';
        } else {
            alert('暂无匹配记录，请先运行采集');
        }
    } catch (error) {
        console.error('获取匹配说明失败:', error);
    }
}

function closeModal() {
    document.getElementById('reportModal').style.display = 'none';
}

window.onclick = function(event) {
    const modal = document.getElementById('reportModal');
    if (event.target === modal) {
        closeModal();
    }
}

function exportConfig() {
    window.location.href = '/api/config/export';
}

async function importConfig(event) {
    const file = event.target.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = async function(e) {
        try {
            const config = JSON.parse(e.target.result);
            const response = await fetch('/api/config/import', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(config)
            });
            const data = await response.json();
            if (data.success) {
                alert(data.message);
                loadConfig();
            } else {
                alert(data.message);
            }
        } catch (error) {
            console.error('导入配置失败:', error);
            alert('导入配置失败，请检查文件格式');
        }
    };
    reader.readAsText(file);
    event.target.value = '';
}
