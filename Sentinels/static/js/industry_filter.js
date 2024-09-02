let filters = {
    level1: [],
    level2: [],
    level3: [],
    level4: []
}; // 存储行业层级数据

// 加载数据
function loadData() {
    data = [
        // 从后端获取的数据数组
        {% for row in stock_prices %}
        {
            date: '{{ row['date'] }}',
            industry_level_1_name: '{{ row['industry_level_1_name'] }}',
            industry_level_2_name: '{{ row['industry_level_2_name'] }}',
            industry_level_3_name: '{{ row['industry_level_3_name'] }}',
            industry_level_4_name: '{{ row['industry_level_4_name'] }}',
            stock_code: '{{ row['stock_code'] }}',
            stock_name: '{{ row['stock_name'] }}',
            open: {{ row['open'] }},
            close: {{ row['close'] }},
            price_change_percentage: {{ row['price_change_percentage'] }},
            '20_day_ma': {{ row['20_day_ma'] }},
            ATR: {{ row['ATR'] }},
            eps: {{ row['eps'] }},
            pe: {{ row['pe'] }},
            net_assets_per_share: {{ row['net_assets_per_share'] }},
            roe: {{ row['roe'] }},
            pb: {{ row['pb'] }},
            operating_cash_flow_per_share: {{ row['operating_cash_flow_per_share'] }},
            gross_profit_margin: {{ row['gross_profit_margin'] }},
            latest_announcement_date: '{{ row['latest_announcement_date'] }}'
        },
        {% endfor %}
    ];

    populateFilters(); // 填充筛选下拉菜单
    filterData(); // 默认加载所有数据
}

// 填充筛选下拉菜单
function populateFilters() {
    const level1Select = document.getElementById('industry-level-1');
    const level2Select = document.getElementById('industry-level-2');
    const level3Select = document.getElementById('industry-level-3');
    const level4Select = document.getElementById('industry-level-4');

    // 从数据中提取唯一的行业层级
    filters.level1 = [...new Set(data.map(item => item.industry_level_1_name))];
    filters.level2 = [...new Set(data.map(item => item.industry_level_2_name))];
    filters.level3 = [...new Set(data.map(item.industry_level_3_name))];
    filters.level4 = [...new Set(data.map(item.industry_level_4_name))];

    // 填充 Level 1 下拉菜单
    level1Select.innerHTML = '<option value="">Select Level 1 Industry</option>';
    filters.level1.forEach(option => {
        const optionElement = document.createElement('option');
        optionElement.value = option;
        optionElement.textContent = option;
        level1Select.appendChild(optionElement);
    });

    // 清空下级层级选项
    level2Select.innerHTML = '<option value="">Select Level 2 Industry</option>';
    level3Select.innerHTML = '<option value="">Select Level 3 Industry</option>';
    level4Select.innerHTML = '<option value="">Select Level 4 Industry</option>';
}

function updateLevel2() {
    const level1Select = document.getElementById('industry-level-1');
    const level2Select = document.getElementById('industry-level-2');
    const selectedLevel1 = level1Select.value;

    if (selectedLevel1) {
        // 根据 Level 1 筛选 Level 2 选项
        const level2Options = [...new Set(data.filter(item => item.industry_level_1_name === selectedLevel1)
                                         .map(item => item.industry_level_2_name))];

        level2Select.innerHTML = '<option value="">Select Level 2 Industry</option>';
        level2Options.forEach(option => {
            const optionElement = document.createElement('option');
            optionElement.value = option;
            optionElement.textContent = option;
            level2Select.appendChild(optionElement);
        });
    } else {
        level2Select.innerHTML = '<option value="">Select Level 2 Industry</option>';
    }

    // 清空 Level 3 和 Level 4 选项
    updateLevel3();
}

function updateLevel3() {
    const level2Select = document.getElementById('industry-level-2');
    const level3Select = document.getElementById('industry-level-3');
    const selectedLevel2 = level2Select.value;

    if (selectedLevel2) {
        // 根据 Level 2 筛选 Level 3 选项
        const level3Options = [...new Set(data.filter(item => item.industry_level_2_name === selectedLevel2)
                                         .map(item => item.industry_level_3_name))];

        level3Select.innerHTML = '<option value="">Select Level 3 Industry</option>';
        level3Options.forEach(option => {
            const optionElement = document.createElement('option');
            optionElement.value = option;
            optionElement.textContent = option;
            level3Select.appendChild(optionElement);
        });
    } else {
        level3Select.innerHTML = '<option value="">Select Level 3 Industry</option>';
    }

    // 清空 Level 4 选项
    updateLevel4();
}

// 更新第四级行业
function updateLevel4() {
    const level3Select = document.getElementById('industry-level-3');
    const level4Select = document.getElementById('industry-level-4');
    const selectedLevel3 = level3Select.value;

    console.log('Selected Level 3:', selectedLevel3); // 调试信息

    if (selectedLevel3) {
        // 根据选中的第三级行业来过滤第四级行业
        const level4Options = [...new Set(data
            .filter(item => item.industry_level_3_name === selectedLevel3) // 仅过滤出符合第三级行业的数据
            .map(item => item.industry_level_4_name))]; // 提取出第四级行业的选项

        console.log('Level 4 Options after filtering:', level4Options); // 调试信息

        // 清空并填充第四级菜单
        level4Select.innerHTML = '<option value="">Select Level 4 Industry</option>';
        level4Options.forEach(option => {
            const optionElement = document.createElement('option');
            optionElement.value = option;
            optionElement.textContent = option;
            level4Select.appendChild(optionElement);
        });
    } else {
        // 如果没有选择第三级行业，清空第四级菜单
        level4Select.innerHTML = '<option value="">Select Level 4 Industry</option>';
    }

    filterData(); // 重新过滤数据
}

// 筛选数据 （在筛选时使用 filteredData 而不是 data）
function filterData() {
    // 假设你有一系列筛选条件
    const selectedLevel1 = document.getElementById('industry-level-1').value;
    const selectedLevel2 = document.getElementById('industry-level-2').value;
    const selectedLevel3 = document.getElementById('industry-level-3').value;
    const selectedLevel4 = document.getElementById('industry-level-4').value;

    filteredData = data;

    if (selectedLevel1) {
        filteredData = filteredData.filter(item => item.industry_level_1_name === selectedLevel1);
    }

    if (selectedLevel2) {
        filteredData = filteredData.filter(item => item.industry_level_2_name === selectedLevel2);
    }

    if (selectedLevel3) {
        filteredData = filteredData.filter(item => item.industry_level_3_name === selectedLevel3);
    }

    if (selectedLevel4) {
        filteredData = filteredData.filter(item => item.industry_level_4_name === selectedLevel4);
    }

    // 更新表格内容
    sortTable(currentSort.column);  // 按照当前的排序规则更新表格
}

// 页面加载时初始化数据
document.addEventListener('DOMContentLoaded', () => {
    loadData();
    filteredData = [...data];  // 初始化时将所有数据赋值给 filteredData
    filterData();  // 初始化筛选数据
});