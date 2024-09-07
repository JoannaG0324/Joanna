// 点击表格行时显示模态框
function showStockChart(stockCode, stockName) {
    $('#stockModalLabel').text('Stock Chart for ' + stockName);
    window.currentStockCode = stockCode;  // 储存 stockCode
    $('#stockModal').modal('show');  // 显示模态框
}

// 处理模态框显示后的图表渲染
$('#stockModal').on('shown.bs.modal', function () {
    var chartDom = document.getElementById('chart');
    var myChart = echarts.init(chartDom);

    // 使用全局变量获取当前 stockCode
    $.ajax({
        url: '/get_stock_history',
        method: 'GET',
        data: { stock_code: window.currentStockCode },
        success: function (data) {
            var option = createChartOption(data);  // 确保 createChartOption 函数已定义
            myChart.setOption(option);

            // 添加显示最近5个数据的按钮
            myChart.setOption({
                toolbox: {
                    feature: {
                        myTool_5d: {
                            show: true,
                            title: '5D',
                            icon: 'path://M344.615 435.956l-65.544 58.338-28.899-32.213c-15.182-15.336-22.657-30.71-24.121-47.549-0.54-6.165-1.195-13.833-0.463-23.08l33.37-373.033 111.782 119.72-26.125 297.817zM627.829 901.16l64.541 70.63-31.25 29.169c-18.34 18.418-34.833 23.042-70.128 23.042H293.174l-57.413-59.88 68.164-62.961h323.904zM395.478 122.84L281.884 0h449.79l57.798 64.503-68.549 58.338H395.478z m239.094 328.49c36.837 0 41.73 3.044 68.819 32.29l27.127 29.169-72.942 61.344H408.887c-47.587 0-58.762-4.586-84.078-30.71l-27.127-29.17 71.247-62.962h265.643z m44.312 138.177l68.434-59.88 30.325 30.711c11.83 12.253 19.536 30.71 20.769 44.505 0.655 7.668 0.192 19.96-0.116 33.793L779.57 845.864c-2.928 36.838-11.79 58.377-26.048 70.63l-42.694 38.379-57.837-64.504 25.894-300.86z', // 自定义图标
                            onclick: function () {
                                myChart.dispatchAction({
                                    type: 'dataZoom',
                                    start: Math.max(0, 100 - (5 / data.length) * 100),
                                    end: 100
                                });
                            }
                        },  // 5D
                        myTool_20d: {
                            show: true,
                            title: '20D',
                            icon: 'path://M613.449865 887.558615l-57.825401 68.910306-17.419136-24.519604c-8.786192-13.792278-14.200938-27.584556-15.120423-41.376834-0.306495-4.597426 0.40866-10.727327 0.153247-15.27367l20.177592-300.262995 48.170807-59.766537 42.347401 59.766537-20.484087 312.522797z m28.452959-434.967574l-50.265191 62.831487-40.3041-62.831487 16.755063-266.497456c2.196548-18.389704 2.656291-29.117031 3.371446-35.246932 1.379228-13.741195 6.334231-24.519605 15.835578-35.19585l30.445176-36.728325 43.930959 67.377831-19.768931 306.290732z m208.978549 448.810934l48.885962 73.507733-25.183677 30.700588c-10.420832 13.741195-22.57847 18.389704-44.084207 18.389704h-199.22179c-21.505737 0-34.327447-6.129901-43.062556-18.389704l-20.126509-30.700588 60.890352-73.507733h221.902425zM678.426818 124.845655l-47.251322-65.845356 21.812232-30.700588c13.536865-18.389704 26.613988-26.05208 43.573381-26.052081h207.139579c15.835578 0 31.262496 10.727327 41.325751 26.052081l17.725631 29.11703-59.051382 77.377831h-225.27387z m211.379428 450.241245l50.316272-61.24793 40.201936 61.24793-16.806146 265.016063c-0.970568 19.922179-2.45196 32.181981-2.145466 36.779408-1.379228 13.741195-6.436396 22.936047-16.959393 35.195849l-32.794971 35.19585-41.325751-61.24793 19.513519-310.93924z m28.861618-428.837673l58.898134-68.910307 13.84336 21.454655c10.063255 15.324753 15.478001 29.117031 16.448569 44.390701 0.40866 6.129901 0.81732 12.259802 0.051082 18.389704l-18.542951 291.017061-51.49117 61.299012-39.078121-61.299012 19.871097-306.341814zM119.890652 883.727427l-97.05677 118.000599 23.80445-364.524793c0.561908-9.194852 1.123815-18.389704 2.86062-26.052081 1.787888-7.662377 3.626858-13.741195 6.691809-18.389703 0 0 6.283149-7.662377 16.601816-21.454655l30.087599-39.793275 37.188067 53.585553L119.890652 883.727427z m254.135488 15.375835l41.121421 61.299013-48.937045 61.299012H35.298015l101.296617-122.546942 237.431508-0.051083z m24.928265-347.718647c-12.41305 16.857228-22.271974 21.454654-48.119725 21.454655H157.283049l-40.968173-59.766537 32.028734-44.390702c4.137683-6.129901 9.450264-10.727327 14.86501-13.792277 5.414746-3.013868 9.807842-4.597426 15.426918-4.597426h212.707573l40.968173 59.766537-33.356879 41.32575zM173.322957 122.546942l-40.968173-59.766537L180.065849 0h229.564801c22.476304 0 33.152549 7.662377 45.667764 27.584556l15.06934 24.519604-57.365659 70.442782H173.322957z m251.12162 19.871097l58.489475-70.442782 19.973261 30.649506c2.45196 3.064951 5.210416 10.727327 8.122119 21.454654 1.43031 4.597426 1.736805 9.245934 1.992218 13.792278 0.40866 6.129901-0.051083 16.857228-0.306495 30.649506l-15.835578 226.653098c-1.63464 27.584556-5.414746 38.311883-19.871097 58.182979l-31.211413 39.844358-40.866008-58.182979 19.513519-292.600618z m28.861618-428.837673l58.898134-68.910307 13.84336 21.454655c10.063255 15.324753 15.478001 29.117031 16.448569 44.390701 0.40866 6.129901 0.81732 12.259802 0.051082 18.389704l-18.542951 291.017061-51.49117 61.299012-39.078121-61.299012 19.871097-306.341814z', // 自定义图标
                            onclick: function () {
                                myChart.dispatchAction({
                                    type: 'dataZoom',
                                    start: Math.max(0, 100 - (20 / data.length) * 100),
                                    end: 100
                                });
                            }
                        } //20D
                    }
                }
            });
        },
        error: function (error) {
            console.error('Error fetching stock history:', error);
        }
    });
});

// 创建K线图表的配置
function createChartOption(rawData) {
    const upColor = '#00da3c';
    const downColor = '#ec0000';
    function splitData(rawData) {
        let categoryData = [];
        let values = [];
        for (let i = 0; i < rawData.length; i++) {
            // 因为后端已经格式化日期，所以直接使用
            categoryData.push(rawData[i].date);  
            values.push([rawData[i].open, rawData[i].close, rawData[i].low, rawData[i].high, rawData[i].volume]);
        }
        return {
            categoryData: categoryData,
            values: values,
        };
    }
    // 计算MA
    function calculateMA(dayCount, data) {
        var result = [];
        for (var i = 0, len = data.values.length; i < len; i++) {
            if (i < dayCount) {
                result.push('-');
                continue;
            }
            var sum = 0;
            for (var j = 0; j < dayCount; j++) {
                sum += data.values[i - j][1];
            }
            result.push((sum / dayCount).toFixed(2));
        }
        return result;
    }

    var data = splitData(rawData);

    return {
        animation: false,
        legend: {
            top: 10,
            left: 'center',
            data: ['Stock Price', 'MA5', 'MA10', 'MA20', 'MA30']
        },
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'cross'
            },
        },
        xAxis: {
            type: 'category',
            data: data.categoryData,
            boundaryGap: false,
            axisLine: { onZero: false },
            splitLine: { show: false },
            min: 'dataMin',
            max: 'dataMax'
        },
        yAxis: {
            scale: true,
            splitArea: {
                show: true
            }
        },
        dataZoom: [
            {
                type: 'slider',
                show: true,
                xAxisIndex: [0],
                start: 0, // Initial start value (percentage)
                end: 100, // Initial end value (percentage)
                handleSize: '100%', // Handle size
                handleStyle: {
                    color: '#aaa' // Handle color
                },
                textStyle: {
                    color: '#333' // Text color
                },
                borderColor: '#ddd', // Border color
            },
            {
                type: 'inside',
                xAxisIndex: [0],
                start: 0, // Initial start value (percentage)
                end: 100  // Initial end value (percentage)
            }
        ],
        series: [
            {
                name: 'Stock Price',
                type: 'candlestick',
                data: data.values,
                itemStyle: {
                    color: downColor, //upColor
                    color0: upColor, //downColor
                    borderColor: undefined,
                    borderColor0: undefined
                }
            },
            {
                name: 'MA5',
                type: 'line',
                data: calculateMA(5, data),
                smooth: true,
                lineStyle: {
                    opacity: 0.5
                }
            },
            {
                name: 'MA10',
                type: 'line',
                data: calculateMA(10, data),
                smooth: true,
                lineStyle: {
                    opacity: 0.5
                }
            },
            {
                name: 'MA20',
                type: 'line',
                data: calculateMA(20, data),
                smooth: true,
                lineStyle: {
                    opacity: 0.5
                }
            },
            {
                name: 'MA30',
                type: 'line',
                data: calculateMA(30, data),
                smooth: true,
                lineStyle: {
                    opacity: 0.5
                }
            }
        ]
    };
}