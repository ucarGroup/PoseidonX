import React from 'react';
import { Form, Row, Col, Select, Card, DatePicker,Button, Tabs, List} from 'antd';
import {auth, qs, util} from 'libs';
import { Chart, Geom, Axis, Tooltip} from 'bizcharts';
import '../../styles/glob.less'
import moment from 'moment';
const FormItem = Form.Item;
const { RangePicker } = DatePicker;

class JstormTaskMoniter extends React.Component {

    timeTicket = null;

    constructor(props, context) {
        super(props, context);
        this.state = {
            currentTaskId:null,
            taskDatas:[],
            sendTpsData: null,
            sendTpsChartObject: null,
            recvTpsData: null,
            recvTpsChartObject: null,
            nettyCliSendSpeedData: null,
            nettyCliSendSpeedChartObject: null,
            nettySrvRecvSpeedData: null,
            nettySrvRecvSpeedChartObject: null,
            fullGcData: null,
            fullGcChartObject: null,
            memoryUsedData: null,
            memoryUsedChartObject: null,
            heapMemoryData: null,
            heapMemoryChartObject: null,
            cpuUsedRatioData: null,
            cpuUsedRatioChartObject: null,
            timeTickCount:10,
            timeTickInterval :60000,
            reportType:0,
            sendTpsData4Time: null,
            sendTpsChartObject4Time: null,
            recvTpsData4Time: null,
            recvTpsChartObject4Time: null,
            nettyCliSendSpeedData4Time: null,
            nettyCliSendSpeedChartObject4Time: null,
            nettySrvRecvSpeedData4Time: null,
            nettySrvRecvSpeedChartObject4Time: null,
            fullGcData4Time: null,
            fullGcChartObject4Time: null,
            memoryUsedData4Time: null,
            memoryUsedChartObject4Time: null,
            heapMemoryData4Time: null,
            heapMemoryChartObject4Time: null,
            cpuUsedRatioData4Time: null,
            cpuUsedRatioChartObject4Time: null,
            workerErrorData: [],
        }
    }

    //组件加载时执行查询
    componentWillMount() {
        let postData = {
            engineType:0,
        };
        qs.form("/streamsuite/task/task/getTaskByUser", postData).then((data) => {
            this.setState({
                taskDatas:data
            });
        });
    }

    componentWillUnmount() {
        if (this.timeTicket) {
            clearInterval(this.timeTicket);
        }
    };

    fetchInitDate = (taskId,hourNumber) => {
        let newtime = (new Date()).getTime();
        //取前一小时
        let startTime = newtime-3600*1000*hourNumber;
        //拼出查询方法要求的参数类型
        let rangeTime = (new Date(startTime))+","+(new Date(newtime));

        if(taskId == null){
          taskId = this.state.currentTaskId;
        }
        let postData = {
            taskId:taskId,
            rangeTime:rangeTime
        };
        let postUrl = "/streamsuite/jstormMonitor/getReportDataByTime";
        qs.form(postUrl,postData).then((rsData) => {
            let sendTpsData = [];
            let recvTpsData = [];
            let nettyCliSendSpeedData = [];
            let nettySrvRecvSpeedData = [];
            let fullGcData = [];
            let memoryUsedData = [];
            let heapMemoryData = [];
            let cpuUsedRatioData = [];

            if(rsData != null && rsData.length > 0){
                rsData.forEach(function(item, index, array) {
                    let metricValues = item.groupToTimeline.defaultGroup.metricValues;
                    let reportTitle = item.title;
                    metricValues.forEach(function(inneritem, index, array) {

                        let metricValue = inneritem.metricValue;
                        if(inneritem.metricValue == '-1'){
                            metricValue = null;
                        }

                        if(reportTitle == 'SendTps'){
                            sendTpsData.push({ minute: inneritem.time , sendTps:  parseInt(metricValue)} );
                        }
                        if(reportTitle == 'RecvTps'){
                            recvTpsData.push({ minute: inneritem.time, recvTps: parseInt(metricValue)}) ;
                        }
                        if(reportTitle == 'NettyCliSendSpeed'){
                            nettyCliSendSpeedData.push({ minute: inneritem.time, nettyCliSendSpeed: parseInt(metricValue)}) ;
                        }
                        if(reportTitle == 'NettySrvRecvSpeed'){
                            nettySrvRecvSpeedData.push({ minute: inneritem.time, nettySrvRecvSpeed: parseInt(metricValue)}) ;
                        }
                        if(reportTitle == 'FullGc'){
                            fullGcData.push({ minute: inneritem.time, fullGc: parseInt(metricValue)}) ;
                        }
                        if(reportTitle == 'MemoryUsed'){
                            memoryUsedData.push( { minute: inneritem.time, memoryUsed: parseFloat(metricValue)}) ;
                        }
                        if(reportTitle == 'HeapMemory'){
                            heapMemoryData.push({ minute: inneritem.time, heapMemory: parseFloat(metricValue)}) ;
                        }
                        if(reportTitle == 'CpuUsedRatio'){
                            cpuUsedRatioData.push({ minute: inneritem.time, cpuUsedRatio: parseInt(metricValue)}) ;
                        }

                    });
                });
            }else{
                sendTpsData.push({ minute: newtime, sendTps: null}) ;
                recvTpsData.push({ minute: newtime, recvTps: null}) ;
                nettyCliSendSpeedData.push({ minute: newtime, nettyCliSendSpeed: null}) ;
                nettySrvRecvSpeedData.push({ minute: newtime, nettySrvRecvSpeed: null}) ;
                fullGcData.push({ minute: newtime, fullGc: null}) ;
                memoryUsedData.push( { minute: newtime, memoryUsed: null}) ;
                heapMemoryData.push({ minute: newtime, heapMemory: null}) ;
                cpuUsedRatioData.push({ minute: newtime, cpuUsedRatio: null}) ;
            }
            this.setState({
                sendTpsData: sendTpsData,
                recvTpsData: recvTpsData,
                nettyCliSendSpeedData: nettyCliSendSpeedData,
                nettySrvRecvSpeedData: nettySrvRecvSpeedData,
                fullGcData: fullGcData,
                memoryUsedData: memoryUsedData,
                heapMemoryData: heapMemoryData,
                cpuUsedRatioData: cpuUsedRatioData,
                searchStartTime:startTime,
                searchEndTime:newtime,
            });
        });
    }
    fetchNewDate = (taskId) => {
        let now = new Date();
        let newtime = now.getTime();

        if(taskId == null){
            taskId = this.state.currentTaskId;
        }
        let postData = {
            taskId:taskId
        };
        let postUrl = "/streamsuite/jstormMonitor/getReportRecentDataByTaskId";
        qs.form(postUrl, postData).then((rsdata) => {

            let sendTpsData = this.state.sendTpsData!=null?this.state.sendTpsData:[];
            let recvTpsData = this.state.recvTpsData!=null?this.state.recvTpsData:[];
            let nettyCliSendSpeedData = this.state.nettyCliSendSpeedData!=null?this.state.nettyCliSendSpeedData:[];
            let nettySrvRecvSpeedData = this.state.nettySrvRecvSpeedData!=null?this.state.nettySrvRecvSpeedData:[];
            let fullGcData = this.state.fullGcData!=null?this.state.fullGcData:[];
            let memoryUsedData = this.state.memoryUsedData!=null?this.state.memoryUsedData:[];
            let heapMemoryData = this.state.heapMemoryData!=null?this.state.heapMemoryData:[];
            let cpuUsedRatioData = this.state.cpuUsedRatioData!=null?this.state.cpuUsedRatioData:[];

            if(rsdata != null && rsdata.length > 0){
                rsdata.forEach(function(item, index, array) {
                    let lastMetricValue = item.groupToTimeline.defaultGroup.lastMetric.metricValue;
                    if(item.title == 'SendTps'){
                        sendTpsData.push({ minute: newtime, sendTps: parseInt(lastMetricValue)}) ;
                    }
                    if(item.title == 'RecvTps'){
                        recvTpsData.push({ minute: newtime, recvTps: parseInt(lastMetricValue)}) ;
                    }
                    if(item.title == 'NettyCliSendSpeed'){
                        nettyCliSendSpeedData.push({ minute: newtime, nettyCliSendSpeed: parseInt(lastMetricValue)}) ;
                    }
                    if(item.title == 'NettySrvRecvSpeed'){
                        nettySrvRecvSpeedData.push({ minute: newtime, nettySrvRecvSpeed: parseInt(lastMetricValue)}) ;
                    }
                    if(item.title == 'FullGc'){
                        fullGcData.push({ minute: newtime, fullGc: parseInt(lastMetricValue)}) ;
                    }
                    if(item.title == 'MemoryUsed'){
                        memoryUsedData.push( { minute: newtime, memoryUsed: parseFloat(lastMetricValue)}) ;
                    }
                    if(item.title == 'HeapMemory'){
                        heapMemoryData.push({ minute: newtime, heapMemory: parseFloat(lastMetricValue)}) ;
                    }
                    if(item.title == 'CpuUsedRatio'){
                        cpuUsedRatioData.push({ minute: newtime, cpuUsedRatio: parseInt(lastMetricValue)}) ;
                    }
                });
            }else{
                sendTpsData.push({ minute: newtime, sendTps: null}) ;
                recvTpsData.push({ minute: newtime, recvTps: null}) ;
                nettyCliSendSpeedData.push({ minute: newtime, nettyCliSendSpeed: null}) ;
                nettySrvRecvSpeedData.push({ minute: newtime, nettySrvRecvSpeed: null}) ;
                fullGcData.push({ minute: newtime, fullGc: null}) ;
                memoryUsedData.push( { minute: newtime, memoryUsed: null}) ;
                heapMemoryData.push({ minute: newtime, heapMemory: null}) ;
                cpuUsedRatioData.push({ minute: newtime, cpuUsedRatio: null}) ;
            }

            this.setState({
                sendTpsData: sendTpsData,
                recvTpsData: recvTpsData,
                nettyCliSendSpeedData: nettyCliSendSpeedData,
                nettySrvRecvSpeedData: nettySrvRecvSpeedData,
                fullGcData: fullGcData,
                memoryUsedData: memoryUsedData,
                heapMemoryData: heapMemoryData,
                cpuUsedRatioData: cpuUsedRatioData,
            });
        });
    };

    resetTaskData = () => {
        if (this.timeTicket) {
            clearInterval(this.timeTicket);
        }
        this.setState({
            sendTpsData: null,
            recvTpsData: null,
            nettyCliSendSpeedData: null,
            nettySrvRecvSpeedData: null,
            fullGcData: null,
            memoryUsedData: null,
            heapMemoryData: null,
            cpuUsedRatioData: null,
            sendTpsData4Time: null,
            recvTpsData4Time: null,
            nettyCliSendSpeedData4Time: null,
            nettySrvRecvSpeedData4Time: null,
            fullGcData4Time: null,
            memoryUsedData4Time: null,
            heapMemoryData4Time: null,
            cpuUsedRatioData4Time: null,
        });
    }

    handleSearch = (e) => {
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.resetTaskData();
                this.fetchHistoryDate(params);
            }
        });
    }

    handleSearchRecent = (hourNumber,e) => {
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.fetchInitDate(params.taskId,hourNumber);
                let fields = ['rangeTimePicker','rangeTime'];
                this.props.form.resetFields(fields);

            }
        });
    }

    fetchHistoryDate = (params) => {

        this.setState({
            reportType :1,
        });
        let postData = {
            taskId:this.state.currentTaskId,
            rangeTime:params.rangeTime,
        };

        qs.form("/streamsuite/jstormMonitor/getReportDataByTime", postData).then((rsdata) => {

            if(rsdata != null && rsdata.length > 0){
                let sendTpsData = [];
                let recvTpsData = [];
                let nettyCliSendSpeedData = [];
                let nettySrvRecvSpeedData = [];
                let fullGcData = [];
                let memoryUsedData = [];
                let heapMemoryData = [];
                let cpuUsedRatioData = [];

                rsdata.forEach(function(item, index, array) {
                    let metricValues = item.groupToTimeline.defaultGroup.metricValues;
                    let reportTitle = item.title;
                    metricValues.forEach(function(inneritem, index, array) {

                        let metricValue = inneritem.metricValue;
                        if(inneritem.metricValue == '-1'){
                            metricValue = null;
                        }

                        if(reportTitle == 'SendTps'){
                            sendTpsData.push({ minute: inneritem.time , sendTps:  parseInt(metricValue)} );
                        }
                        if(reportTitle == 'RecvTps'){
                            recvTpsData.push({ minute: inneritem.time, recvTps: parseInt(metricValue)}) ;
                        }
                        if(reportTitle == 'NettyCliSendSpeed'){
                            nettyCliSendSpeedData.push({ minute: inneritem.time, nettyCliSendSpeed: parseInt(metricValue)}) ;
                        }
                        if(reportTitle == 'NettySrvRecvSpeed'){
                            nettySrvRecvSpeedData.push({ minute: inneritem.time, nettySrvRecvSpeed: parseInt(metricValue)}) ;
                        }
                        if(reportTitle == 'FullGc'){
                            fullGcData.push({ minute: inneritem.time, fullGc: parseInt(metricValue)}) ;
                        }
                        if(reportTitle == 'MemoryUsed'){
                            memoryUsedData.push( { minute: inneritem.time, memoryUsed: parseFloat(metricValue)}) ;
                        }
                        if(reportTitle == 'HeapMemory'){
                            heapMemoryData.push({ minute: inneritem.time, heapMemory: parseFloat(metricValue)}) ;
                        }
                        if(reportTitle == 'CpuUsedRatio'){
                            cpuUsedRatioData.push({ minute: inneritem.time, cpuUsedRatio: parseInt(metricValue)}) ;
                        }

                    });
                });

                this.setState({
                    sendTpsData4Time: sendTpsData,
                    recvTpsData4Time: recvTpsData,
                    nettyCliSendSpeedData4Time: nettyCliSendSpeedData,
                    nettySrvRecvSpeedData4Time: nettySrvRecvSpeedData,
                    fullGcData4Time: fullGcData,
                    memoryUsedData4Time: memoryUsedData,
                    heapMemoryData4Time: heapMemoryData,
                    cpuUsedRatioData4Time: cpuUsedRatioData,
                });
            }
        });

        qs.form("/streamsuite/jstormMonitor/getWorkerErrorData", postData).then((rsdata) => {

            let workerErrorData = [];
            if(rsdata != null && rsdata.length > 0){
                workerErrorData = rsdata;
            }
            this.setState({
                workerErrorData: workerErrorData,
            });
        });
    }

    handleTaskChange = (value) => {

        let fields = ['rangeTimePicker','rangeTime'];
        this.props.form.resetFields(fields);

        //设置各个报表数据
        this.setState({
            reportType :0,
            currentTaskId :value
        });

        //初始化查询最近一小时的数据方法。
        this.fetchInitDate(value,1);

        // 重新设置时间定时
        if (this.timeTicket) {
            clearInterval(this.timeTicket);
        }
        this.timeTicket = setInterval(this.fetchNewDate, this.state.timeTickInterval);
    }

    render() {
        const {getFieldDecorator} = this.props.form;
        const {taskDatas,searchStartTime,searchEndTime} = this.state;
        let selectedTaskId = (this.state.currentTaskId != null);
        let showDynamicReport = (selectedTaskId && this.state.reportType == 0);
        let showStaticReport = (selectedTaskId && this.state.reportType == 1);

        if(this.state.sendTpsChartObject!=null){
            this.state.sendTpsChartObject.source(this.state.sendTpsData);
        }
        if(this.state.recvTpsChartObject!=null){
            this.state.recvTpsChartObject.source(this.state.recvTpsData);
        }
        if(this.state.nettyCliSendSpeedChartObject!=null){
            this.state.nettyCliSendSpeedChartObject.source(this.state.nettyCliSendSpeedData);
        }
        if(this.state.nettySrvRecvSpeedChartObject!=null){
            this.state.nettySrvRecvSpeedChartObject.source(this.state.nettySrvRecvSpeedData);
        }
        if(this.state.fullGcChartObject!=null){
            this.state.fullGcChartObject.source(this.state.fullGcData);
        }
        if(this.state.memoryUsedChartObject!=null){
            this.state.memoryUsedChartObject.source(this.state.memoryUsedData);
        }
        if(this.state.heapMemoryChartObject!=null){
            this.state.heapMemoryChartObject.source(this.state.heapMemoryData);
        }
        if(this.state.cpuUsedRatioChartObject!=null){
            this.state.cpuUsedRatioChartObject.source(this.state.cpuUsedRatioData);
        }


        if(this.state.sendTpsChartObject4Time!=null){
            this.state.sendTpsChartObject4Time.source(this.state.sendTpsData4Time);
        }
        if(this.state.recvTpsChartObject4Time!=null){
            this.state.recvTpsChartObject4Time.source(this.state.recvTpsData4Time);
        }
        if(this.state.nettyCliSendSpeedChartObject4Time!=null){
            this.state.nettyCliSendSpeedChartObject4Time.source(this.state.nettyCliSendSpeedData4Time);
        }
        if(this.state.nettySrvRecvSpeedChartObject4Time!=null){
            this.state.nettySrvRecvSpeedChartObject4Time.source(this.state.nettySrvRecvSpeedData4Time);
        }
        if(this.state.fullGcChartObject4Time!=null){
            this.state.fullGcChartObject4Time.source(this.state.fullGcData4Time);
        }
        if(this.state.memoryUsedChartObject4Time!=null){
            this.state.memoryUsedChartObject4Time.source(this.state.memoryUsedData4Time);
        }
        if(this.state.heapMemoryChartObject4Time!=null){
            this.state.heapMemoryChartObject4Time.source(this.state.heapMemoryData4Time);
        }
        if(this.state.cpuUsedRatioChartObject4Time!=null){
            this.state.cpuUsedRatioChartObject4Time.source(this.state.cpuUsedRatioData4Time);
        }


        const TabPane = Tabs.TabPane;

        return (
            <div>
                <Form >
                    <Row gutter={24}>
                        <Col span={6}  >
                            <FormItem label="">
                                {getFieldDecorator('taskId', {rules: [{required: true, message: '请选择任务'}],})(
                                    <Select
                                        showSearch
                                        style={{ width: 400 }}
                                        placeholder="选择任务"
                                        optionFilterProp="children"
                                        onChange={this.handleTaskChange}
                                        filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                                    >
                                        {taskDatas.map(d => <Select.Option key={d.id}>{d.taskName}</Select.Option>)}
                                    </Select>
                                )}
                            </FormItem>
                        </Col>
                        <Col span={5}  >
                            <FormItem label="">
                                {getFieldDecorator('rangeTime',{
                                    initialValue: [moment(searchStartTime),moment(searchEndTime)],
                                    rules: [{required: true, message: '请选择时间范围'},
                                        (rule, value, callback) => {
                                            const errors = []
                                            if(value!=""){
                                                let selectDate = new String(value);
                                                let startTime = new Date(selectDate.split(",")[0]);
                                                let endTime = new Date(selectDate.split(",")[1])
                                                let length = (endTime.getTime() - startTime.getTime())/(1000 * 60);
                                                let currentTime = new Date().getTime();
                                                if (endTime.getTime() > currentTime || startTime.getTime() > currentTime) {
                                                    errors.push(new Error('不能选择一个未来的时间!', rule.field))
                                                    callback(errors)
                                                }
                                                if (length < 2) {
                                                    errors.push(new Error('选择的时间过短，必须大于2分钟!', rule.field))
                                                    callback(errors)
                                                }
                                                if (length > 180) {
                                                    errors.push(new Error('选择的时间段过长，必须小于3小时!', rule.field))
                                                    callback(errors)
                                                }
                                            }
                                            callback();
                                        }
                                    ],
                                })(
                                    <RangePicker
                                        showTime={{ format: 'HH:mm' }}
                                        format="YYYY-MM-DD HH:mm"
                                        placeholder={['Start Time', 'End Time']}
                                    />
                                )}
                            </FormItem>
                        </Col>
                        <Col span={3}  >
                            <Button type="primary" style={{marginLeft: 16}} htmlType="button" onClick={this.handleSearch}>查询历史</Button>
                        </Col>
                        <Col span={3}  >
                            <Button type="primary" style={{marginLeft: -87}} htmlType="button" onClick={this.handleSearchRecent.bind(this,1)}>最近一小时</Button>
                        </Col>
                    </Row>
                </Form>


                <div style={{'display':showDynamicReport?'inline':'none'}}>

                    <Row gutter={24}>
                        <Col span={8}>
                            <Card title="SendTps" bordered={false}>
                                <Chart data={this.state.sendTpsData} height={320}
                                       scale={{'sendTps': { min: 0}, 'minute': {type: 'time', mask: 'HH:mm', tickCount: this.state.timeTickCount }}}
                                       placeholder="no data"  padding={[20, 80, 30]}  forceFit
                                       onGetG2Instance={g2Chart => {
                                           this.state.sendTpsChartObject = g2Chart;
                                       }}>
                                    <Axis name="minute" line={{stroke: '#000'}}/>
                                    <Axis name="sendTps" label={{formatter: val => `${val} tuple/s`}} line={{stroke: '#000'}}/>
                                    <Tooltip crosshairs={{type : "y"}}/>
                                    <Geom type="line" position="minute*sendTps" size={2} />
                                    <Geom type='point' position="minute*sendTps" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                </Chart>
                            </Card>
                        </Col>
                        <Col span={8}>
                            <Card title="RecvTps" bordered={false}>
                                <Chart data={this.state.recvTpsData} height={320}
                                       scale={{'recvTps': { min: 0}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                       placeholder="no data"  padding={[20, 80, 30]}  forceFit
                                       onGetG2Instance={g2Chart => {
                                           this.state.recvTpsChartObject = g2Chart;
                                       }}>
                                    <Axis name="minute"  line={{stroke: '#000'}}/>
                                    <Axis name="recvTps" label={{formatter: val => `${val} tuple/s`}} line={{stroke: '#000'}}/>
                                    <Tooltip crosshairs={{type : "y"}}/>
                                    <Geom type="line" position="minute*recvTps" size={2}/>
                                    <Geom type='point' position="minute*recvTps" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                </Chart>
                            </Card>
                        </Col>
                        <Col span={8}>
                            <Card title="NettyCliSendSpeed" bordered={true}>
                                <Chart data={this.state.nettyCliSendSpeedData} height={320}
                                       scale={{'nettyCliSendSpeed': { min: 0}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                       placeholder="no data"  padding={[20, 80, 30]}  forceFit
                                       onGetG2Instance={g2Chart => {
                                           this.state.nettyCliSendSpeedChartObject = g2Chart;
                                       }}>
                                    <Axis name="minute"  line={{stroke: '#000'}}/>
                                    <Axis name="nettyCliSendSpeed" label={{formatter: val => `${val} bit/s`}} line={{stroke: '#000'}}/>
                                    <Tooltip crosshairs={{type : "y"}}/>
                                    <Geom type="line" position="minute*nettyCliSendSpeed" size={2} />
                                    <Geom type='point' position="minute*nettyCliSendSpeed" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                </Chart>
                            </Card>
                        </Col>
                    </Row>
                    <br></br>
                    <Row gutter={24}>
                        <Col span={8}>
                            <Card title="NettySrvSendSpeed" bordered={true}>
                                <Chart data={this.state.nettySrvRecvSpeedData} height={320}
                                       scale={{'nettySrvRecvSpeed': { min: 0}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                       placeholder="no data"  padding={[20, 80, 30]}  forceFit
                                       onGetG2Instance={g2Chart => {
                                           this.state.nettySrvRecvSpeedChartObject = g2Chart;
                                       }}>
                                    <Axis name="minute"  line={{stroke: '#000'}}/>
                                    <Axis name="nettySrvRecvSpeed" label={{formatter: val => `${val} bit/s`}} line={{stroke: '#000'}}/>
                                    <Tooltip crosshairs={{type : "y"}}/>
                                    <Geom type="line" position="minute*nettySrvRecvSpeed" size={2}/>
                                    <Geom type='point' position="minute*nettySrvRecvSpeed" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                </Chart>
                            </Card>
                        </Col>
                        <Col span={8}>
                            <Card title="FullGc" bordered={true}>
                                <Chart data={this.state.fullGcData} height={320}
                                       scale={{'fullGc': { min: 0, tickInterval: 1}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                       placeholder="no data"  padding={[20, 80, 30]} forceFit
                                       onGetG2Instance={g2Chart => {
                                           this.state.fullGcChartObject = g2Chart;
                                       }}>
                                    <Axis name="minute"  line={{stroke: '#000'}}/>
                                    <Axis name="fullGc" label={{formatter: val => `${val} 次`}} line={{stroke: '#000'}}/>
                                    <Tooltip crosshairs={{type : "y"}}/>
                                    <Geom type="area" position="minute*fullGc" color='#8FFFB2'  />
                                </Chart>
                            </Card>
                        </Col>
                        <Col span={8}>
                            <Card title="CpuUsedRatio" bordered={true}>
                                <Chart data={this.state.cpuUsedRatioData} height={320}
                                       scale={{'cpuUsedRatio': { min: 0 , tickInterval: 100}, 'minute': {type: 'time', mask: 'HH:mm', tickCount: this.state.timeTickCount }}}
                                       placeholder="no data"  padding={[20, 80, 30]} forceFit
                                       onGetG2Instance={g2Chart => {
                                           this.state.cpuUsedRatioChartObject = g2Chart;
                                       }}>
                                    <Axis name="minute"  line={{stroke: '#000'}}/>
                                    <Axis name="cpuUsedRatio" label={{formatter: val => `${val} %`}} line={{stroke: '#000'}}/>
                                    <Tooltip crosshairs={{type : "y"}}/>
                                    <Geom type="area" position="minute*cpuUsedRatio" color='#8FFFB2'/>
                                </Chart>
                            </Card>
                        </Col>
                    </Row>
                    <br></br>
                    <Row gutter={24}>
                        <Col span={8}>
                            <Card title="HeapMemory" bordered={true}>
                                <Chart data={this.state.heapMemoryData} height={320}
                                       scale={{'heapMemory': { type: 'linear', min: 0, tickInterval: 1}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                       placeholder="no data"  padding={[20, 80, 30]} forceFit
                                       onGetG2Instance={g2Chart => {
                                           this.state.heapMemoryChartObject = g2Chart;
                                       }}>
                                    <Axis name="minute" line={{stroke: '#000'}} />
                                    <Axis name="heapMemory" label={{formatter: val => `${val} G`}} line={{stroke: '#000'}}/>
                                    <Tooltip crosshairs={{type : "y"}}/>
                                    <Geom type="area" position="minute*heapMemory" />
                                    <Geom type="line" position="minute*heapMemory" size={2} />
                                </Chart>
                            </Card>
                        </Col>
                        <Col span={8}>
                            <Card title="MemoryUsed" bordered={true}>
                                <Chart data={this.state.memoryUsedData} height={320}
                                       scale={{'memoryUsed': { min: 0, tickInterval: 1 }, 'minute': {type: 'time', mask: 'HH:mm', tickCount: this.state.timeTickCount }}}
                                       placeholder="no data"  padding={[20, 80, 30]} forceFit
                                       onGetG2Instance={g2Chart => {
                                           this.state.memoryUsedChartObject = g2Chart;
                                       }}>
                                    <Axis name="minute" line={{stroke: '#000'}}/>
                                    <Axis name="memoryUsed" label={{formatter: val => `${val} G`}} line={{stroke: '#000'}}/>
                                    <Tooltip crosshairs={{type : "y"}}/>
                                    <Geom type="area" position="minute*memoryUsed" />
                                    <Geom type="line" position="minute*memoryUsed" size={2} />
                                </Chart>
                            </Card>
                        </Col>
                    </Row>
                </div>

                <div style={{'display':showStaticReport?'inline':'none'}}>

                    <Tabs defaultActiveKey="taskHistoryReport">
                        <TabPane tab="topology" key="taskHistoryReport">

                            <Row gutter={24}>
                                <Col span={8}>
                                    <Card title="SendTps" bordered={false}>
                                        <Chart data={this.state.sendTpsData4Time} height={320}
                                               scale={{'sendTps': { min: 0}, 'minute': {type: 'time', mask: 'HH:mm', tickCount: this.state.timeTickCount }}}
                                               placeholder="no data"  padding={[20, 80, 30]}  forceFit
                                               onGetG2Instance={g2Chart => {
                                                   this.state.sendTpsChartObject4Time = g2Chart;
                                               }}>
                                            <Axis name="minute" line={{stroke: '#000'}}/>
                                            <Axis name="sendTps" label={{formatter: val => `${val} tuple/s`}} line={{stroke: '#000'}}/>
                                            <Tooltip crosshairs={{type : "y"}}/>
                                            <Geom type="line" position="minute*sendTps" size={2} />
                                            <Geom type='point' position="minute*sendTps" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                        </Chart>
                                    </Card>
                                </Col>
                                <Col span={8}>
                                    <Card title="RecvTps" bordered={false}>
                                        <Chart data={this.state.recvTpsData4Time} height={320}
                                               scale={{'recvTps': { min: 0}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                               placeholder="no data"  padding={[20, 80, 30]}  forceFit
                                               onGetG2Instance={g2Chart => {
                                                   this.state.recvTpsChartObject4Time = g2Chart;
                                               }}>
                                            <Axis name="minute"  line={{stroke: '#000'}}/>
                                            <Axis name="recvTps" label={{formatter: val => `${val} tuple/s`}} line={{stroke: '#000'}}/>
                                            <Tooltip crosshairs={{type : "y"}}/>
                                            <Geom type="line" position="minute*recvTps" size={2}/>
                                            <Geom type='point' position="minute*recvTps" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                        </Chart>
                                    </Card>
                                </Col>
                                <Col span={8}>
                                    <Card title="NettyCliSendSpeed" bordered={true}>
                                        <Chart data={this.state.nettyCliSendSpeedData4Time} height={320}
                                               scale={{'nettyCliSendSpeed': { min: 0}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                               placeholder="no data"  padding={[20, 80, 30]}  forceFit
                                               onGetG2Instance={g2Chart => {
                                                   this.state.nettyCliSendSpeedChartObject4Time = g2Chart;
                                               }}>
                                            <Axis name="minute"  line={{stroke: '#000'}}/>
                                            <Axis name="nettyCliSendSpeed" label={{formatter: val => `${val} bit/s`}} line={{stroke: '#000'}}/>
                                            <Tooltip crosshairs={{type : "y"}}/>
                                            <Geom type="line" position="minute*nettyCliSendSpeed" size={2} />
                                            <Geom type='point' position="minute*nettyCliSendSpeed" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                        </Chart>
                                    </Card>
                                </Col>
                            </Row>
                            <br></br>
                            <Row gutter={24}>
                                <Col span={8}>
                                    <Card title="NettySrvSendSpeed" bordered={true}>
                                        <Chart data={this.state.nettySrvRecvSpeedData4Time} height={320}
                                               scale={{'nettySrvRecvSpeed': { min: 0}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                               placeholder="no data"  padding={[20, 80, 30]}  forceFit
                                               onGetG2Instance={g2Chart => {
                                                   this.state.nettySrvRecvSpeedChartObject4Time = g2Chart;
                                               }}>
                                            <Axis name="minute"  line={{stroke: '#000'}}/>
                                            <Axis name="nettySrvRecvSpeed" label={{formatter: val => `${val} bit/s`}} line={{stroke: '#000'}}/>
                                            <Tooltip crosshairs={{type : "y"}}/>
                                            <Geom type="line" position="minute*nettySrvRecvSpeed" size={2}/>
                                            <Geom type='point' position="minute*nettySrvRecvSpeed" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                        </Chart>
                                    </Card>
                                </Col>
                                <Col span={8}>
                                    <Card title="FullGc" bordered={true}>
                                        <Chart data={this.state.fullGcData4Time} height={320}
                                               scale={{'fullGc': { min: 0, tickInterval: 1}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                               placeholder="no data"  padding={[20, 80, 30]} forceFit
                                               onGetG2Instance={g2Chart => {
                                                   this.state.fullGcChartObject4Time = g2Chart;
                                               }}>
                                            <Axis name="minute"  line={{stroke: '#000'}}/>
                                            <Axis name="fullGc" label={{formatter: val => `${val} 次`}} line={{stroke: '#000'}}/>
                                            <Tooltip crosshairs={{type : "y"}}/>
                                            <Geom type="area" position="minute*fullGc" color='#8FFFB2'  />
                                        </Chart>
                                    </Card>
                                </Col>
                                <Col span={8}>
                                    <Card title="CpuUsedRatio" bordered={true}>
                                        <Chart data={this.state.cpuUsedRatioData4Time} height={320}
                                               scale={{'cpuUsedRatio': { min: 0 , tickInterval: 100}, 'minute': {type: 'time', mask: 'HH:mm', tickCount: this.state.timeTickCount }}}
                                               placeholder="no data"  padding={[20, 80, 30]} forceFit
                                               onGetG2Instance={g2Chart => {
                                                   this.state.cpuUsedRatioChartObject4Time = g2Chart;
                                               }}>
                                            <Axis name="minute"  line={{stroke: '#000'}}/>
                                            <Axis name="cpuUsedRatio" label={{formatter: val => `${val} %`}} line={{stroke: '#000'}}/>
                                            <Tooltip crosshairs={{type : "y"}}/>
                                            <Geom type="area" position="minute*cpuUsedRatio" color='#8FFFB2'/>
                                        </Chart>
                                    </Card>
                                </Col>
                            </Row>
                            <br></br>
                            <Row gutter={24}>
                                <Col span={8}>
                                    <Card title="HeapMemory" bordered={true}>
                                        <Chart data={this.state.heapMemoryData4Time} height={320}
                                               scale={{'heapMemory': { type: 'linear', min: 0, tickInterval: 1}, 'minute': {type: 'time', mask: 'HH:mm' , tickCount: this.state.timeTickCount }}}
                                               placeholder="no data"  padding={[20, 80, 30]} forceFit
                                               onGetG2Instance={g2Chart => {
                                                   this.state.heapMemoryChartObject4Time = g2Chart;
                                               }}>
                                            <Axis name="minute" line={{stroke: '#000'}} />
                                            <Axis name="heapMemory" label={{formatter: val => `${val} G`}} line={{stroke: '#000'}}/>
                                            <Tooltip crosshairs={{type : "y"}}/>
                                            <Geom type="area" position="minute*heapMemory" />
                                            <Geom type="line" position="minute*heapMemory" size={2} />
                                        </Chart>
                                    </Card>
                                </Col>
                                <Col span={8}>
                                    <Card title="MemoryUsed" bordered={true}>
                                        <Chart data={this.state.memoryUsedData4Time} height={320}
                                               scale={{'memoryUsed': { min: 0, tickInterval: 1 }, 'minute': {type: 'time', mask: 'HH:mm', tickCount: this.state.timeTickCount }}}
                                               placeholder="no data"  padding={[20, 80, 30]} forceFit
                                               onGetG2Instance={g2Chart => {
                                                   this.state.memoryUsedChartObject4Time = g2Chart;
                                               }}>
                                            <Axis name="minute" line={{stroke: '#000'}}/>
                                            <Axis name="memoryUsed" label={{formatter: val => `${val} G`}} line={{stroke: '#000'}}/>
                                            <Tooltip crosshairs={{type : "y"}}/>
                                            <Geom type="area" position="minute*memoryUsed" />
                                            <Geom type="line" position="minute*memoryUsed" size={2} />
                                        </Chart>
                                    </Card>
                                </Col>
                            </Row>

                        </TabPane>
                        <TabPane tab="worker error" key="workerErrorData">
                            <List
                                bordered
                                dataSource={this.state.workerErrorData}
                                size = 'small'
                                renderItem={item => (
                                    <List.Item>
                                        <div>{item}</div>
                                    </List.Item>
                                )}
                            />
                        </TabPane>
                    </Tabs>

                </div>
            </div>
        )
    }
}

export default Form.create()(JstormTaskMoniter);

