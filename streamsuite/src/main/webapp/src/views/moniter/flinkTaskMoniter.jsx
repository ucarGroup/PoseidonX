import React from 'react';
import { Form, Row, Col, Select, Card, Collapse , DatePicker,Button, Tabs} from 'antd';
import {auth, qs, util} from 'libs';
import moment from 'moment';
import { Chart, Geom, Axis, Tooltip} from 'bizcharts';
import '../../styles/glob.less'

const FormItem = Form.Item;
const Panel = Collapse.Panel;
const { RangePicker } = DatePicker;
const dateFormat = 'YYYY/MM/DD';

class FlinkTaskMoniter extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            currentTaskId:null,
            taskDatas:[],
            verticeDatas:[],
            reportType:0,
            timeTickInterval :10000,
        }
    }

    //组件加载时执行查询
    componentWillMount() {

        let postData = {
            engineType:1,
        };
        qs.form("/streamsuite/task/task/getTaskByUser", postData).then((data) => {
            this.setState({
                taskDatas:data
            });
        });
    }

    fetchInitData = (taskId,hourNumber) => {
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
        let searchVerticeDatas = [];

        qs.form("/streamsuite/flinkMonitor/getReportDataByTime", postData).then((rsdata) => {

            if(rsdata != null && rsdata.length > 0){

                let i = 0;
                rsdata.forEach(function(item, index, array) {
                    i++;

                    let lineReports = item.lineReports;
                    let verticeName = item.title;
                    let lineReportsContent = [];

                    lineReports.forEach(function(lineReport, index, array) {

                        let reportDatas = [];
                        let metricValues = lineReport.groupToTimeline.defaultGroup.metricValues;
                        let reportTitle = lineReport.title;
                        metricValues.forEach(function(inneritem, index, array) {
                            let metricValue = inneritem.metricValue;
                            if(metricValue == '-1'){
                                reportDatas.push({ minute: inneritem.time , data:  null} );
                            }else{
                                reportDatas.push({ minute: inneritem.time , data:  parseInt(metricValue)} );
                            }
                        });

                        lineReportsContent.push(
                            <Col span={8}>
                                <Card title={reportTitle} bordered={false}>
                                    <Chart data={reportDatas} height={320}
                                           scale={{'minute': {type: 'time', mask: 'HH:mm', tickCount:10}}}
                                           placeholder="no data"  padding={[20, 80, 30]}>
                                        <Axis name="minute" line={{stroke: '#000'}}/>
                                        <Axis name="data" line={{stroke: '#000'}}/>
                                        <Tooltip crosshairs={{type : "y"}}/>
                                        <Geom type="line" position="minute*data" size={2} />
                                        <Geom type='point' position="minute*data" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                    </Chart>
                                </Card>
                            </Col>);

                    });

                    searchVerticeDatas.push(<Panel header={verticeName} key={i}>{lineReportsContent}</Panel>);

                });
            }else{
                searchVerticeDatas.push(<Panel header={'数据暂无'} key={1}></Panel>);
            }

            this.setState({
                verticeDatas: searchVerticeDatas,
                searchStartTime:startTime,
                searchEndTime:newtime,
            });
        });

    }

    handleSearch = (e) => {
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.fetchHistoryDate(params);
            }
        });
    }

    handleSearchRecent = (hourNumber,e) => {
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.fetchInitData(params.taskId,hourNumber);
                let fields = ['rangeTimePicker','rangeTime'];
                this.props.form.resetFields(fields);
            }
        });
    }

    fetchHistoryDate = (params) => {
        this.setState({
            reportType: 1,
        });
        let postData = {
            taskId: this.state.currentTaskId,
            rangeTime: params.rangeTime,
        };

        let searchVerticeDatas = [];

        qs.form("/streamsuite/flinkMonitor/getReportDataByTime", postData).then((rsdata) => {
            if(rsdata != null && rsdata.length > 0){

                let i = 0;
                rsdata.forEach(function(item, index, array) {
                    i++;

                    let lineReports = item.lineReports;
                    let verticeName = item.title;
                    let lineReportsContent = [];

                    lineReports.forEach(function(lineReport, index, array) {

                        let reportDatas = [];
                        let metricValues = lineReport.groupToTimeline.defaultGroup.metricValues;
                        let reportTitle = lineReport.title;
                        metricValues.forEach(function(inneritem, index, array) {
                            let metricValue = inneritem.metricValue;
                            if(metricValue == '-1'){
                                reportDatas.push({ minute: inneritem.time , data:  null} );
                            }else{
                                reportDatas.push({ minute: inneritem.time , data:  parseInt(metricValue)} );
                            }
                        });

                        lineReportsContent.push(
                            <Col span={8}>
                                <Card title={reportTitle} bordered={false}>
                                <Chart data={reportDatas} height={320}
                                    scale={{'minute': {type: 'time', mask: 'HH:mm', tickCount:10}}}
                                    placeholder="no data"  padding={[20, 80, 30]}>
                                    <Axis name="minute" line={{stroke: '#000'}}/>
                                    <Axis name="data" line={{stroke: '#000'}}/>
                                    <Tooltip crosshairs={{type : "y"}}/>
                                    <Geom type="line" position="minute*data" size={2} />
                                    <Geom type='point' position="minute*data" size={2} shape={'circle'} style={{ stroke: '#fff', lineWidth: 1}} />
                                </Chart>
                             </Card>
                            </Col>);

                    });

                    searchVerticeDatas.push(<Panel header={verticeName} key={i}>{lineReportsContent}</Panel>);

                });
            }else{
                searchVerticeDatas.push(<Panel header={'数据暂无'} key={1}></Panel>);
            }

            this.setState({
                verticeDatas: searchVerticeDatas,
            });
        });
    }

    handleTaskChange = (value) => {
        let fields = ['rangeTimePicker','rangeTime'];
        this.props.form.resetFields(fields);

        this.setState({
            reportType :0,
            currentTaskId :value,
            verticeDatas:[],
        });

        this.fetchInitData(value,1);
    }

    render() {
        const {getFieldDecorator} = this.props.form;
        const {taskDatas,verticeDatas,searchStartTime,searchEndTime} = this.state;

        return (
            <div>
                <Form>
                    <Row gutter={24}>
                        <Col span={6}  >
                            <FormItem label="">
                                {getFieldDecorator('taskId', {
                                    rules: [{required: true, message: '请选择任务'}],
                                })(
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
                <div>
                    <Collapse accordion>
                        {verticeDatas}
                    </Collapse>
                </div>
            </div>
    )
    }
}

export default Form.create()(FlinkTaskMoniter);

