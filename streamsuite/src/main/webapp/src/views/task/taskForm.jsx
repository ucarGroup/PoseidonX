import React from 'react';
import PropTypes from 'prop-types';
import {Button, Form, Input,Select, InputNumber , Modal,RadioGroup,notification} from "antd";
import { qs, util, config, auth } from "libs";

const FormItem = Form.Item;
const { TextArea } = Input;

class TaskForm extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            loading: false,
            btnText: '保存',
            showModal: this.props.visible,
            archiveData:[],
            cqlData:[],
            archiveVersionData:[],
            engineVersionAmData:[],
            engineVersionJstormData:[],
            configZkInfo:"",
            containerMemMax:0,
            containerMemMin:0,
            taskName: "",
            taskId:"",
            remark: "",
            classPath: "",
            workerNum: "",
            workerMem:"",
            blotNum: "",
            spoutNum: "",
            archiveId:"",
            taskCqlId:"",
            archiveVersionId:"",
            jstormEngineVersionId:"",
            yarnAmEngineVersionId:"",
            customParams:"",
            slots: "",
            parallelism: "",
            isCQL:"",
            isJstorm:"",
            isFlink:"",
            isEditInit: true,
            currentCql:'',
        }
    }

    static propTypes = {
        visible: PropTypes.bool,
        onRefresh: PropTypes.func
    }

    static defaultProps = {
        visible: false,
        onRefresh: () => {
        }
    }

    componentWillReceiveProps(nextProps) {
        if ('visible' in nextProps && nextProps.visible) {

            if(this.state.isEditInit){

                qs.form("/streamsuite/task/archive/getArchiveByUser", null).then((data) => {
                    this.setState({
                        archiveData:data
                    });
                });

                let postData = {
                    engineVersionType:"JSTORM_AM"
                };
                qs.form("/streamsuite/engineVersion/queryByType", postData).then((data) => {
                    this.setState({
                        engineVersionAmData:data
                    });
                });

                postData = {
                    engineVersionType:"JSTORM"
                };
                qs.form("/streamsuite/engineVersion/queryByType", postData).then((data) => {
                    this.setState({
                        engineVersionJstormData:data
                    });
                });

                qs.form("/streamsuite/task/task/getConfigZkInfo", null).then((data) => {
                    this.setState({
                        configZkInfo:data
                    });
                });

                qs.form("/streamsuite/jstormEngine/getContainerMem", null).then((data) => {
                    let containerMem = [];
                    containerMem = data.split(",");
                    this.setState({
                        containerMemMax:parseInt(containerMem[0]),
                        containerMemMin:parseInt(containerMem[1]),
                    });
                });

                this.setState({
                    showModal: nextProps.visible,
                    isNew:nextProps.isNew,
                    taskName: nextProps.taskName,
                    taskId:nextProps.taskId,
                    taskType:nextProps.taskType,
                    engineType:nextProps.engineType,
                    remark: nextProps.remark,
                    archiveId: nextProps.archiveId,
                    archiveVersionId: nextProps.archiveVersionId,
                    classPath: nextProps.classPath,
                    workerNum: nextProps.workerNum,
                    workerMem: nextProps.workerMem,
                    blotNum: nextProps.blotNum,
                    spoutNum: nextProps.spoutNum,
                    jstormEngineVersionId: nextProps.jstormEngineVersionId,
                    yarnAmEngineVersionId: nextProps.yarnAmEngineVersionId,
                    isCQL: nextProps.isCQL,
                    taskCqlId: nextProps.taskCqlId,
                    customParams: nextProps.customParams,
                    slots: nextProps.slots,
                    parallelism: nextProps.parallelism,
                    currentCql:'',
                });

                if(nextProps.taskId != '-1' ){
                    this.handleEngineTypeChange(nextProps.engineType);
                    if(nextProps.isCQL == '0'){
                        this.handleArchiveChange(nextProps.archiveId);
                    }
                }

                this.setState({
                    isEditInit: false
                });
            }
        }
    }

    handleSubmit = (e) => {
        e.preventDefault();
        this.props.form.validateFields((err, params) => {
            if (!err) {
                this.postTask(params);
            }
        });
    }

    handleArchiveChange = (value) => {
        let postData = {
            archiveId:value
        };
        qs.form("/streamsuite/task/archive/getArchiveVersionByArchiveId", postData).then((data) => {
            this.setState({
                archiveVersionData:data
            });
            if(data.length == 0 || value!= this.state.archiveId){
                this.setState({
                    archiveVersionId:''
                });
            }
        });
    }

    handleEngineTypeChange = (value) => {
        let engineType = value;


        qs.form("/streamsuite/cql/getCqlByUser?cqlType="+engineType,null).then((data) => {
            this.setState({
                cqlData:data,
            });
        });

        if(engineType == '0'){
            this.setState({
                isJstorm:'1',
                isFlink:'0',
            });
        }
        if(engineType == '1'){
            this.setState({
                isJstorm:'0',
                isFlink:'1',
            });
        }
        if(engineType == '2'){
            this.setState({
                isJstorm:'0',
                isFlink:'0',
            });
        }
    }

    //提交信息
    postTask(params) {
        this.setState({
            loading: true,
            btnText: "保存中..."
        });

        let postData = {
            ...params,
            isCql:this.state.isCQL,
            id:this.props.taskId
        };

        let postUrl = "";
        if(this.props.taskId == '-1'){
            postUrl = "/streamsuite/task/task/save";
        }else{
            postUrl = "/streamsuite/task/task/update";
        }

        qs.form(postUrl, postData).then((data) => {
            this.setState({
                loading: false,
                btnText: "保存"
            });
            //保存失败
            if (!data.result) {
                notification.error({
                    message:  "任务信息保存失败",
                    description: data.errMsg
                });
            }
            //保存成功
            else {
                if(this.props.taskId == '-1'){
                    notification.info({
                        message: "任务信息保存成功",
                    });
                    this.setState({showModal: false,isEditInit: true});
                    this.props.onRefresh();
                    return;
                }else{
                    notification.info({
                        message: "任务信息修改成功",
                    });
                    this.props.onRefresh();
                    return;
                }
            }
        });
    }

    render() {
        const formItemLayout = {
            labelCol: {
                xs: { span: 24 },
                sm: { span: 8 },
            },
            wrapperCol: {
                xs: { span: 24 },
                sm: { span: 16 },
            },
        };

        const {getFieldDecorator} = this.props.form;

        const {archiveData, archiveVersionData,engineVersionJstormData,engineVersionAmData,cqlData} = this.state;

        const modalProps = {
            destroyOnClose:true,
            visible: this.state.showModal,
            title: this.props.taskFormTitle,
            footer: null,
            onCancel: (e) => {
                this.setState({showModal: false,isEditInit: true,isJstorm: '',isFlink: ''});
                this.props.onRefresh();
            }
        }

        let isCQL = (this.state.isCQL == '1');

        let isJstorm = (this.state.isJstorm == '1');
        let isFlink = (this.state.isFlink == '1');
        let isJstormOrFlink = (this.state.isJstorm == '1' || this.state.isFlink == '1');

        let isNeedInputMainClass = (isJstormOrFlink && !isCQL);

        let workerShowName = 'worker';
        if(this.state.isFlink == '1'){
            workerShowName = 'taskManager';
        }

        return (
           <Modal {...modalProps} width='40%'>
            <Form onSubmit={this.handleSubmit}>
                <FormItem {...formItemLayout}
                          label="任务名称："
                          extra="必须以字母开头，并且只能包含字母数字或下划线"
                          hasFeedback>
                    {getFieldDecorator('taskName', {
                        initialValue: this.state.taskName,
                        rules: [{required: true, message: '请输入任务名称!'},
                            (rule, value, callback) => {
                                const errors = []
                                if(value!=""){
                                    var regTaskName = /^[a-zA-Z][a-zA-Z0-9_]*$/;
                                    if (!regTaskName.test(value)) {
                                        errors.push(new Error('任务名称不合法!', rule.field))
                                        callback(errors)
                                    }
                                }
                                callback();
                            }
                        ],
                    })(
                        <Input readOnly={!this.state.isNew}/>
                    )}
                </FormItem>

                <FormItem {...formItemLayout}
                          label="引擎类型："
                          hasFeedback>
                    {getFieldDecorator('engineType', {
                        initialValue: this.state.engineType + '',
                        rules: [{required: true, message: '请选择引擎类型!'}],
                    })(
                        <Select
                            showSearch
                            placeholder="请选择引擎类型"
                            optionFilterProp="children"
                            onChange={this.handleEngineTypeChange}
                            disabled={!this.state.isNew}
                        >
                            <Select.Option value="0">Jstorm</Select.Option>
                            <Select.Option value="1">Flink</Select.Option>
                        </Select>
                    )}
                </FormItem>

                <div style={{'display':!isCQL?'inline':'none'}}>
                    <FormItem {...formItemLayout}
                              label="任务运行文件："
                              extra=""
                              hasFeedback>
                        {getFieldDecorator('archiveId', {
                            initialValue: this.state.archiveId + '',
                            rules: [{required: !isCQL?true:false, message: '请选择任务运行文件!'}],
                        })(
                            <Select
                                showSearch
                                placeholder="请选择任务运行文件"
                                optionFilterProp="children"
                                onChange={this.handleArchiveChange}
                                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}>
                                {archiveData.map(d => <Select.Option key={d.id}>{d.taskArchiveName}</Select.Option>)}

                            </Select>
                        )}
                    </FormItem>

                    <FormItem {...formItemLayout}
                              label="任务运行文件版本："
                              hasFeedback>
                        {getFieldDecorator('archiveVersionId', {
                            initialValue: this.state.archiveVersionId + '',
                            rules: [{required: !isCQL?true:false, message: '请选择任务运行文件版本!'}],
                        })(
                            <Select
                                showSearch
                                placeholder="请选择任务运行文件版本"
                                optionFilterProp="children"
                                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                            >
                                {archiveVersionData.map(d => <Select.Option key={d.id}>{d.taskArchiveVersionUrl}</Select.Option>)}

                            </Select>
                        )}
                    </FormItem>

                    <div style={{'display':isJstormOrFlink?'inline':'none'}}>
                        <FormItem {...formItemLayout}
                                  label="任务执行main类名："
                                  hasFeedback>
                            {getFieldDecorator('classPath', {
                                initialValue: this.state.classPath,
                                rules: [{required: isNeedInputMainClass ?true:false, message: '请填写任务执行main类名!'}],
                            })(
                                <Input/>
                            )}
                        </FormItem>
                    </div>

                </div>

                <div style={{'display':isCQL?'inline':'none'}}>
                    <FormItem {...formItemLayout}
                              label="CQL脚本："
                              hasFeedback>
                        {getFieldDecorator('taskCqlId', {
                            initialValue: this.state.taskCqlId + '',
                            rules: [{required: isCQL?true:false, message: '请选择CQL脚本!'}],
                        })(
                            <Select
                                showSearch
                                placeholder="请选择CQL脚本"
                                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}>
                                {cqlData.map(d => <Select.Option key={d.id} >{d.cqlName}&nbsp;[{d.cqlRemark}]</Select.Option>)}
                            </Select>
                        )}
                    </FormItem>

                </div>

                <div style={{'display':isJstormOrFlink?'inline':'none'}}>
                    <FormItem {...formItemLayout}
                              label={ workerShowName + "数："}
                              hasFeedback>
                        {getFieldDecorator('workerNum', {
                            initialValue: this.state.workerNum,
                            rules: [{required: isJstormOrFlink ?true:false, message: '请输入' + workerShowName + '数!'}],
                        })(
                            <InputNumber min={1} max={100} defaultValue={1} />
                        )}
                    </FormItem>

                    <FormItem {...formItemLayout}
                              label={ "每个" + workerShowName + "分配内存数（MB）："}
                              hasFeedback>
                        {getFieldDecorator('workerMem', {
                            initialValue: this.state.workerMem,
                            rules: [{required: isJstormOrFlink ?true:false, message: '请输入每个' + workerShowName + '分配内存数!'}],
                        })(
                            <InputNumber min={this.state.containerMemMin} max={isJstorm?this.state.containerMemMax-1024:this.state.containerMemMax} defaultValue={this.state.containerMemMin} />
                        )}
                    </FormItem>
                </div>

                <div style={{'display':isJstorm?'inline':'none'}}>

                    <FormItem {...formItemLayout}
                              label="Spout并行度："
                              extra="必须按照此格式录入: ExampleSpout:2"
                              hasFeedback>
                        {getFieldDecorator('spoutNum', {
                            initialValue: this.state.spoutNum ,
                        })(
                            <Input/>
                        )}
                    </FormItem>

                    <FormItem {...formItemLayout}
                              label="Blot并行度："
                              extra="必须按照此格式录入: ExampleFirstBolt:2;ExampleLastBolt:3"
                              hasFeedback>
                        {getFieldDecorator('blotNum', {
                            initialValue: this.state.blotNum ,
                        })(
                            <Input/>
                        )}
                    </FormItem>

                    <FormItem {...formItemLayout}
                              label="Jstorm版本包："
                              hasFeedback>
                        {getFieldDecorator('jstormEngineVersionId', {
                            initialValue: this.state.jstormEngineVersionId + '',
                            rules: [{required: isJstorm?true:false, message: '请选择Jstorm版本包!'}],
                        })(
                            <Select
                                showSearch
                                placeholder="请选择Jstorm版本包"
                                optionFilterProp="children"
                                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                            >
                                {engineVersionJstormData.map(d => <Select.Option key={d.id}>{d.versionRemark}</Select.Option>)}

                            </Select>
                        )}
                    </FormItem>

                    <FormItem {...formItemLayout}
                              label="Jstorm AM版本包："
                              hasFeedback>
                        {getFieldDecorator('yarnAmEngineVersionId', {
                            initialValue: this.state.yarnAmEngineVersionId + '',
                            rules: [{required: isJstorm?true:false, message: '请选择Jstorm AM版本包!'}],
                        })(
                            <Select
                                showSearch
                                placeholder="请选择Jstorm AM版本包"
                                optionFilterProp="children"
                                filterOption={(input, option) => option.props.children.toLowerCase().indexOf(input.toLowerCase()) >= 0}
                            >
                                {engineVersionAmData.map(d => <Select.Option key={d.id}>{d.versionRemark}</Select.Option>)}

                            </Select>
                        )}
                    </FormItem>

                    <FormItem {...formItemLayout}
                              label="Zk地址信息："
                              hasFeedback>
                        {getFieldDecorator('configZkInfo', {
                            initialValue: this.state.configZkInfo,
                            rules: [{required: isJstorm?true:false, message: 'Zk地址信息未进行配置，请联系管理员进行配置!'}],
                        })(
                            <Input readOnly="true"/>
                        )}
                    </FormItem>
                </div>

                <div style={{'display':isFlink?'inline':'none'}}>

                    <FormItem {...formItemLayout}
                              label= "Slot数："
                              hasFeedback>
                        {getFieldDecorator('slots', {
                            initialValue: this.state.slots,
                            rules: [{required: isFlink?true:false, message: '请填写Slot数!'}],
                        })(
                            <InputNumber min={1} max={100} defaultValue={1} />
                        )}
                    </FormItem>

                    <FormItem {...formItemLayout}
                              label= "并发度 parallelism："
                              hasFeedback>
                        {getFieldDecorator('parallelism', {
                            initialValue: this.state.parallelism + "",
                        })(
                            <InputNumber min={1} max={100}/>
                        )}
                    </FormItem>

                    <div style={{'display':!isCQL?'inline':'none'}}>
                        <FormItem {...formItemLayout}
                                  label="用户参数："
                                  extra="每行填写一个参数 例如 input /file.log"
                                  hasFeedback>
                            {getFieldDecorator('customParams', {
                                initialValue: this.state.customParams
                            })(
                                <TextArea rows={4} />
                            )}
                        </FormItem>
                    </div>

                </div>

                <FormItem {...formItemLayout}
                          label="备注："
                          hasFeedback>
                    {getFieldDecorator('remark', {
                        initialValue: this.state.remark,
                    })(
                        <Input placeholder="备注"/>
                    )}
                </FormItem>

                <Button type="primary"
                        htmlType="submit"
                        loading={this.state.loading}
                        className="login-form-button"
                        style={{width: '100%'}}>{this.state.btnText}
                </Button>

            </Form>
        </Modal>
        )
    }
}

export default Form.create()(TaskForm);