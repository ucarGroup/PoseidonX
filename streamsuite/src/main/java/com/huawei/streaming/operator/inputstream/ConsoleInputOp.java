package com.huawei.streaming.operator.inputstream;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IInputStreamOperator;
import com.huawei.streaming.serde.StreamSerDe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ConsoleInputOp implements IInputStreamOperator {

	/**
	 * 
	 */

	private static final long serialVersionUID = -5281942032303171627L;

	private static List<Object[]> global_value = new ArrayList<Object[]>();
	
	private IEmitter emitter;
	
	private Thread thread;
	
	private TupleEventType schema;
	
	private long schemaLen;

	private StreamSerDe serde;

    private StreamingConfig config;

	@Override
	public void setConfig(StreamingConfig conf) throws StreamingException {
		// TODO Auto-generated method stub
		this.config = conf;
	}

	@Override
	public void initialize() throws StreamingException {
		//读取线程启动
		ReadThread rt = new ReadThread();
		thread =new Thread(rt);
		thread.start();
	}

	@Override
	public void destroy() throws StreamingException {
		
	}

	@Override
	public void execute() throws StreamingException {
		if(!global_value.isEmpty()){
			for(Object[] valueArr: global_value){
				emitter.emit(valueArr);
			}
			global_value.clear();
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
			}
		}
	}

	@Override
	public void setEmitter(IEmitter emitter) {
		// TODO Auto-generated method stub
		this.emitter = emitter;
	}

	@Override
	public void setSerDe(StreamSerDe serde) {
		// TODO Auto-generated method stub
		this.schema = serde.getSchema();
		schemaLen = schema.getAllAttributes().length;
		this.serde = serde;
	}

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamSerDe getSerDe()
    {
        return serde;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamingConfig getConfig()
    {
        return config;
    }

	private class ReadThread implements Runnable{

		@Override
		public void run() {
			// TODO Auto-generated method stub
			byte[] buffers = new byte[1024];
			int size = 0;
			try {
				while((size = System.in.read(buffers)) >= 0){
					String str = new String(buffers,0,size);
					String tmpArr[] = str.split("\\s+");
					if(schemaLen != tmpArr.length){
						System.out.println("input argument invalid :" + (schemaLen - tmpArr.length));
						continue;
					}
					global_value.clear();
					global_value.add(tmpArr);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
