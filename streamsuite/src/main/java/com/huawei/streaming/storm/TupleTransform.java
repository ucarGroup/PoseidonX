package com.huawei.streaming.storm;

import java.util.List;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamSerDeException;

import backtype.storm.tuple.Tuple;

/**
 *
 * 进行Storm中Tuple数据类型到统一数据类型Event的转换
 * <功能详细描述>
 *
 */
public final class TupleTransform
{

    private IEventType iEventType;

    /**
     * <默认构造函数>
     *@param type 事件类型
     */
    public TupleTransform(IEventType type)
    {
        this.iEventType = type;
    }

    /**
     * 从Storm的Tuple事件转换为内部事件类型IEvent
     * <功能详细描述>
     */
    public IEvent getEvent(Tuple tuple)
        throws StreamSerDeException
    {
        List<Object> value = tuple.getValues();

        return new TupleEvent(tuple.getSourceStreamId(), iEventType, value.toArray(new Object[value.size()]));
    }

    /**
     * 根据内部事件获得数据值
     * <功能详细描述>
     */
    public Object[] getObjectList(IEvent event)
    {
        return event.getAllValues();
    }

    /**
     * 将tuple转换为内部使用的tupleEvent
     * TODO 该方法为静态方法，和使用对象进行转换的性能哪个好还有待进一步验证
     */
    public static TupleEvent tupeToEvent(Tuple input, IEventType inputTupleEventType)
    {
        List<Object> value = input.getValues();
        return new TupleEvent(input.getSourceStreamId(), inputTupleEventType, value.toArray(new Object[value.size()]));
    }

}
