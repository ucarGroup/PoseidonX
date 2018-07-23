package com.ucar.streamsuite.common.util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

/**
 * Description: 用于类相关的操作
 * Created on 2018/1/30 上午9:43
 *
 */
public class ClassUtil {


    /**
     * 通过反射赋值
     *
     * @param object  需要赋值的对象
     * @param attrMap 数据信息
     * @param <T>
     */
    public static <T> void assignAttrs(T object, Map<String, String> attrMap) {
        for (String key : attrMap.keySet()) {
            try {
                Field field = object.getClass().getDeclaredField(key);
                String originValue = attrMap.get(key);
                if (field != null && originValue != null) {
                    String fieldType = field.getGenericType().toString();
                    Object value = originValue;
                    if (fieldType.equals("class java.lang.Integer")) {
                        value = Integer.valueOf(originValue);
                    } else if (fieldType.equals("class java.lang.Long")) {
                        value = Long.valueOf(originValue);
                    }
                    String setMethodName = getSetMethodName(key);
                    Method method = object.getClass().getDeclaredMethod(setMethodName, field.getType());
                    if (method != null) {
                        method.invoke(object, value);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("通过反射赋值异常", e);
            }
        }
    }

    private static String getSetMethodName(String name) {
        name = name.substring(0, 1).toUpperCase() + name.substring(1);
        return "set" + name;
    }


}
