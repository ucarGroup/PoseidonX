/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.streaming.api;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 对某个类进行申明，表明它是Streamingconfig中的一个属性
 * 
 * 该申明只能放到属性上
 * 
 * 为了使用反射只能取到Retention为RUNTIME和CLASS的注解。
 * 
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ConfigAnnotation
{
    /**
     * 对应config中的属性键值
     * 
     * 当整个annotation只有一个属性的时候，推荐使用value，
     * 这样就可以使@ConfigAnnotation("xx")的 方式
     * 不然，就必须@ConfigAnnotation(name="xx")
     * 
     */
    String value();
}
