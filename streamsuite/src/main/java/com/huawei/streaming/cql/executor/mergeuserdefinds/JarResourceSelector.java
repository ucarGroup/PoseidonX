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

package com.huawei.streaming.cql.executor.mergeuserdefinds;

import org.apache.tools.ant.types.Resource;
import org.apache.tools.ant.types.resources.ZipResource;
import org.apache.tools.ant.types.resources.selectors.ResourceSelector;

/**
 * 解压缩jar包的时候，排除META-INF目录
 * 因为这个目录一般不放class，另外这个目录还有其他作用
 * 
 */
public class JarResourceSelector implements ResourceSelector
{
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSelected(Resource r)
    {
        if (!(r instanceof ZipResource))
        {
            return true;
        }
        
        ZipResource zip = (ZipResource)r;
        if (zip.getName().startsWith("META-INF"))
        {
            return false;
        }
        return true;
    }
    
}
