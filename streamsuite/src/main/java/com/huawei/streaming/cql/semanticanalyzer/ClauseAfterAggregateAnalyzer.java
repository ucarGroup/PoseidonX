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

package com.huawei.streaming.cql.semanticanalyzer;

import java.util.ArrayList;
import java.util.List;

import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.HavingParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.AtomExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectItemContext;

/**
 * 
 * 聚合之后的子句语义分析
 * 
 */
public abstract class ClauseAfterAggregateAnalyzer extends BaseAnalyzer
{
    private List<Schema> inputSchemas;
    
    private Schema outputSchema;
    
    /**
     * select的表达式
     * 仅在进行having的解析的时候用到
     * 一般的解析并不会使用
     */
    private List<SelectItemContext> selectItems = null;
    
    /**
     * <默认构造函数>
     */
    public ClauseAfterAggregateAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void init(List<Schema> schemas)
        throws SemanticAnalyzerException
    {
        super.init(schemas);
        checkSchemas(schemas);
        setSchemaNameInAttributes(schemas);
    }
    
    /**
     * 遍历AST树，查找树种是否包含和select列表一模一样的树
     * 如果包含，则替换为outputSchema中的列
     */
    protected void replaceInputSchemas(ExpressionContext expressionContext)
        throws SemanticAnalyzerException
    {
        for (int i = 0; i < selectItems.size(); i++)
        {
            SelectItemContext selectItem = selectItems.get(i);
            Column c = outputSchema.getCols().get(i);
            AtomExpressionContext replacedExp = createReplacedExpression(c.getName());
            HavingParseContextReplacer replacer = new HavingParseContextReplacer(selectItem, replacedExp);
            expressionContext.walkChildAndReplace(replacer);
        }
    }
    
    private AtomExpressionContext createReplacedExpression(String columnName)
    {
        AtomExpressionContext atomExp = new AtomExpressionContext();
        atomExp.setColumnName(columnName);
        return atomExp;
    }
    
    /**
     * 设置select的表达式
     */
    public void addSelectItems(List<SelectItemContext> selects)
    {
        this.selectItems = selects;
        
        /*
         * 该方法一旦调用，就说明是having子句
         * schema属于混合schema，需要进行分离
         */
        outputSchema = getAllSchemas().get(0);
        inputSchemas = new ArrayList<Schema>();
        for (Schema schema : getAllSchemas())
        {
            inputSchemas.add(schema);
        }
    }
    
    public List<Schema> getInputSchemas()
    {
        return inputSchemas;
    }
    
    public Schema getOutputSchema()
    {
        return outputSchema;
    }
    
    public List<SelectItemContext> getSelectItems()
    {
        return selectItems;
    }
    
}
