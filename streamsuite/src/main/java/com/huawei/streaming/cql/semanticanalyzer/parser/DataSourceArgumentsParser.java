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

package com.huawei.streaming.cql.semanticanalyzer.parser;

import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.exception.ParseException;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.visitor.DatasourceBodyVisitor;

/**
 * 数据源参数解析
 * 
 */
public class DataSourceArgumentsParser implements IParser
{
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceArgumentsParser.class);
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext parse(String cql)
        throws ParseException
    {
        LOG.info("start to parse cql : {}", cql);
        CQLErrorListener errorListener = new CQLErrorListener();
        
        CQLLexer lexer = new CQLLexer(new ANTLRIgnoreCaseStringStream(cql));
        lexer.removeErrorListeners();
        lexer.addErrorListener(errorListener);
        
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        CQLParser parser = new CQLParser(tokens);
        
        CQLErrorStrategy errorHandler = new CQLErrorStrategy();
        parser.setErrorHandler(errorHandler);
        
        parser.removeErrorListeners();
        parser.addErrorListener(errorListener);
        
        ParserRuleContext tree = parser.datasourceQueryArguments();
        
        if (errorListener.getRecException() != null)
        {
            errorListener.getRecException().setCql(cql);
            throw errorListener.getRecException();
        }
        
        LOG.info("Parse Completed");
        DatasourceBodyVisitor visitor = new DatasourceBodyVisitor();
        return visitor.visit(tree);
    }
}
