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

package com.huawei.streaming.cql.semanticanalyzer.parser.visitor;

import org.antlr.v4.runtime.misc.NotNull;

import com.huawei.streaming.cql.semanticanalyzer.parser.CQLParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * ddl 语句语法遍历
 * 
 */
public class DDLStatementVisitor extends AbsCQLParserBaseVisitor<ParseContext>
{
    /**
     * <默认构造函数>
     */
    public DDLStatementVisitor()
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ParseContext defaultResult()
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitCreateInputStreamStatement(@NotNull CQLParser.CreateInputStreamStatementContext ctx)
    {
        CreateInputStreamVisitor visitor = new CreateInputStreamVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitCreateOutputStreamStatement(@NotNull CQLParser.CreateOutputStreamStatementContext ctx)
    {
        CreateOutputStreamVisitor visitor = new CreateOutputStreamVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitCreatePipeStreamStatement(@NotNull CQLParser.CreatePipeStreamStatementContext ctx)
    {
        CreatePipeStreamVisitor visitor = new CreatePipeStreamVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitCreateDataSourceStatement(@NotNull CQLParser.CreateDataSourceStatementContext ctx)
    {
        CreateDataSourceVisitor visitor = new CreateDataSourceVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitCreateOperatorStatement(@NotNull CQLParser.CreateOperatorStatementContext ctx)
    {
        CreateOperatorVisitor visitor = new CreateOperatorVisitor();
        return visitor.visit(ctx);      
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitDropApplication(@NotNull CQLParser.DropApplicationContext ctx)
    {
        DropApplicationVisitor visitor = new DropApplicationVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitSubmitApplication(@NotNull CQLParser.SubmitApplicationContext ctx)
    {
        SubmitApplicationVisitor visitor = new SubmitApplicationVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitShowApplications(@NotNull CQLParser.ShowApplicationsContext ctx)
    {
        ShowApplicationsVisitor visitor = new ShowApplicationsVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitShowFunctions(@NotNull CQLParser.ShowFunctionsContext ctx)
    {
        ShowFunctionsVisitor visitor = new ShowFunctionsVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitLoadStatement(@NotNull CQLParser.LoadStatementContext ctx)
    {
        LoadStatementVisitor visitor = new LoadStatementVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitExplainStatement(@NotNull CQLParser.ExplainStatementContext ctx)
    {
        ExplainStatementVisitor visitor = new ExplainStatementVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitGetStatement(@NotNull CQLParser.GetStatementContext ctx)
    {
        GetStatementVisitor visitor = new GetStatementVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitSetStatement(@NotNull CQLParser.SetStatementContext ctx)
    {
        SetStatementVisitor visitor = new SetStatementVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitCreateFunctionStatement(@NotNull CQLParser.CreateFunctionStatementContext ctx)
    {
        CreateFunctionStatementVisitor visitor = new CreateFunctionStatementVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitDropFunctionStatement(@NotNull CQLParser.DropFunctionStatementContext ctx)
    {
        DropFunctionStatementVisitor visitor = new DropFunctionStatementVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitAddFileStatement(@NotNull CQLParser.AddFileStatementContext ctx)
    {
        AddFileStatementVisitor visitor = new AddFileStatementVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitAddJARStatement(@NotNull CQLParser.AddJARStatementContext ctx)
    {
        AddJarStatementVisitor visitor = new AddJarStatementVisitor();
        return visitor.visit(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitDeactiveApplication(@NotNull CQLParser.DeactiveApplicationContext ctx)
    {
        DeactiveApplicationVisitor visitor = new DeactiveApplicationVisitor();
        return visitor.visit(ctx);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitActiveApplication(@NotNull CQLParser.ActiveApplicationContext ctx)
    {
        ActiveApplicationVisitor visitor = new ActiveApplicationVisitor();
        return visitor.visit(ctx);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ParseContext visitRebalanceApplication(@NotNull CQLParser.RebalanceApplicationContext ctx)
    {
        RebalanceApplicationVisitor visitor = new RebalanceApplicationVisitor();
        return visitor.visit(ctx);
    }
}
