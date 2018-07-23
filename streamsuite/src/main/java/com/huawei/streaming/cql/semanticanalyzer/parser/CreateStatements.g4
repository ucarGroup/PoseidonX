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

/*
 *	CQL中的输入流输出流定义
 */
parser grammar CreateStatements;

createInputStreamStatement
    :	KW_CREATE KW_INPUT KW_STREAM streamName
     	(LPAREN columnNameTypeList RPAREN)?
      	comment?
        serdeDefine?
        sourceDefine
        parallelClause?
    ;

createOutputStreamStatement
    :	KW_CREATE KW_OUTPUT KW_STREAM streamName
     	(LPAREN columnNameTypeList RPAREN)?
        comment?
        serdeDefine?
        sinkDefine
        parallelClause?
    ;	

createPipeStreamStatement
    :	KW_CREATE KW_STREAM streamName
      	(LPAREN columnNameTypeList RPAREN)?
      	comment?
    ;		

/**
	数据源定义语法
*/
createDataSourceStatement
	:	KW_CREATE KW_DATASOURCE dataSourceName
		KW_SOURCE	className
		datasourceProperties?
	;

/**
	自定义算子语法
*/
createOperatorStatement
	:	KW_CREATE KW_OPERATOR operatorName KW_AS className
		inputSchemaStatement outputSchemaStatement
		streamProperties?
	;

/**
	自定义算子的输入schema
*/
inputSchemaStatement
	:	KW_INPUT LPAREN columnNameTypeList RPAREN
	;

/**
	自定义算子的输出schema
*/
outputSchemaStatement
	:	KW_OUTPUT LPAREN columnNameTypeList RPAREN
	;

serdeDefine
    :   serdeClass
        serdeProperties?
    ;

sourceDefine
    :   sourceClause
        sourceProperties?
    ;

sinkDefine
    :   sinkClause
        sinkProperties?
    ;

primitiveType
    :	KW_INT
    |	KW_LONG
    |	KW_BOOLEAN
    |	KW_FLOAT 
    |	KW_DOUBLE
    |	KW_STRING
    |	KW_TIMESTAMP
    |	KW_DATE
    |	KW_TIME
    |	KW_DECIMAL	
    ;

colType
    :	primitiveType
    ;

columnNameType
    :	columnName colType comment? 
    ;

comment
	:	KW_COMMENT commentString
	;

commentString
	:	StringLiteral
	;

columnNameTypeList
    :	columnNameType (COMMA columnNameType)*
    ;

streamPropertiesList
    :	keyValueProperty (COMMA keyValueProperty)*
    ;

keyValueProperty
    :	confName EQUAL confValue
    ;
	
serdeClass
    :	KW_SERDE className
    ;

serdeProperties
    :	streamProperties
    ;
    
datasourceProperties
    :	streamProperties
    ;
	
sinkClause
    :	KW_SINK className
    ;   
     
sinkProperties
    :	streamProperties
    ;

sourceClause
    :	KW_SOURCE className
    ;    	
    
sourceProperties
    :	streamProperties
    ; 
