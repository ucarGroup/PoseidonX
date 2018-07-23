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
 *	CQL中的一些关键性词法定义
 */
parser grammar Identifiers;

/**
 * 由于担心语法冲突或者其他导致别名失效等原因，暂时只修改函数名称相关
 * 即只修改函数名称中包含的这些关键字。
 * KW_UNIDIRECTION
 */
cqlIdentifier
	:	KW_DAY
	|	KW_HOUR
	|	KW_MINUTES
	|	KW_SECONDS
	|	KW_MILLISECONDS
	|	KW_BOOLEAN
	|	KW_INT
	|	KW_LONG
	|	KW_FLOAT
	|	KW_DOUBLE
	|	KW_STRING
	|	KW_TIMESTAMP
	|	KW_DATE
	|	KW_TIME
	|	KW_DECIMAL
	|	KW_OPERATOR
	|	Identifier
	;

streamProperties
    :	KW_PROPERTIES LPAREN streamPropertiesList RPAREN
    ;

confName
	:	StringLiteral
	;

/*
   配置属性的值只能是字符串类型，
   不支持其他类型
*/
confValue
	:	StringLiteral
	;

strValue
	:	StringLiteral
	;

/*
	位操作符
	正，负等操作符号
*/
unaryOperator
	:	PLUS 
	|	MINUS 
	|	TILDE
	;
	
functionName
	:	cqlIdentifier
	;

windowName
	:	cqlIdentifier
	;

className
	:   innerClassName
	|   userDefinedClassName
	;

innerClassName
    :   cqlIdentifier
    ;

userDefinedClassName
    :   StringLiteral
    ;

path
	:	StringLiteral
	;	
	
applicationName
    :	cqlIdentifier
    |	constIntegerValue
    ;	

columnName
	:	cqlIdentifier
	;
	
isForce
    :	KW_FORCE
    ;	
	
ifExists
    :	KW_IF KW_EXISTS
    ;

ifNotExists
    :	KW_IF KW_NOT KW_EXISTS
    ;
    
streamName
    :	cqlIdentifier
    ;	

dataSourceName
    :	cqlIdentifier
    ;	

streamAlias
	:	cqlIdentifier
	;

streamNameOrAlias
	:	cqlIdentifier
	;

columnALias
	:	cqlIdentifier
	;

constNull
	:	KW_NULL
	;

extended
	:	KW_EXTENDED
	;

identifierNot
	:	KW_NOT
	;
	
nullCondition
    :	identifierNot? KW_NULL
    ;

operatorName
	:	cqlIdentifier
	;
		
