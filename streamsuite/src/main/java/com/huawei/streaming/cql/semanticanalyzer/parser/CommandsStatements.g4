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
 * CQL中的命令定义
 */
parser grammar CommandsStatements;

addFileStatement
    :	KW_ADD KW_FILE path
    ;

addJARStatement
    :	KW_ADD KW_JAR path
    ;

createFunctionStatement
    :	KW_CREATE KW_FUNCTION functionName KW_AS userDefinedClassName streamProperties?
    ;
    
dropFunctionStatement
    :	KW_DROP KW_FUNCTION ifExists? functionName
    ;

getStatement
    :	KW_GET confName
    ;

setStatement
    :	KW_SET confName EQUAL confValue
    ;

loadStatement
    :	KW_LOAD KW_APPLICATION path
    ;

explainStatement
	:	KW_EXPLAIN KW_APPLICATION applicationName path?
	;

showFunctions
	:	KW_SHOW KW_FUNCTIONS extended? functionName
	;

showApplications
	:	KW_SHOW KW_APPLICATIONS applicationName?
	|   KW_SHOW KW_APPLICATIONS strValue?
	;

dropApplication
	:	KW_DROP KW_APPLICATION ifExists? applicationName
	;
	
submitApplication
	:	KW_SUBMIT KW_APPLICATION isForce? applicationName? path?
	;

deactiveApplication
	:   KW_DEACTIVE KW_APPLICATION applicationName
	;
	
activeApplication
	:   KW_ACTIVE KW_APPLICATION applicationName
	;
	
rebalanceApplication
    :   KW_REBALANCE KW_APPLICATION applicationName KW_SET KW_WORKER constIntegerValue
    ;
