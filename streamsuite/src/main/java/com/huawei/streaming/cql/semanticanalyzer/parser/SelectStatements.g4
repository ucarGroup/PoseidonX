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
 *	 CQL中的select语句定义
 */

parser grammar SelectStatements;

insertStatement
	:	insertClause selectStatement
	;

insertUserOperatorStatement
	:	insertClause usingStatement
	;

usingStatement
	:	KW_USING KW_OPERATOR operatorName KW_FROM streamName distributeClause? parallelClause?
	;

distributeClause
	:	KW_DISTRIBUTE KW_BY columnName
	;

/*
    split语法的实现
    只允许在语句最后有一个并发
*/
multiInsertStatement
    :   fromClause
        multiInsert+
        parallelClause?
    ;

selectStatement
   :	selectClause
   		fromClause
   		whereClause?
   		groupByClause?
  		havingClause?
   		orderByClause?
   		limitClause? 
   		parallelClause?
   ;

multiInsert
    :   insertClause multiSelect
    ;

multiSelect
    :   selectClause
        whereClause?
        groupByClause?
        havingClause?
        orderByClause?
        limitClause?
    ;

parallelClause
   :	KW_PARALLEL constIntegerValue
   ;

insertClause
   :	KW_INSERT KW_INTO KW_STREAM? streamName
   ;

fromClause
    :	KW_FROM joinSource combineCondition?
    ;

/*
	a join b join c 应该先处理b join c 
	再用Join的结果和a做Join
*/
joinSource
    :	fromSource joinRigthBody*
    ;

joinRigthBody
	:	joinToken fromSource onCondition?
	;
 
onCondition
	:	KW_ON expression
	; 
 
combineCondition
	:	KW_COMBINE LPAREN expression (COMMA expression)+ RPAREN
	;

joinToken
    :	innerJoin
    |	leftJoin
    |	rightJoin
    |	fullJoin
    |	crossJoin
    |	naturalJoin       		
    ;

innerJoin
	:   KW_JOIN
	|	KW_INNER KW_JOIN
	;
	
leftJoin
	:	 KW_LEFT (KW_OUTER)? KW_JOIN
	;

rightJoin
	:	KW_RIGHT (KW_OUTER)? KW_JOIN
	;

fullJoin
	:	KW_FULL (KW_OUTER)? KW_JOIN
	;

crossJoin
	:	COMMA
	|   KW_CROSS KW_JOIN
	;		

naturalJoin
	:	KW_NATURAL KW_JOIN
	;

fromSource
    :	streamBody
    |	datasourceBody
    ;

streamBody
	:	streamSource filterBeforeWindow? windowSource? sourceAlias? unidirection?
	;

/**
	数据源引用的别名是可选的，
	也就是说可以使用dataSourceName.columnName,
	也可以使用sourceAlias.columnName
*/ 
datasourceBody
	:	KW_DATASOURCE dataSourceName datasourceArguments sourceAlias?
	;	
    
datasourceArguments
	:	LSQUARE 
		datasourceSchema
		COMMA
		datasourceQuery
		RSQUARE
	;    

/**
	数据源中使用到的schema
*/  
datasourceSchema
	:	KW_SCHEMA LPAREN columnNameTypeList RPAREN
	;
    
/**
	数据源查询语句，所有的CQL参数都在这个query中定义
*/    
datasourceQuery
	:	KW_QUERY LPAREN datasourceQueryArguments? RPAREN
	;

/**
	数据源查询参数，
*/    	
datasourceQueryArguments
	:	expression (COMMA expression)*
	;     
    
streamSource
    :	streamName
    |	subQuerySource
    ;

unidirection
	:	KW_UNIDIRECTION
	;

filterBeforeWindow
	:	LPAREN searchCondition RPAREN
	;

windowSource
	:	LSQUARE windowBody RSQUARE
	;

windowBody
	:	rangeWindow
	|	rowsWindow
	|	rangeToday
	;
	
rowsWindow
	:	KW_ROWS constIntegerValue windowProperties windowDeterminer
	;

rangeWindow
	:	KW_RANGE rangeBound windowProperties? windowDeterminer
	;

rangeToday
	:	KW_RANGE KW_TODAY expression windowDeterminer
	;	

rangeBound
	:	rangeTime
	|	rangeUnBound
	;

rangeUnBound
	:	KW_UNBOUNDED
	;

rangeTime
	:	rangeDay? rangeHour? rangeMinutes? rangeSeconds? rangeMilliSeconds?
	;

rangeDay
	:	constIntegerValue KW_DAY
	;
	
rangeHour
	:	constIntegerValue KW_HOUR
	;
	
rangeMinutes
	:	constIntegerValue KW_MINUTES
	;
		
rangeSeconds
	:	constIntegerValue KW_SECONDS
	;
		
rangeMilliSeconds
	:	constIntegerValue KW_MILLISECONDS
	;
			
windowProperties
	:	KW_SLIDE
	|	KW_BATCH
	|   KW_ACCUMBATCH
	;

windowDeterminer
	:	partitionbyDeterminer?
		sortbyDeterminer?
		triggerbyDeterminer?
		excludeNowDeterminer?
	;

partitionbyDeterminer
	:	KW_PARTITION KW_BY expression
	;

sortbyDeterminer
	:	KW_SORT KW_BY expression
	;
	
triggerbyDeterminer
	:	KW_TRIGGER KW_BY expression
	;	
	
excludeNowDeterminer
	:	KW_EXCLUDE KW_NOW
	;	
	
subQuerySource
    :	LPAREN selectStatement RPAREN
    ;

sourceAlias
	:	KW_AS? streamAlias
	;

groupByClause
    :	KW_GROUP KW_BY  groupByList
    ;

groupByList
    :   groupByExpression (COMMA groupByExpression )*
    ;

groupByExpression
    :	expression
    ;

havingClause
    :	KW_HAVING havingCondition
    ;

havingCondition
    :	expression
    ;

orderByClause
    :	KW_ORDER KW_BY columnNameOrderList
    ;

limitClause
	:	KW_LIMIT limitRow
	;

limitAll
	:	KW_ALL
	;

limitRow
	:	constIntegerValue
	;

distinct
	:	KW_DISTINCT
	;

selectClause
    :   KW_SELECT subSelectClause
    ;

subSelectClause
    :   distinct? selectList
    ;
    
selectList
    :	selectItem (COMMA selectItem)*
    ;

selectItem
    :	selectExpression 
    ;

selectAlias
	:	multialias
	|	singleAlias
	;

multialias
	:	KW_AS LPAREN columnALias (COMMA columnALias)* RPAREN
	;

singleAlias
	:	KW_AS? columnALias
	;

selectExpression
    :	expression selectAlias?
    |	streamAllColumns
    ;

columnNameOrderList
    :	columnNameOrder (COMMA columnNameOrder)* 
    ;

columnNameOrder
    :	expression columnOrder?
    ;

columnOrder
	:	KW_ASC
	|	KW_DESC
	;

whereClause
    :	KW_WHERE searchCondition
    ;

searchCondition
    :	expression
    ;

streamAllColumns
    :	(streamName DOT)? STAR
    ;    
