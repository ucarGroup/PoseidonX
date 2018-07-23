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
表达式语法定义
CQL的各种运算符
算术运算符:+,-,*,/,%,DIV(取余)
关系运算符:>,<,>=,<=,=,!=,<>
逻辑运算符：And,Or,Not
位运算符：按位与(&)，按位或(|)，按位非(~)，按位异或(^)
连接运算符： '+'

CQL运算符优先级从高到低：
cast,case,when,
一元运算符(+,-,^) (正负)
算术运算符(*,/,DIV(取余))
算术运算符(+,-)
连接运算符（'||'）
位运算符(&,|,~,^)
关系运算符（=, !=, <, >, <=, >=,<>）
IS [NOT] NULL, LIKE, [NOT] BETWEEN,[NOT] IN, EXISTS,
not
and
or

说明：
1、可以使用括号改变优先级顺序,括号内的运算先执行
2、可以看出OR的优先级最低，算术运算符的优先级最高,原子类型的表达式和常量优先计算
3、乘除的优先级高于加减;
4、同一优先级运算符从左向右执行
*/

parser grammar Expressions;

expression
    :	logicExpressionOr
    ;

logicExpressionOr
    :	logicExpressionAnd (KW_OR logicExpressionAnd)*
    ;
    
logicExpressionAnd
    :	logicExpressionNot (KW_AND logicExpressionNot)*
    ;
    
logicExpressionNot
    :	identifierNot? equalRelationExpression
    ;
     
equalRelationExpression
    :	isNullLikeInExpressions
    |	expressionExists
    ; 

isNullLikeInExpressions
	:   binaryExpression
		(KW_IS nullCondition
		|	expressionLike
		|	expressionBetween
	    |	expressionIn
	    )?
	;
	
expressionExists
	:	KW_EXISTS subQueryExpression
	;	

subQueryExpression 
    :	LPAREN selectStatement RPAREN
 	;
	
expressionBetween
	:	identifierNot? KW_BETWEEN expressionBetweenMinValue KW_AND expressionBetweenMaxValue 
	;

binaryExpression
	:	bitExpression relationExpression*
	;

relationExpression
	:	relationOperator bitExpression
	;

relationOperator
    :	EQUAL 
    |	EQUAL_NS 
    |	NOTEQUAL 
    |	LESSTHANOREQUALTO
    |	LESSTHAN
    |	GREATERTHANOREQUALTO
    |	GREATERTHAN
    ;

expressionPrevious
	:	KW_PREVIOUS expressions
	;

expressionIn
	:	identifierNot? KW_IN expressions
	;

expressionLike
	:	identifierNot? precedenceEqualNegatableOperator bitExpression
	;
    
precedenceEqualNegatableOperator
    :	KW_LIKE 
    |	KW_RLIKE
    |	KW_REGEXP
    ;

expressionBetweenMinValue
	:	bitExpression
	;
	
expressionBetweenMaxValue	
	:	bitExpression
	;

bitExpression
    :	arithmeticPlusMinusExpression (bitOperator arithmeticPlusMinusExpression)*
    ;

bitOperator
	:	BITWISEOR
	|	AMPERSAND
	|	BITWISEXOR
	;
 
arithmeticPlusMinusExpression
    :	arithmeticStarExpression (arithmeticPlusOperator arithmeticStarExpression)*
    ;
    
arithmeticPlusOperator
    :	PLUS 
    |	MINUS
    |	CONCATENATION
    ;
    
arithmeticStarExpression
    :	fieldExpression (arithmeticStarOperator fieldExpression)*
    ;

fieldExpression
	:	 (streamNameOrAlias DOT)? atomExpression
	;
    
arithmeticStarOperator
    :	STAR 
    |	DIVIDE 
    |	MOD 
    |	DIV
    ;

atomExpression
    :	constNull
    |	constant
    |	expressionPrevious
    |	function
    |	castExpression
    |	caseExpression
    | 	whenExpression
    |	columnName
    | 	expressionWithLaparen
    ;
    
expressionWithLaparen
	:	LPAREN expression RPAREN
	;    
    
constant
    :	unaryOperator?
	    (
	    	constIntegerValue
	    |	constLongValue
	    |	constFloatValue
	    |	constDoubleValue
	    |	constBigDecimalValue
	    )
    |	constStingValue
    |	booleanValue
    ;

constStingValue
	:	StringLiteral
	;

constIntegerValue
	:	IntegerNumber
	;

constLongValue
	:	LongLiteral
	;

constFloatValue
	:	FloatLiteral
	;

constDoubleValue
	:	DoubleLiteral
	;

constBigDecimalValue
	:	DecimalLiteral
	;

function
    :	functionName LPAREN distinct? (selectExpression (COMMA selectExpression)*)?  RPAREN 
    ;

castExpression
    :	KW_CAST LPAREN expression KW_AS  primitiveType RPAREN
    ;

caseExpression
    :	KW_CASE caseHeadExpression (caseWhenBodyWhenBody caseWhenBodyThenBody)+ caseWhenElse? KW_END 
    ;
    
whenExpression
    :	KW_CASE	(caseWhenBodyWhenBody caseWhenBodyThenBody)+ caseWhenElse? KW_END
    ;

caseHeadExpression
	:	expression
	;

caseWhenBodyWhenBody
	:	KW_WHEN expression
	;

caseWhenBodyThenBody
	:	KW_THEN expression
	;

caseWhenElse
	:	KW_ELSE expression
	;

booleanValue
    :	KW_TRUE 
    |	KW_FALSE
    ;
    
expressions
    :	LPAREN expression (COMMA expression)* RPAREN
    ;
