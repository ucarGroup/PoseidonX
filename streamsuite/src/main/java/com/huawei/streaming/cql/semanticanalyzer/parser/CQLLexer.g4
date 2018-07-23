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
 * SQL的词法规则
 */

lexer grammar CQLLexer;

@lexer::header {
package com.huawei.streaming.cql.semanticanalyzer.parser;
}

   
//key words
KW_CREATE						:	'CREATE';
KW_SHOW							:	'SHOW';
KW_EXPLAIN						:	'EXPLAIN';
KW_SET							:	'SET';
KW_GET							:	'GET';
KW_LOAD							:	'LOAD';
KW_EXPORT						:	'EXPORT';
KW_DROP							:	'DROP';
KW_ADD							:	'ADD';
KW_SELECT						:	'SELECT';
KW_COMMENT						:	'COMMENT';
KW_FORCE						:	'FORCE';
KW_SERDE					    :	'SERDE';
KW_WITH							:	'WITH';
KW_PROPERTIES					:	'PROPERTIES';
KW_SOURCE					    :	'SOURCE';
KW_INPUT						:	'INPUT';
KW_STREAM						:	'STREAM';
KW_OUTPUT						:	'OUTPUT';
KW_SINK					        :	'SINK';
KW_SUBMIT						:	'SUBMIT';
KW_APPLICATION					:	'APPLICATION';
KW_DISTINCT						:	'DISTINCT';
KW_AND							:	'AND';
KW_OR							:	'OR';
KW_BETWEEN						:	'BETWEEN';
KW_IN							:	'IN';
KW_LIKE							:	'LIKE';
KW_RLIKE						:	'RLIKE';
KW_REGEXP						:	'REGEXP';
KW_CASE							:	'CASE';
KW_WHEN							:	'WHEN';
KW_THEN							:	'THEN';
KW_ELSE							:	'ELSE';
KW_END							:	'END';
KW_CAST							:	'CAST';
KW_EXISTS						:	'EXISTS';
KW_IF							:	'IF';
KW_FALSE						:	'FALSE';
KW_AS							:	'AS';
KW_NULL							:	'NULL';
KW_IS							:	'IS';
KW_TRUE							:	'TRUE';
KW_ALL							:	'ALL';
KW_NOT							:	'NOT' | '!';
KW_ASC							:	'ASC';
KW_DESC							:	'DESC';
KW_SORT							:	'SORT';
KW_ORDER						:	'ORDER';
KW_GROUP						:	'GROUP';
KW_BY							:	'BY';
KW_HAVING						:	'HAVING';
KW_WHERE						:	'WHERE';
KW_FROM							:	'FROM';
KW_ON							:	'ON';
KW_JOIN							:	'JOIN';
KW_FULL							:	'FULL';
KW_PRESERVE						:	'PRESERVE';
KW_OUTER						:	'OUTER';
KW_CROSS						:	'CROSS';
KW_SEMI							:	'SEMI';
KW_LEFT							:	'LEFT';
KW_INNER						:	'INNER';
KW_NATURAL						:	'NATURAL';
KW_RIGHT						:	'RIGHT';
KW_INTO							:	'INTO';
KW_INSERT						:	'INSERT';
KW_OVERWRITE					:	'OVERWRITE';
KW_LIMIT						:	'LIMIT';
KW_UNION						:	'UNION';
KW_APPLICATIONS					:	'APPLICATIONS';
KW_WINDOWS						:	'WINDOWS';
KW_EXTENDED						:	'EXTENDED';
KW_FUNCTIONS					:	'FUNCTIONS';
KW_FILE							:	'FILE';
KW_INPATH						:	'INPATH';
KW_WINDOW						:	'WINDOW';
KW_JAR							:	'JAR';
KW_FUNCTION						:	'FUNCTION';
KW_COMBINE						:	'COMBINE';
KW_UNIDIRECTION					:	'UNIDIRECTION';
KW_PARALLEL						:	'PARALLEL';
KW_TRIGGER						:	'TRIGGER';
KW_PARTITION					:	'PARTITION';
KW_SLIDE						:	'SLIDE';
KW_BATCH						:	'BATCH';
KW_RANGE						:	'RANGE';
KW_ROWS							:	'ROWS';
KW_TODAY						:	'TODAY';
KW_UNBOUNDED					:	'UNBOUNDED';
KW_EXCLUDE						:	'EXCLUDE';
KW_NOW							:	'NOW';
KW_PREVIOUS						:	'PREVIOUS';
KW_DATASOURCE					:	'DATASOURCE';
KW_SCHEMA						:	'SCHEMA';
KW_QUERY						:	'QUERY';
KW_DEACTIVE						:	'DEACTIVE';
KW_ACTIVE						:	'ACTIVE';
KW_WORKER						:	'WORKER';
KW_REBALANCE					:	'REBALANCE';
KW_OPERATOR                     :   'OPERATOR';
KW_USING                        :   'USING';
KW_DISTRIBUTE                   :   'DISTRIBUTE';

KW_ACCUMBATCH					:   'ACCUMBATCH';


/*
	窗口时间单位
*/
KW_DAY							:	'DAY'|'DAYS';
KW_HOUR							:	'HOUR'|'HOURS';
KW_MINUTES						:	'MINUTE'|'MINUTES';
KW_SECONDS						:	'SECOND'|'SECONDS';
KW_MILLISECONDS					:	'MILLISECOND'|'MILLISECONDS';


//DataTypes
KW_BOOLEAN						:	'BOOLEAN';
KW_INT							:	'INT';
KW_LONG							:	'LONG';
KW_FLOAT						:	'FLOAT';
KW_DOUBLE						:	'DOUBLE';
KW_STRING						:	'STRING';
KW_TIMESTAMP					:	'TIMESTAMP';
KW_DATE							:	'DATE';
KW_TIME							:	'TIME';
KW_DECIMAL						:	'DECIMAL';


// generated as a part of Number rule
DOT								:	'.';
COLON							:	':' ;
COMMA							:	',' ;
SEMICOLON						:	';' ;
LPAREN							:	'(' ;
RPAREN							:	')' ;
LSQUARE							:	'[' ;
RSQUARE							:	']' ;
LCURLY							:	'{';
RCURLY							:	'}';
EQUAL 							:	'=' | '==';
EQUAL_NS 						:	'<=>';
NOTEQUAL 						:	'<>' | '!=';
LESSTHANOREQUALTO 				:	'<=';
LESSTHAN 						:	'<';
GREATERTHANOREQUALTO			:	'>=';
GREATERTHAN						:	'>';
DIVIDE							:	'/';
PLUS							:	'+';
MINUS 							:	'-';
CONCATENATION 					:	'||';
STAR 							:	'*';
MOD 							:	'%';
DIV 							:	'DIV';
TILDE 							:	'~';
BITWISEOR 						:	'|';
AMPERSAND 						:	'&';
BITWISEXOR 						:	'^';
QUESTION 						:	'?';
DOLLAR 							:	'$';

fragment
Letter
    :	'a'..'z' | 'A'..'Z'
    ;

fragment
HexDigit
    :	'a'..'f' | 'A'..'F'
    ;

fragment
Digit
    :	'0'..'9'
    ;

fragment
Exponent
    :	('e' | 'E') ( PLUS|MINUS )? (Digit)+
    ;

LongLiteral
    :	Number 'L'
    ;

FloatLiteral
    :	Number 'F'
    ;

DoubleLiteral
    :	Number 'D'
    ;

DecimalLiteral
    :	Number 'B' 'D'
    ;
/**
	Date、Time、TimeStamp类型比较特殊，不能使用常量，只能用udf函数的方式创建
	比如cast函数或者date函数
 */

StringLiteral
    :	( 
    		'\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    		| '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    	)+
    ;

CharSetLiteral
    :	StringLiteral
    |	'0' 'X' (HexDigit|Digit)+
    ;

IntegerNumber
	:	(Digit)+
	;

Number
    :	(Digit)+ ( DOT (Digit)* (Exponent)? | Exponent)?
    ;

Identifier
    :	(Letter | Digit) (Letter | Digit | '_')*
    ;

CharSetName
    :	'_' (Letter | Digit | '_' | '-' | '.' | ':' )+
    ;

//
// Whitespace and comments
//
WS  
	:	[ \t\r\n\u000C]+ -> skip
    ;

COMMENT
    :	'/*' .*? '*/' -> skip
    ;

LINE_COMMENT
    :	  '--' ~[\r\n]* -> skip
    ;
