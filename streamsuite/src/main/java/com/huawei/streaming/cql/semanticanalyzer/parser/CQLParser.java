// Generated from CQLParser.g4 by ANTLR 4.1

package com.huawei.streaming.cql.semanticanalyzer.parser;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class CQLParser extends Parser {
	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		KW_WINDOWS=72, KW_SECONDS=107, KW_DATASOURCE=94, KW_WHERE=53, KW_ASC=46, 
		MINUS=138, RSQUARE=126, CONCATENATION=139, KW_GET=5, KW_INTO=66, SEMICOLON=122, 
		KW_NATURAL=64, KW_UNIDIRECTION=81, KW_OUTER=59, KW_FUNCTION=79, KW_EXPLAIN=3, 
		KW_SET=4, AMPERSAND=145, KW_BETWEEN=26, KW_REBALANCE=100, FloatLiteral=150, 
		KW_LONG=111, DOT=119, LPAREN=123, KW_SERDE=13, KW_SEMI=61, KW_FILE=75, 
		KW_STRING=114, KW_TRIGGER=83, KW_NULL=41, LINE_COMMENT=161, KW_EXISTS=37, 
		StringLiteral=153, KW_UNION=70, KW_SUBMIT=21, DOLLAR=148, KW_FALSE=39, 
		DoubleLiteral=151, KW_OPERATOR=101, KW_BY=51, KW_ORDER=49, KW_WORKER=99, 
		COMMENT=160, DIVIDE=136, KW_DECIMAL=118, LESSTHANOREQUALTO=132, KW_APPLICATIONS=71, 
		KW_PRESERVE=58, DecimalLiteral=152, KW_EXPORT=7, KW_DISTINCT=23, KW_JAR=78, 
		KW_THEN=33, KW_JOIN=56, BITWISEXOR=146, CharSetLiteral=154, RCURLY=128, 
		KW_CASE=31, COMMA=121, KW_WHEN=32, KW_PARTITION=84, KW_SOURCE=16, KW_ELSE=34, 
		LCURLY=127, KW_FORCE=12, KW_INPUT=17, DIV=142, KW_CAST=36, STAR=140, KW_MINUTES=106, 
		KW_MILLISECONDS=108, KW_RANGE=87, KW_PREVIOUS=93, KW_WINDOW=77, KW_TIMESTAMP=115, 
		KW_ROWS=88, KW_EXCLUDE=91, KW_DOUBLE=113, KW_TIME=117, KW_INT=110, KW_COMBINE=80, 
		KW_IN=27, KW_NOT=45, KW_RIGHT=65, KW_OUTPUT=19, KW_NOW=92, KW_HOUR=105, 
		Identifier=157, KW_IS=42, KW_BATCH=86, KW_AND=24, KW_END=35, KW_IF=38, 
		KW_TODAY=89, EQUAL_NS=130, GREATERTHANOREQUALTO=134, KW_LOAD=6, KW_DROP=8, 
		KW_SCHEMA=95, KW_CROSS=60, LongLiteral=149, KW_PARALLEL=82, KW_COMMENT=11, 
		KW_LIMIT=69, RPAREN=124, KW_AS=40, KW_GROUP=50, KW_RLIKE=29, NOTEQUAL=131, 
		KW_PROPERTIES=15, BITWISEOR=144, KW_APPLICATION=22, KW_SHOW=2, KW_SELECT=10, 
		KW_LEFT=62, PLUS=137, KW_DEACTIVE=97, KW_HAVING=52, KW_FULL=57, KW_DESC=47, 
		KW_BOOLEAN=109, KW_TRUE=43, KW_SINK=20, KW_LIKE=28, QUESTION=147, KW_DISTRIBUTE=103, 
		KW_CREATE=1, KW_EXTENDED=73, KW_FUNCTIONS=74, Number=156, KW_STREAM=18, 
		KW_DAY=104, KW_FROM=54, KW_SORT=48, KW_ON=55, CharSetName=158, WS=159, 
		KW_ADD=9, KW_OR=25, KW_ALL=44, KW_INNER=63, KW_REGEXP=30, KW_DATE=116, 
		MOD=141, EQUAL=129, KW_UNBOUNDED=90, KW_INSERT=67, COLON=120, KW_OVERWRITE=68, 
		LSQUARE=125, KW_WITH=14, KW_USING=102, IntegerNumber=155, KW_INPATH=76, 
		LESSTHAN=133, KW_SLIDE=85, KW_FLOAT=112, KW_QUERY=96, TILDE=143, KW_ACTIVE=98, 
		GREATERTHAN=135;

    public static final int KW_ACCUMBATCH = 104;

	public static final String[] tokenNames = {
		"<INVALID>", "'CREATE'", "'SHOW'", "'EXPLAIN'", "'SET'", "'GET'", "'LOAD'", 
		"'EXPORT'", "'DROP'", "'ADD'", "'SELECT'", "'COMMENT'", "'FORCE'", "'SERDE'", 
		"'WITH'", "'PROPERTIES'", "'SOURCE'", "'INPUT'", "'STREAM'", "'OUTPUT'", 
		"'SINK'", "'SUBMIT'", "'APPLICATION'", "'DISTINCT'", "'AND'", "'OR'", 
		"'BETWEEN'", "'IN'", "'LIKE'", "'RLIKE'", "'REGEXP'", "'CASE'", "'WHEN'", 
		"'THEN'", "'ELSE'", "'END'", "'CAST'", "'EXISTS'", "'IF'", "'FALSE'", 
		"'AS'", "'NULL'", "'IS'", "'TRUE'", "'ALL'", "KW_NOT", "'ASC'", "'DESC'", 
		"'SORT'", "'ORDER'", "'GROUP'", "'BY'", "'HAVING'", "'WHERE'", "'FROM'", 
		"'ON'", "'JOIN'", "'FULL'", "'PRESERVE'", "'OUTER'", "'CROSS'", "'SEMI'", 
		"'LEFT'", "'INNER'", "'NATURAL'", "'RIGHT'", "'INTO'", "'INSERT'", "'OVERWRITE'", 
		"'LIMIT'", "'UNION'", "'APPLICATIONS'", "'WINDOWS'", "'EXTENDED'", "'FUNCTIONS'", 
		"'FILE'", "'INPATH'", "'WINDOW'", "'JAR'", "'FUNCTION'", "'COMBINE'", 
		"'UNIDIRECTION'", "'PARALLEL'", "'TRIGGER'", "'PARTITION'", "'SLIDE'", 
		"'BATCH'", "'RANGE'", "'ROWS'", "'TODAY'", "'UNBOUNDED'", "'EXCLUDE'", 
		"'NOW'", "'PREVIOUS'", "'DATASOURCE'", "'SCHEMA'", "'QUERY'", "'DEACTIVE'", 
		"'ACTIVE'", "'WORKER'", "'REBALANCE'", "'OPERATOR'", "'USING'", "'DISTRIBUTE'", 
		"KW_DAY", "KW_HOUR", "KW_MINUTES", "KW_SECONDS", "KW_MILLISECONDS", "'BOOLEAN'", 
		"'INT'", "'LONG'", "'FLOAT'", "'DOUBLE'", "'STRING'", "'TIMESTAMP'", "'DATE'", 
		"'TIME'", "'DECIMAL'", "'.'", "':'", "','", "';'", "'('", "')'", "'['", 
		"']'", "'{'", "'}'", "EQUAL", "'<=>'", "NOTEQUAL", "'<='", "'<'", "'>='", 
		"'>'", "'/'", "'+'", "'-'", "'||'", "'*'", "'%'", "'DIV'", "'~'", "'|'", 
		"'&'", "'^'", "'?'", "'$'", "LongLiteral", "FloatLiteral", "DoubleLiteral", 
		"DecimalLiteral", "StringLiteral", "CharSetLiteral", "IntegerNumber", 
		"Number", "Identifier", "CharSetName", "WS", "COMMENT", "LINE_COMMENT",

         "ACCUMBATCH"
	};
	public static final int
		RULE_statement = 0, RULE_execStatement = 1, RULE_ddlStatement = 2, RULE_createInputStreamStatement = 3, 
		RULE_createOutputStreamStatement = 4, RULE_createPipeStreamStatement = 5, 
		RULE_createDataSourceStatement = 6, RULE_createOperatorStatement = 7, 
		RULE_inputSchemaStatement = 8, RULE_outputSchemaStatement = 9, RULE_serdeDefine = 10, 
		RULE_sourceDefine = 11, RULE_sinkDefine = 12, RULE_primitiveType = 13, 
		RULE_colType = 14, RULE_columnNameType = 15, RULE_comment = 16, RULE_commentString = 17, 
		RULE_columnNameTypeList = 18, RULE_streamPropertiesList = 19, RULE_keyValueProperty = 20, 
		RULE_serdeClass = 21, RULE_serdeProperties = 22, RULE_datasourceProperties = 23, 
		RULE_sinkClause = 24, RULE_sinkProperties = 25, RULE_sourceClause = 26, 
		RULE_sourceProperties = 27, RULE_addFileStatement = 28, RULE_addJARStatement = 29, 
		RULE_createFunctionStatement = 30, RULE_dropFunctionStatement = 31, RULE_getStatement = 32, 
		RULE_setStatement = 33, RULE_loadStatement = 34, RULE_explainStatement = 35, 
		RULE_showFunctions = 36, RULE_showApplications = 37, RULE_dropApplication = 38, 
		RULE_submitApplication = 39, RULE_deactiveApplication = 40, RULE_activeApplication = 41, 
		RULE_rebalanceApplication = 42, RULE_expression = 43, RULE_logicExpressionOr = 44, 
		RULE_logicExpressionAnd = 45, RULE_logicExpressionNot = 46, RULE_equalRelationExpression = 47, 
		RULE_isNullLikeInExpressions = 48, RULE_expressionExists = 49, RULE_subQueryExpression = 50, 
		RULE_expressionBetween = 51, RULE_binaryExpression = 52, RULE_relationExpression = 53, 
		RULE_relationOperator = 54, RULE_expressionPrevious = 55, RULE_expressionIn = 56, 
		RULE_expressionLike = 57, RULE_precedenceEqualNegatableOperator = 58, 
		RULE_expressionBetweenMinValue = 59, RULE_expressionBetweenMaxValue = 60, 
		RULE_bitExpression = 61, RULE_bitOperator = 62, RULE_arithmeticPlusMinusExpression = 63, 
		RULE_arithmeticPlusOperator = 64, RULE_arithmeticStarExpression = 65, 
		RULE_fieldExpression = 66, RULE_arithmeticStarOperator = 67, RULE_atomExpression = 68, 
		RULE_expressionWithLaparen = 69, RULE_constant = 70, RULE_constStingValue = 71, 
		RULE_constIntegerValue = 72, RULE_constLongValue = 73, RULE_constFloatValue = 74, 
		RULE_constDoubleValue = 75, RULE_constBigDecimalValue = 76, RULE_function = 77, 
		RULE_castExpression = 78, RULE_caseExpression = 79, RULE_whenExpression = 80, 
		RULE_caseHeadExpression = 81, RULE_caseWhenBodyWhenBody = 82, RULE_caseWhenBodyThenBody = 83, 
		RULE_caseWhenElse = 84, RULE_booleanValue = 85, RULE_expressions = 86, 
		RULE_insertStatement = 87, RULE_insertUserOperatorStatement = 88, RULE_usingStatement = 89, 
		RULE_distributeClause = 90, RULE_multiInsertStatement = 91, RULE_selectStatement = 92, 
		RULE_multiInsert = 93, RULE_multiSelect = 94, RULE_parallelClause = 95, 
		RULE_insertClause = 96, RULE_fromClause = 97, RULE_joinSource = 98, RULE_joinRigthBody = 99, 
		RULE_onCondition = 100, RULE_combineCondition = 101, RULE_joinToken = 102, 
		RULE_innerJoin = 103, RULE_leftJoin = 104, RULE_rightJoin = 105, RULE_fullJoin = 106, 
		RULE_crossJoin = 107, RULE_naturalJoin = 108, RULE_fromSource = 109, RULE_streamBody = 110, 
		RULE_datasourceBody = 111, RULE_datasourceArguments = 112, RULE_datasourceSchema = 113, 
		RULE_datasourceQuery = 114, RULE_datasourceQueryArguments = 115, RULE_streamSource = 116, 
		RULE_unidirection = 117, RULE_filterBeforeWindow = 118, RULE_windowSource = 119, 
		RULE_windowBody = 120, RULE_rowsWindow = 121, RULE_rangeWindow = 122, 
		RULE_rangeToday = 123, RULE_rangeBound = 124, RULE_rangeUnBound = 125, 
		RULE_rangeTime = 126, RULE_rangeDay = 127, RULE_rangeHour = 128, RULE_rangeMinutes = 129, 
		RULE_rangeSeconds = 130, RULE_rangeMilliSeconds = 131, RULE_windowProperties = 132, 
		RULE_windowDeterminer = 133, RULE_partitionbyDeterminer = 134, RULE_sortbyDeterminer = 135, 
		RULE_triggerbyDeterminer = 136, RULE_excludeNowDeterminer = 137, RULE_subQuerySource = 138, 
		RULE_sourceAlias = 139, RULE_groupByClause = 140, RULE_groupByList = 141, 
		RULE_groupByExpression = 142, RULE_havingClause = 143, RULE_havingCondition = 144, 
		RULE_orderByClause = 145, RULE_limitClause = 146, RULE_limitAll = 147, 
		RULE_limitRow = 148, RULE_distinct = 149, RULE_selectClause = 150, RULE_subSelectClause = 151, 
		RULE_selectList = 152, RULE_selectItem = 153, RULE_selectAlias = 154, 
		RULE_multialias = 155, RULE_singleAlias = 156, RULE_selectExpression = 157, 
		RULE_columnNameOrderList = 158, RULE_columnNameOrder = 159, RULE_columnOrder = 160, 
		RULE_whereClause = 161, RULE_searchCondition = 162, RULE_streamAllColumns = 163, 
		RULE_cqlIdentifier = 164, RULE_streamProperties = 165, RULE_confName = 166, 
		RULE_confValue = 167, RULE_strValue = 168, RULE_unaryOperator = 169, RULE_functionName = 170, 
		RULE_windowName = 171, RULE_className = 172, RULE_innerClassName = 173, 
		RULE_userDefinedClassName = 174, RULE_path = 175, RULE_applicationName = 176, 
		RULE_columnName = 177, RULE_isForce = 178, RULE_ifExists = 179, RULE_ifNotExists = 180, 
		RULE_streamName = 181, RULE_dataSourceName = 182, RULE_streamAlias = 183, 
		RULE_streamNameOrAlias = 184, RULE_columnALias = 185, RULE_constNull = 186, 
		RULE_extended = 187, RULE_identifierNot = 188, RULE_nullCondition = 189, 
		RULE_operatorName = 190;
	public static final String[] ruleNames = {
		"statement", "execStatement", "ddlStatement", "createInputStreamStatement", 
		"createOutputStreamStatement", "createPipeStreamStatement", "createDataSourceStatement", 
		"createOperatorStatement", "inputSchemaStatement", "outputSchemaStatement", 
		"serdeDefine", "sourceDefine", "sinkDefine", "primitiveType", "colType", 
		"columnNameType", "comment", "commentString", "columnNameTypeList", "streamPropertiesList", 
		"keyValueProperty", "serdeClass", "serdeProperties", "datasourceProperties", 
		"sinkClause", "sinkProperties", "sourceClause", "sourceProperties", "addFileStatement", 
		"addJARStatement", "createFunctionStatement", "dropFunctionStatement", 
		"getStatement", "setStatement", "loadStatement", "explainStatement", "showFunctions", 
		"showApplications", "dropApplication", "submitApplication", "deactiveApplication", 
		"activeApplication", "rebalanceApplication", "expression", "logicExpressionOr", 
		"logicExpressionAnd", "logicExpressionNot", "equalRelationExpression", 
		"isNullLikeInExpressions", "expressionExists", "subQueryExpression", "expressionBetween", 
		"binaryExpression", "relationExpression", "relationOperator", "expressionPrevious", 
		"expressionIn", "expressionLike", "precedenceEqualNegatableOperator", 
		"expressionBetweenMinValue", "expressionBetweenMaxValue", "bitExpression", 
		"bitOperator", "arithmeticPlusMinusExpression", "arithmeticPlusOperator", 
		"arithmeticStarExpression", "fieldExpression", "arithmeticStarOperator", 
		"atomExpression", "expressionWithLaparen", "constant", "constStingValue", 
		"constIntegerValue", "constLongValue", "constFloatValue", "constDoubleValue", 
		"constBigDecimalValue", "function", "castExpression", "caseExpression", 
		"whenExpression", "caseHeadExpression", "caseWhenBodyWhenBody", "caseWhenBodyThenBody", 
		"caseWhenElse", "booleanValue", "expressions", "insertStatement", "insertUserOperatorStatement", 
		"usingStatement", "distributeClause", "multiInsertStatement", "selectStatement", 
		"multiInsert", "multiSelect", "parallelClause", "insertClause", "fromClause", 
		"joinSource", "joinRigthBody", "onCondition", "combineCondition", "joinToken", 
		"innerJoin", "leftJoin", "rightJoin", "fullJoin", "crossJoin", "naturalJoin", 
		"fromSource", "streamBody", "datasourceBody", "datasourceArguments", "datasourceSchema", 
		"datasourceQuery", "datasourceQueryArguments", "streamSource", "unidirection", 
		"filterBeforeWindow", "windowSource", "windowBody", "rowsWindow", "rangeWindow", 
		"rangeToday", "rangeBound", "rangeUnBound", "rangeTime", "rangeDay", "rangeHour", 
		"rangeMinutes", "rangeSeconds", "rangeMilliSeconds", "windowProperties", 
		"windowDeterminer", "partitionbyDeterminer", "sortbyDeterminer", "triggerbyDeterminer", 
		"excludeNowDeterminer", "subQuerySource", "sourceAlias", "groupByClause", 
		"groupByList", "groupByExpression", "havingClause", "havingCondition", 
		"orderByClause", "limitClause", "limitAll", "limitRow", "distinct", "selectClause", 
		"subSelectClause", "selectList", "selectItem", "selectAlias", "multialias", 
		"singleAlias", "selectExpression", "columnNameOrderList", "columnNameOrder", 
		"columnOrder", "whereClause", "searchCondition", "streamAllColumns", "cqlIdentifier", 
		"streamProperties", "confName", "confValue", "strValue", "unaryOperator", 
		"functionName", "windowName", "className", "innerClassName", "userDefinedClassName", 
		"path", "applicationName", "columnName", "isForce", "ifExists", "ifNotExists", 
		"streamName", "dataSourceName", "streamAlias", "streamNameOrAlias", "columnALias", 
		"constNull", "extended", "identifierNot", "nullCondition", "operatorName"
	};

	@Override
	public String getGrammarFileName() { return "CQLParser.g4"; }

	@Override
	public String[] getTokenNames() { return tokenNames; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public CQLParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class StatementContext extends ParserRuleContext {
		public ExecStatementContext execStatement() {
			return getRuleContext(ExecStatementContext.class,0);
		}
		public TerminalNode EOF() { return getToken(CQLParser.EOF, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public DdlStatementContext ddlStatement() {
			return getRuleContext(DdlStatementContext.class,0);
		}
		public StatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_statement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StatementContext statement() throws RecognitionException {
		StatementContext _localctx = new StatementContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_statement);
		try {
			setState(391);
			switch (_input.LA(1)) {
			case KW_CREATE:
			case KW_SHOW:
			case KW_EXPLAIN:
			case KW_SET:
			case KW_GET:
			case KW_LOAD:
			case KW_DROP:
			case KW_ADD:
			case KW_SUBMIT:
			case KW_DEACTIVE:
			case KW_ACTIVE:
			case KW_REBALANCE:
				enterOuterAlt(_localctx, 1);
				{
				setState(382); ddlStatement();
				setState(383); match(EOF);
				}
				break;
			case KW_SELECT:
			case KW_FROM:
			case KW_INSERT:
				enterOuterAlt(_localctx, 2);
				{
				setState(385); execStatement();
				setState(386); match(EOF);
				}
				break;
			case KW_CASE:
			case KW_CAST:
			case KW_EXISTS:
			case KW_FALSE:
			case KW_NULL:
			case KW_TRUE:
			case KW_NOT:
			case KW_PREVIOUS:
			case KW_OPERATOR:
			case KW_DAY:
			case KW_HOUR:
			case KW_MINUTES:
			case KW_SECONDS:
			case KW_MILLISECONDS:
			case KW_BOOLEAN:
			case KW_INT:
			case KW_LONG:
			case KW_FLOAT:
			case KW_DOUBLE:
			case KW_STRING:
			case KW_TIMESTAMP:
			case KW_DATE:
			case KW_TIME:
			case KW_DECIMAL:
			case LPAREN:
			case PLUS:
			case MINUS:
			case TILDE:
			case LongLiteral:
			case FloatLiteral:
			case DoubleLiteral:
			case DecimalLiteral:
			case StringLiteral:
			case IntegerNumber:
			case Identifier:
				enterOuterAlt(_localctx, 3);
				{
				setState(388); expression();
				setState(389); match(EOF);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExecStatementContext extends ParserRuleContext {
		public InsertStatementContext insertStatement() {
			return getRuleContext(InsertStatementContext.class,0);
		}
		public MultiInsertStatementContext multiInsertStatement() {
			return getRuleContext(MultiInsertStatementContext.class,0);
		}
		public InsertUserOperatorStatementContext insertUserOperatorStatement() {
			return getRuleContext(InsertUserOperatorStatementContext.class,0);
		}
		public SelectStatementContext selectStatement() {
			return getRuleContext(SelectStatementContext.class,0);
		}
		public ExecStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_execStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExecStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExecStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExecStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExecStatementContext execStatement() throws RecognitionException {
		ExecStatementContext _localctx = new ExecStatementContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_execStatement);
		try {
			setState(397);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(393); insertStatement();
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(394); insertUserOperatorStatement();
				}
				break;

			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(395); multiInsertStatement();
				}
				break;

			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(396); selectStatement();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DdlStatementContext extends ParserRuleContext {
		public DeactiveApplicationContext deactiveApplication() {
			return getRuleContext(DeactiveApplicationContext.class,0);
		}
		public LoadStatementContext loadStatement() {
			return getRuleContext(LoadStatementContext.class,0);
		}
		public CreateOutputStreamStatementContext createOutputStreamStatement() {
			return getRuleContext(CreateOutputStreamStatementContext.class,0);
		}
		public ExplainStatementContext explainStatement() {
			return getRuleContext(ExplainStatementContext.class,0);
		}
		public AddFileStatementContext addFileStatement() {
			return getRuleContext(AddFileStatementContext.class,0);
		}
		public RebalanceApplicationContext rebalanceApplication() {
			return getRuleContext(RebalanceApplicationContext.class,0);
		}
		public CreateDataSourceStatementContext createDataSourceStatement() {
			return getRuleContext(CreateDataSourceStatementContext.class,0);
		}
		public DropApplicationContext dropApplication() {
			return getRuleContext(DropApplicationContext.class,0);
		}
		public SubmitApplicationContext submitApplication() {
			return getRuleContext(SubmitApplicationContext.class,0);
		}
		public GetStatementContext getStatement() {
			return getRuleContext(GetStatementContext.class,0);
		}
		public CreateInputStreamStatementContext createInputStreamStatement() {
			return getRuleContext(CreateInputStreamStatementContext.class,0);
		}
		public CreateOperatorStatementContext createOperatorStatement() {
			return getRuleContext(CreateOperatorStatementContext.class,0);
		}
		public SetStatementContext setStatement() {
			return getRuleContext(SetStatementContext.class,0);
		}
		public ShowApplicationsContext showApplications() {
			return getRuleContext(ShowApplicationsContext.class,0);
		}
		public CreatePipeStreamStatementContext createPipeStreamStatement() {
			return getRuleContext(CreatePipeStreamStatementContext.class,0);
		}
		public ActiveApplicationContext activeApplication() {
			return getRuleContext(ActiveApplicationContext.class,0);
		}
		public CreateFunctionStatementContext createFunctionStatement() {
			return getRuleContext(CreateFunctionStatementContext.class,0);
		}
		public ShowFunctionsContext showFunctions() {
			return getRuleContext(ShowFunctionsContext.class,0);
		}
		public DropFunctionStatementContext dropFunctionStatement() {
			return getRuleContext(DropFunctionStatementContext.class,0);
		}
		public AddJARStatementContext addJARStatement() {
			return getRuleContext(AddJARStatementContext.class,0);
		}
		public DdlStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ddlStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDdlStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDdlStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDdlStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DdlStatementContext ddlStatement() throws RecognitionException {
		DdlStatementContext _localctx = new DdlStatementContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_ddlStatement);
		try {
			setState(419);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(399); createInputStreamStatement();
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(400); createOutputStreamStatement();
				}
				break;

			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(401); createPipeStreamStatement();
				}
				break;

			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(402); createDataSourceStatement();
				}
				break;

			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(403); createOperatorStatement();
				}
				break;

			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(404); dropApplication();
				}
				break;

			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(405); submitApplication();
				}
				break;

			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(406); showFunctions();
				}
				break;

			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(407); showApplications();
				}
				break;

			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(408); loadStatement();
				}
				break;

			case 11:
				enterOuterAlt(_localctx, 11);
				{
				setState(409); explainStatement();
				}
				break;

			case 12:
				enterOuterAlt(_localctx, 12);
				{
				setState(410); getStatement();
				}
				break;

			case 13:
				enterOuterAlt(_localctx, 13);
				{
				setState(411); setStatement();
				}
				break;

			case 14:
				enterOuterAlt(_localctx, 14);
				{
				setState(412); createFunctionStatement();
				}
				break;

			case 15:
				enterOuterAlt(_localctx, 15);
				{
				setState(413); dropFunctionStatement();
				}
				break;

			case 16:
				enterOuterAlt(_localctx, 16);
				{
				setState(414); addFileStatement();
				}
				break;

			case 17:
				enterOuterAlt(_localctx, 17);
				{
				setState(415); addJARStatement();
				}
				break;

			case 18:
				enterOuterAlt(_localctx, 18);
				{
				setState(416); deactiveApplication();
				}
				break;

			case 19:
				enterOuterAlt(_localctx, 19);
				{
				setState(417); activeApplication();
				}
				break;

			case 20:
				enterOuterAlt(_localctx, 20);
				{
				setState(418); rebalanceApplication();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateInputStreamStatementContext extends ParserRuleContext {
		public ColumnNameTypeListContext columnNameTypeList() {
			return getRuleContext(ColumnNameTypeListContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public ParallelClauseContext parallelClause() {
			return getRuleContext(ParallelClauseContext.class,0);
		}
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public SerdeDefineContext serdeDefine() {
			return getRuleContext(SerdeDefineContext.class,0);
		}
		public TerminalNode KW_INPUT() { return getToken(CQLParser.KW_INPUT, 0); }
		public TerminalNode KW_STREAM() { return getToken(CQLParser.KW_STREAM, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public SourceDefineContext sourceDefine() {
			return getRuleContext(SourceDefineContext.class,0);
		}
		public TerminalNode KW_CREATE() { return getToken(CQLParser.KW_CREATE, 0); }
		public StreamNameContext streamName() {
			return getRuleContext(StreamNameContext.class,0);
		}
		public CreateInputStreamStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createInputStreamStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCreateInputStreamStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCreateInputStreamStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCreateInputStreamStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateInputStreamStatementContext createInputStreamStatement() throws RecognitionException {
		CreateInputStreamStatementContext _localctx = new CreateInputStreamStatementContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_createInputStreamStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(421); match(KW_CREATE);
			setState(422); match(KW_INPUT);
			setState(423); match(KW_STREAM);
			setState(424); streamName();
			setState(429);
			_la = _input.LA(1);
			if (_la==LPAREN) {
				{
				setState(425); match(LPAREN);
				setState(426); columnNameTypeList();
				setState(427); match(RPAREN);
				}
			}

			setState(432);
			_la = _input.LA(1);
			if (_la==KW_COMMENT) {
				{
				setState(431); comment();
				}
			}

			setState(435);
			_la = _input.LA(1);
			if (_la==KW_SERDE) {
				{
				setState(434); serdeDefine();
				}
			}

			setState(437); sourceDefine();
			setState(439);
			_la = _input.LA(1);
			if (_la==KW_PARALLEL) {
				{
				setState(438); parallelClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateOutputStreamStatementContext extends ParserRuleContext {
		public SinkDefineContext sinkDefine() {
			return getRuleContext(SinkDefineContext.class,0);
		}
		public ColumnNameTypeListContext columnNameTypeList() {
			return getRuleContext(ColumnNameTypeListContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public TerminalNode KW_OUTPUT() { return getToken(CQLParser.KW_OUTPUT, 0); }
		public ParallelClauseContext parallelClause() {
			return getRuleContext(ParallelClauseContext.class,0);
		}
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public SerdeDefineContext serdeDefine() {
			return getRuleContext(SerdeDefineContext.class,0);
		}
		public TerminalNode KW_STREAM() { return getToken(CQLParser.KW_STREAM, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public TerminalNode KW_CREATE() { return getToken(CQLParser.KW_CREATE, 0); }
		public StreamNameContext streamName() {
			return getRuleContext(StreamNameContext.class,0);
		}
		public CreateOutputStreamStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createOutputStreamStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCreateOutputStreamStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCreateOutputStreamStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCreateOutputStreamStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateOutputStreamStatementContext createOutputStreamStatement() throws RecognitionException {
		CreateOutputStreamStatementContext _localctx = new CreateOutputStreamStatementContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_createOutputStreamStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(441); match(KW_CREATE);
			setState(442); match(KW_OUTPUT);
			setState(443); match(KW_STREAM);
			setState(444); streamName();
			setState(449);
			_la = _input.LA(1);
			if (_la==LPAREN) {
				{
				setState(445); match(LPAREN);
				setState(446); columnNameTypeList();
				setState(447); match(RPAREN);
				}
			}

			setState(452);
			_la = _input.LA(1);
			if (_la==KW_COMMENT) {
				{
				setState(451); comment();
				}
			}

			setState(455);
			_la = _input.LA(1);
			if (_la==KW_SERDE) {
				{
				setState(454); serdeDefine();
				}
			}

			setState(457); sinkDefine();
			setState(459);
			_la = _input.LA(1);
			if (_la==KW_PARALLEL) {
				{
				setState(458); parallelClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreatePipeStreamStatementContext extends ParserRuleContext {
		public ColumnNameTypeListContext columnNameTypeList() {
			return getRuleContext(ColumnNameTypeListContext.class,0);
		}
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public TerminalNode KW_STREAM() { return getToken(CQLParser.KW_STREAM, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public TerminalNode KW_CREATE() { return getToken(CQLParser.KW_CREATE, 0); }
		public StreamNameContext streamName() {
			return getRuleContext(StreamNameContext.class,0);
		}
		public CreatePipeStreamStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createPipeStreamStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCreatePipeStreamStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCreatePipeStreamStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCreatePipeStreamStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreatePipeStreamStatementContext createPipeStreamStatement() throws RecognitionException {
		CreatePipeStreamStatementContext _localctx = new CreatePipeStreamStatementContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_createPipeStreamStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(461); match(KW_CREATE);
			setState(462); match(KW_STREAM);
			setState(463); streamName();
			setState(468);
			_la = _input.LA(1);
			if (_la==LPAREN) {
				{
				setState(464); match(LPAREN);
				setState(465); columnNameTypeList();
				setState(466); match(RPAREN);
				}
			}

			setState(471);
			_la = _input.LA(1);
			if (_la==KW_COMMENT) {
				{
				setState(470); comment();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateDataSourceStatementContext extends ParserRuleContext {
		public TerminalNode KW_DATASOURCE() { return getToken(CQLParser.KW_DATASOURCE, 0); }
		public DatasourcePropertiesContext datasourceProperties() {
			return getRuleContext(DatasourcePropertiesContext.class,0);
		}
		public TerminalNode KW_CREATE() { return getToken(CQLParser.KW_CREATE, 0); }
		public DataSourceNameContext dataSourceName() {
			return getRuleContext(DataSourceNameContext.class,0);
		}
		public ClassNameContext className() {
			return getRuleContext(ClassNameContext.class,0);
		}
		public TerminalNode KW_SOURCE() { return getToken(CQLParser.KW_SOURCE, 0); }
		public CreateDataSourceStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createDataSourceStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCreateDataSourceStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCreateDataSourceStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCreateDataSourceStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateDataSourceStatementContext createDataSourceStatement() throws RecognitionException {
		CreateDataSourceStatementContext _localctx = new CreateDataSourceStatementContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_createDataSourceStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(473); match(KW_CREATE);
			setState(474); match(KW_DATASOURCE);
			setState(475); dataSourceName();
			setState(476); match(KW_SOURCE);
			setState(477); className();
			setState(479);
			_la = _input.LA(1);
			if (_la==KW_PROPERTIES) {
				{
				setState(478); datasourceProperties();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateOperatorStatementContext extends ParserRuleContext {
		public TerminalNode KW_OPERATOR() { return getToken(CQLParser.KW_OPERATOR, 0); }
		public TerminalNode KW_AS() { return getToken(CQLParser.KW_AS, 0); }
		public OutputSchemaStatementContext outputSchemaStatement() {
			return getRuleContext(OutputSchemaStatementContext.class,0);
		}
		public StreamPropertiesContext streamProperties() {
			return getRuleContext(StreamPropertiesContext.class,0);
		}
		public OperatorNameContext operatorName() {
			return getRuleContext(OperatorNameContext.class,0);
		}
		public InputSchemaStatementContext inputSchemaStatement() {
			return getRuleContext(InputSchemaStatementContext.class,0);
		}
		public TerminalNode KW_CREATE() { return getToken(CQLParser.KW_CREATE, 0); }
		public ClassNameContext className() {
			return getRuleContext(ClassNameContext.class,0);
		}
		public CreateOperatorStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createOperatorStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCreateOperatorStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCreateOperatorStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCreateOperatorStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateOperatorStatementContext createOperatorStatement() throws RecognitionException {
		CreateOperatorStatementContext _localctx = new CreateOperatorStatementContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_createOperatorStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(481); match(KW_CREATE);
			setState(482); match(KW_OPERATOR);
			setState(483); operatorName();
			setState(484); match(KW_AS);
			setState(485); className();
			setState(486); inputSchemaStatement();
			setState(487); outputSchemaStatement();
			setState(489);
			_la = _input.LA(1);
			if (_la==KW_PROPERTIES) {
				{
				setState(488); streamProperties();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InputSchemaStatementContext extends ParserRuleContext {
		public ColumnNameTypeListContext columnNameTypeList() {
			return getRuleContext(ColumnNameTypeListContext.class,0);
		}
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public TerminalNode KW_INPUT() { return getToken(CQLParser.KW_INPUT, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public InputSchemaStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_inputSchemaStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterInputSchemaStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitInputSchemaStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitInputSchemaStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InputSchemaStatementContext inputSchemaStatement() throws RecognitionException {
		InputSchemaStatementContext _localctx = new InputSchemaStatementContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_inputSchemaStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(491); match(KW_INPUT);
			setState(492); match(LPAREN);
			setState(493); columnNameTypeList();
			setState(494); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OutputSchemaStatementContext extends ParserRuleContext {
		public ColumnNameTypeListContext columnNameTypeList() {
			return getRuleContext(ColumnNameTypeListContext.class,0);
		}
		public TerminalNode KW_OUTPUT() { return getToken(CQLParser.KW_OUTPUT, 0); }
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public OutputSchemaStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_outputSchemaStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterOutputSchemaStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitOutputSchemaStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitOutputSchemaStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OutputSchemaStatementContext outputSchemaStatement() throws RecognitionException {
		OutputSchemaStatementContext _localctx = new OutputSchemaStatementContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_outputSchemaStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(496); match(KW_OUTPUT);
			setState(497); match(LPAREN);
			setState(498); columnNameTypeList();
			setState(499); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SerdeDefineContext extends ParserRuleContext {
		public SerdePropertiesContext serdeProperties() {
			return getRuleContext(SerdePropertiesContext.class,0);
		}
		public SerdeClassContext serdeClass() {
			return getRuleContext(SerdeClassContext.class,0);
		}
		public SerdeDefineContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_serdeDefine; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSerdeDefine(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSerdeDefine(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSerdeDefine(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SerdeDefineContext serdeDefine() throws RecognitionException {
		SerdeDefineContext _localctx = new SerdeDefineContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_serdeDefine);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(501); serdeClass();
			setState(503);
			_la = _input.LA(1);
			if (_la==KW_PROPERTIES) {
				{
				setState(502); serdeProperties();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceDefineContext extends ParserRuleContext {
		public SourceClauseContext sourceClause() {
			return getRuleContext(SourceClauseContext.class,0);
		}
		public SourcePropertiesContext sourceProperties() {
			return getRuleContext(SourcePropertiesContext.class,0);
		}
		public SourceDefineContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceDefine; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSourceDefine(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSourceDefine(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSourceDefine(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SourceDefineContext sourceDefine() throws RecognitionException {
		SourceDefineContext _localctx = new SourceDefineContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_sourceDefine);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(505); sourceClause();
			setState(507);
			_la = _input.LA(1);
			if (_la==KW_PROPERTIES) {
				{
				setState(506); sourceProperties();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SinkDefineContext extends ParserRuleContext {
		public SinkClauseContext sinkClause() {
			return getRuleContext(SinkClauseContext.class,0);
		}
		public SinkPropertiesContext sinkProperties() {
			return getRuleContext(SinkPropertiesContext.class,0);
		}
		public SinkDefineContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sinkDefine; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSinkDefine(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSinkDefine(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSinkDefine(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SinkDefineContext sinkDefine() throws RecognitionException {
		SinkDefineContext _localctx = new SinkDefineContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_sinkDefine);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(509); sinkClause();
			setState(511);
			_la = _input.LA(1);
			if (_la==KW_PROPERTIES) {
				{
				setState(510); sinkProperties();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PrimitiveTypeContext extends ParserRuleContext {
		public TerminalNode KW_LONG() { return getToken(CQLParser.KW_LONG, 0); }
		public TerminalNode KW_STRING() { return getToken(CQLParser.KW_STRING, 0); }
		public TerminalNode KW_BOOLEAN() { return getToken(CQLParser.KW_BOOLEAN, 0); }
		public TerminalNode KW_TIMESTAMP() { return getToken(CQLParser.KW_TIMESTAMP, 0); }
		public TerminalNode KW_DOUBLE() { return getToken(CQLParser.KW_DOUBLE, 0); }
		public TerminalNode KW_INT() { return getToken(CQLParser.KW_INT, 0); }
		public TerminalNode KW_TIME() { return getToken(CQLParser.KW_TIME, 0); }
		public TerminalNode KW_DECIMAL() { return getToken(CQLParser.KW_DECIMAL, 0); }
		public TerminalNode KW_DATE() { return getToken(CQLParser.KW_DATE, 0); }
		public TerminalNode KW_FLOAT() { return getToken(CQLParser.KW_FLOAT, 0); }
		public PrimitiveTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_primitiveType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterPrimitiveType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitPrimitiveType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitPrimitiveType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrimitiveTypeContext primitiveType() throws RecognitionException {
		PrimitiveTypeContext _localctx = new PrimitiveTypeContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_primitiveType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(513);
			_la = _input.LA(1);
			if ( !(((((_la - 109)) & ~0x3f) == 0 && ((1L << (_la - 109)) & ((1L << (KW_BOOLEAN - 109)) | (1L << (KW_INT - 109)) | (1L << (KW_LONG - 109)) | (1L << (KW_FLOAT - 109)) | (1L << (KW_DOUBLE - 109)) | (1L << (KW_STRING - 109)) | (1L << (KW_TIMESTAMP - 109)) | (1L << (KW_DATE - 109)) | (1L << (KW_TIME - 109)) | (1L << (KW_DECIMAL - 109)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColTypeContext extends ParserRuleContext {
		public PrimitiveTypeContext primitiveType() {
			return getRuleContext(PrimitiveTypeContext.class,0);
		}
		public ColTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_colType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterColType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitColType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitColType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColTypeContext colType() throws RecognitionException {
		ColTypeContext _localctx = new ColTypeContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_colType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(515); primitiveType();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnNameTypeContext extends ParserRuleContext {
		public CommentContext comment() {
			return getRuleContext(CommentContext.class,0);
		}
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public ColTypeContext colType() {
			return getRuleContext(ColTypeContext.class,0);
		}
		public ColumnNameTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnNameType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterColumnNameType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitColumnNameType(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitColumnNameType(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnNameTypeContext columnNameType() throws RecognitionException {
		ColumnNameTypeContext _localctx = new ColumnNameTypeContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_columnNameType);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(517); columnName();
			setState(518); colType();
			setState(520);
			_la = _input.LA(1);
			if (_la==KW_COMMENT) {
				{
				setState(519); comment();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CommentContext extends ParserRuleContext {
		public TerminalNode KW_COMMENT() { return getToken(CQLParser.KW_COMMENT, 0); }
		public CommentStringContext commentString() {
			return getRuleContext(CommentStringContext.class,0);
		}
		public CommentContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_comment; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterComment(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitComment(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitComment(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CommentContext comment() throws RecognitionException {
		CommentContext _localctx = new CommentContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_comment);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(522); match(KW_COMMENT);
			setState(523); commentString();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CommentStringContext extends ParserRuleContext {
		public TerminalNode StringLiteral() { return getToken(CQLParser.StringLiteral, 0); }
		public CommentStringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_commentString; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCommentString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCommentString(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCommentString(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CommentStringContext commentString() throws RecognitionException {
		CommentStringContext _localctx = new CommentStringContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_commentString);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(525); match(StringLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnNameTypeListContext extends ParserRuleContext {
		public ColumnNameTypeContext columnNameType(int i) {
			return getRuleContext(ColumnNameTypeContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public List<ColumnNameTypeContext> columnNameType() {
			return getRuleContexts(ColumnNameTypeContext.class);
		}
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public ColumnNameTypeListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnNameTypeList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterColumnNameTypeList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitColumnNameTypeList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitColumnNameTypeList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnNameTypeListContext columnNameTypeList() throws RecognitionException {
		ColumnNameTypeListContext _localctx = new ColumnNameTypeListContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_columnNameTypeList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(527); columnNameType();
			setState(532);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(528); match(COMMA);
				setState(529); columnNameType();
				}
				}
				setState(534);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StreamPropertiesListContext extends ParserRuleContext {
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public List<KeyValuePropertyContext> keyValueProperty() {
			return getRuleContexts(KeyValuePropertyContext.class);
		}
		public KeyValuePropertyContext keyValueProperty(int i) {
			return getRuleContext(KeyValuePropertyContext.class,i);
		}
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public StreamPropertiesListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_streamPropertiesList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStreamPropertiesList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStreamPropertiesList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStreamPropertiesList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StreamPropertiesListContext streamPropertiesList() throws RecognitionException {
		StreamPropertiesListContext _localctx = new StreamPropertiesListContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_streamPropertiesList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(535); keyValueProperty();
			setState(540);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(536); match(COMMA);
				setState(537); keyValueProperty();
				}
				}
				setState(542);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeyValuePropertyContext extends ParserRuleContext {
		public TerminalNode EQUAL() { return getToken(CQLParser.EQUAL, 0); }
		public ConfValueContext confValue() {
			return getRuleContext(ConfValueContext.class,0);
		}
		public ConfNameContext confName() {
			return getRuleContext(ConfNameContext.class,0);
		}
		public KeyValuePropertyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyValueProperty; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterKeyValueProperty(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitKeyValueProperty(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitKeyValueProperty(this);
			else return visitor.visitChildren(this);
		}
	}

	public final KeyValuePropertyContext keyValueProperty() throws RecognitionException {
		KeyValuePropertyContext _localctx = new KeyValuePropertyContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_keyValueProperty);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(543); confName();
			setState(544); match(EQUAL);
			setState(545); confValue();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SerdeClassContext extends ParserRuleContext {
		public TerminalNode KW_SERDE() { return getToken(CQLParser.KW_SERDE, 0); }
		public ClassNameContext className() {
			return getRuleContext(ClassNameContext.class,0);
		}
		public SerdeClassContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_serdeClass; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSerdeClass(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSerdeClass(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSerdeClass(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SerdeClassContext serdeClass() throws RecognitionException {
		SerdeClassContext _localctx = new SerdeClassContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_serdeClass);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(547); match(KW_SERDE);
			setState(548); className();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SerdePropertiesContext extends ParserRuleContext {
		public StreamPropertiesContext streamProperties() {
			return getRuleContext(StreamPropertiesContext.class,0);
		}
		public SerdePropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_serdeProperties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSerdeProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSerdeProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSerdeProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SerdePropertiesContext serdeProperties() throws RecognitionException {
		SerdePropertiesContext _localctx = new SerdePropertiesContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_serdeProperties);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(550); streamProperties();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatasourcePropertiesContext extends ParserRuleContext {
		public StreamPropertiesContext streamProperties() {
			return getRuleContext(StreamPropertiesContext.class,0);
		}
		public DatasourcePropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasourceProperties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDatasourceProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDatasourceProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDatasourceProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatasourcePropertiesContext datasourceProperties() throws RecognitionException {
		DatasourcePropertiesContext _localctx = new DatasourcePropertiesContext(_ctx, getState());
		enterRule(_localctx, 46, RULE_datasourceProperties);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(552); streamProperties();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SinkClauseContext extends ParserRuleContext {
		public TerminalNode KW_SINK() { return getToken(CQLParser.KW_SINK, 0); }
		public ClassNameContext className() {
			return getRuleContext(ClassNameContext.class,0);
		}
		public SinkClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sinkClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSinkClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSinkClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSinkClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SinkClauseContext sinkClause() throws RecognitionException {
		SinkClauseContext _localctx = new SinkClauseContext(_ctx, getState());
		enterRule(_localctx, 48, RULE_sinkClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(554); match(KW_SINK);
			setState(555); className();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SinkPropertiesContext extends ParserRuleContext {
		public StreamPropertiesContext streamProperties() {
			return getRuleContext(StreamPropertiesContext.class,0);
		}
		public SinkPropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sinkProperties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSinkProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSinkProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSinkProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SinkPropertiesContext sinkProperties() throws RecognitionException {
		SinkPropertiesContext _localctx = new SinkPropertiesContext(_ctx, getState());
		enterRule(_localctx, 50, RULE_sinkProperties);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(557); streamProperties();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceClauseContext extends ParserRuleContext {
		public ClassNameContext className() {
			return getRuleContext(ClassNameContext.class,0);
		}
		public TerminalNode KW_SOURCE() { return getToken(CQLParser.KW_SOURCE, 0); }
		public SourceClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSourceClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSourceClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSourceClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SourceClauseContext sourceClause() throws RecognitionException {
		SourceClauseContext _localctx = new SourceClauseContext(_ctx, getState());
		enterRule(_localctx, 52, RULE_sourceClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(559); match(KW_SOURCE);
			setState(560); className();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourcePropertiesContext extends ParserRuleContext {
		public StreamPropertiesContext streamProperties() {
			return getRuleContext(StreamPropertiesContext.class,0);
		}
		public SourcePropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceProperties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSourceProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSourceProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSourceProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SourcePropertiesContext sourceProperties() throws RecognitionException {
		SourcePropertiesContext _localctx = new SourcePropertiesContext(_ctx, getState());
		enterRule(_localctx, 54, RULE_sourceProperties);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(562); streamProperties();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AddFileStatementContext extends ParserRuleContext {
		public TerminalNode KW_ADD() { return getToken(CQLParser.KW_ADD, 0); }
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public TerminalNode KW_FILE() { return getToken(CQLParser.KW_FILE, 0); }
		public AddFileStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_addFileStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterAddFileStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitAddFileStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitAddFileStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AddFileStatementContext addFileStatement() throws RecognitionException {
		AddFileStatementContext _localctx = new AddFileStatementContext(_ctx, getState());
		enterRule(_localctx, 56, RULE_addFileStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(564); match(KW_ADD);
			setState(565); match(KW_FILE);
			setState(566); path();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AddJARStatementContext extends ParserRuleContext {
		public TerminalNode KW_ADD() { return getToken(CQLParser.KW_ADD, 0); }
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public TerminalNode KW_JAR() { return getToken(CQLParser.KW_JAR, 0); }
		public AddJARStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_addJARStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterAddJARStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitAddJARStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitAddJARStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AddJARStatementContext addJARStatement() throws RecognitionException {
		AddJARStatementContext _localctx = new AddJARStatementContext(_ctx, getState());
		enterRule(_localctx, 58, RULE_addJARStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(568); match(KW_ADD);
			setState(569); match(KW_JAR);
			setState(570); path();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CreateFunctionStatementContext extends ParserRuleContext {
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public TerminalNode KW_AS() { return getToken(CQLParser.KW_AS, 0); }
		public StreamPropertiesContext streamProperties() {
			return getRuleContext(StreamPropertiesContext.class,0);
		}
		public TerminalNode KW_FUNCTION() { return getToken(CQLParser.KW_FUNCTION, 0); }
		public TerminalNode KW_CREATE() { return getToken(CQLParser.KW_CREATE, 0); }
		public UserDefinedClassNameContext userDefinedClassName() {
			return getRuleContext(UserDefinedClassNameContext.class,0);
		}
		public CreateFunctionStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_createFunctionStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCreateFunctionStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCreateFunctionStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCreateFunctionStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CreateFunctionStatementContext createFunctionStatement() throws RecognitionException {
		CreateFunctionStatementContext _localctx = new CreateFunctionStatementContext(_ctx, getState());
		enterRule(_localctx, 60, RULE_createFunctionStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(572); match(KW_CREATE);
			setState(573); match(KW_FUNCTION);
			setState(574); functionName();
			setState(575); match(KW_AS);
			setState(576); userDefinedClassName();
			setState(578);
			_la = _input.LA(1);
			if (_la==KW_PROPERTIES) {
				{
				setState(577); streamProperties();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropFunctionStatementContext extends ParserRuleContext {
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public TerminalNode KW_FUNCTION() { return getToken(CQLParser.KW_FUNCTION, 0); }
		public TerminalNode KW_DROP() { return getToken(CQLParser.KW_DROP, 0); }
		public IfExistsContext ifExists() {
			return getRuleContext(IfExistsContext.class,0);
		}
		public DropFunctionStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropFunctionStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDropFunctionStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDropFunctionStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDropFunctionStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropFunctionStatementContext dropFunctionStatement() throws RecognitionException {
		DropFunctionStatementContext _localctx = new DropFunctionStatementContext(_ctx, getState());
		enterRule(_localctx, 62, RULE_dropFunctionStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(580); match(KW_DROP);
			setState(581); match(KW_FUNCTION);
			setState(583);
			_la = _input.LA(1);
			if (_la==KW_IF) {
				{
				setState(582); ifExists();
				}
			}

			setState(585); functionName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GetStatementContext extends ParserRuleContext {
		public TerminalNode KW_GET() { return getToken(CQLParser.KW_GET, 0); }
		public ConfNameContext confName() {
			return getRuleContext(ConfNameContext.class,0);
		}
		public GetStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_getStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterGetStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitGetStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitGetStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GetStatementContext getStatement() throws RecognitionException {
		GetStatementContext _localctx = new GetStatementContext(_ctx, getState());
		enterRule(_localctx, 64, RULE_getStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(587); match(KW_GET);
			setState(588); confName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SetStatementContext extends ParserRuleContext {
		public TerminalNode EQUAL() { return getToken(CQLParser.EQUAL, 0); }
		public ConfValueContext confValue() {
			return getRuleContext(ConfValueContext.class,0);
		}
		public ConfNameContext confName() {
			return getRuleContext(ConfNameContext.class,0);
		}
		public TerminalNode KW_SET() { return getToken(CQLParser.KW_SET, 0); }
		public SetStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_setStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSetStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSetStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSetStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SetStatementContext setStatement() throws RecognitionException {
		SetStatementContext _localctx = new SetStatementContext(_ctx, getState());
		enterRule(_localctx, 66, RULE_setStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(590); match(KW_SET);
			setState(591); confName();
			setState(592); match(EQUAL);
			setState(593); confValue();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LoadStatementContext extends ParserRuleContext {
		public TerminalNode KW_LOAD() { return getToken(CQLParser.KW_LOAD, 0); }
		public TerminalNode KW_APPLICATION() { return getToken(CQLParser.KW_APPLICATION, 0); }
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public LoadStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_loadStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterLoadStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitLoadStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitLoadStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LoadStatementContext loadStatement() throws RecognitionException {
		LoadStatementContext _localctx = new LoadStatementContext(_ctx, getState());
		enterRule(_localctx, 68, RULE_loadStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(595); match(KW_LOAD);
			setState(596); match(KW_APPLICATION);
			setState(597); path();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExplainStatementContext extends ParserRuleContext {
		public TerminalNode KW_APPLICATION() { return getToken(CQLParser.KW_APPLICATION, 0); }
		public TerminalNode KW_EXPLAIN() { return getToken(CQLParser.KW_EXPLAIN, 0); }
		public ApplicationNameContext applicationName() {
			return getRuleContext(ApplicationNameContext.class,0);
		}
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public ExplainStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_explainStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExplainStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExplainStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExplainStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExplainStatementContext explainStatement() throws RecognitionException {
		ExplainStatementContext _localctx = new ExplainStatementContext(_ctx, getState());
		enterRule(_localctx, 70, RULE_explainStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(599); match(KW_EXPLAIN);
			setState(600); match(KW_APPLICATION);
			setState(601); applicationName();
			setState(603);
			_la = _input.LA(1);
			if (_la==StringLiteral) {
				{
				setState(602); path();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ShowFunctionsContext extends ParserRuleContext {
		public TerminalNode KW_SHOW() { return getToken(CQLParser.KW_SHOW, 0); }
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public TerminalNode KW_FUNCTIONS() { return getToken(CQLParser.KW_FUNCTIONS, 0); }
		public ExtendedContext extended() {
			return getRuleContext(ExtendedContext.class,0);
		}
		public ShowFunctionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showFunctions; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterShowFunctions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitShowFunctions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitShowFunctions(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ShowFunctionsContext showFunctions() throws RecognitionException {
		ShowFunctionsContext _localctx = new ShowFunctionsContext(_ctx, getState());
		enterRule(_localctx, 72, RULE_showFunctions);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(605); match(KW_SHOW);
			setState(606); match(KW_FUNCTIONS);
			setState(608);
			_la = _input.LA(1);
			if (_la==KW_EXTENDED) {
				{
				setState(607); extended();
				}
			}

			setState(610); functionName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ShowApplicationsContext extends ParserRuleContext {
		public TerminalNode KW_SHOW() { return getToken(CQLParser.KW_SHOW, 0); }
		public ApplicationNameContext applicationName() {
			return getRuleContext(ApplicationNameContext.class,0);
		}
		public TerminalNode KW_APPLICATIONS() { return getToken(CQLParser.KW_APPLICATIONS, 0); }
		public StrValueContext strValue() {
			return getRuleContext(StrValueContext.class,0);
		}
		public ShowApplicationsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_showApplications; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterShowApplications(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitShowApplications(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitShowApplications(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ShowApplicationsContext showApplications() throws RecognitionException {
		ShowApplicationsContext _localctx = new ShowApplicationsContext(_ctx, getState());
		enterRule(_localctx, 74, RULE_showApplications);
		int _la;
		try {
			setState(622);
			switch ( getInterpreter().adaptivePredict(_input,27,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(612); match(KW_SHOW);
				setState(613); match(KW_APPLICATIONS);
				setState(615);
				_la = _input.LA(1);
				if (((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (KW_OPERATOR - 101)) | (1L << (KW_DAY - 101)) | (1L << (KW_HOUR - 101)) | (1L << (KW_MINUTES - 101)) | (1L << (KW_SECONDS - 101)) | (1L << (KW_MILLISECONDS - 101)) | (1L << (KW_BOOLEAN - 101)) | (1L << (KW_INT - 101)) | (1L << (KW_LONG - 101)) | (1L << (KW_FLOAT - 101)) | (1L << (KW_DOUBLE - 101)) | (1L << (KW_STRING - 101)) | (1L << (KW_TIMESTAMP - 101)) | (1L << (KW_DATE - 101)) | (1L << (KW_TIME - 101)) | (1L << (KW_DECIMAL - 101)) | (1L << (IntegerNumber - 101)) | (1L << (Identifier - 101)))) != 0)) {
					{
					setState(614); applicationName();
					}
				}

				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(617); match(KW_SHOW);
				setState(618); match(KW_APPLICATIONS);
				setState(620);
				_la = _input.LA(1);
				if (_la==StringLiteral) {
					{
					setState(619); strValue();
					}
				}

				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DropApplicationContext extends ParserRuleContext {
		public TerminalNode KW_APPLICATION() { return getToken(CQLParser.KW_APPLICATION, 0); }
		public TerminalNode KW_DROP() { return getToken(CQLParser.KW_DROP, 0); }
		public ApplicationNameContext applicationName() {
			return getRuleContext(ApplicationNameContext.class,0);
		}
		public IfExistsContext ifExists() {
			return getRuleContext(IfExistsContext.class,0);
		}
		public DropApplicationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dropApplication; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDropApplication(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDropApplication(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDropApplication(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DropApplicationContext dropApplication() throws RecognitionException {
		DropApplicationContext _localctx = new DropApplicationContext(_ctx, getState());
		enterRule(_localctx, 76, RULE_dropApplication);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(624); match(KW_DROP);
			setState(625); match(KW_APPLICATION);
			setState(627);
			_la = _input.LA(1);
			if (_la==KW_IF) {
				{
				setState(626); ifExists();
				}
			}

			setState(629); applicationName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubmitApplicationContext extends ParserRuleContext {
		public TerminalNode KW_SUBMIT() { return getToken(CQLParser.KW_SUBMIT, 0); }
		public IsForceContext isForce() {
			return getRuleContext(IsForceContext.class,0);
		}
		public TerminalNode KW_APPLICATION() { return getToken(CQLParser.KW_APPLICATION, 0); }
		public ApplicationNameContext applicationName() {
			return getRuleContext(ApplicationNameContext.class,0);
		}
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public SubmitApplicationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_submitApplication; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSubmitApplication(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSubmitApplication(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSubmitApplication(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubmitApplicationContext submitApplication() throws RecognitionException {
		SubmitApplicationContext _localctx = new SubmitApplicationContext(_ctx, getState());
		enterRule(_localctx, 78, RULE_submitApplication);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(631); match(KW_SUBMIT);
			setState(632); match(KW_APPLICATION);
			setState(634);
			_la = _input.LA(1);
			if (_la==KW_FORCE) {
				{
				setState(633); isForce();
				}
			}

			setState(637);
			_la = _input.LA(1);
			if (((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (KW_OPERATOR - 101)) | (1L << (KW_DAY - 101)) | (1L << (KW_HOUR - 101)) | (1L << (KW_MINUTES - 101)) | (1L << (KW_SECONDS - 101)) | (1L << (KW_MILLISECONDS - 101)) | (1L << (KW_BOOLEAN - 101)) | (1L << (KW_INT - 101)) | (1L << (KW_LONG - 101)) | (1L << (KW_FLOAT - 101)) | (1L << (KW_DOUBLE - 101)) | (1L << (KW_STRING - 101)) | (1L << (KW_TIMESTAMP - 101)) | (1L << (KW_DATE - 101)) | (1L << (KW_TIME - 101)) | (1L << (KW_DECIMAL - 101)) | (1L << (IntegerNumber - 101)) | (1L << (Identifier - 101)))) != 0)) {
				{
				setState(636); applicationName();
				}
			}

			setState(640);
			_la = _input.LA(1);
			if (_la==StringLiteral) {
				{
				setState(639); path();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DeactiveApplicationContext extends ParserRuleContext {
		public TerminalNode KW_APPLICATION() { return getToken(CQLParser.KW_APPLICATION, 0); }
		public TerminalNode KW_DEACTIVE() { return getToken(CQLParser.KW_DEACTIVE, 0); }
		public ApplicationNameContext applicationName() {
			return getRuleContext(ApplicationNameContext.class,0);
		}
		public DeactiveApplicationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_deactiveApplication; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDeactiveApplication(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDeactiveApplication(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDeactiveApplication(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DeactiveApplicationContext deactiveApplication() throws RecognitionException {
		DeactiveApplicationContext _localctx = new DeactiveApplicationContext(_ctx, getState());
		enterRule(_localctx, 80, RULE_deactiveApplication);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(642); match(KW_DEACTIVE);
			setState(643); match(KW_APPLICATION);
			setState(644); applicationName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ActiveApplicationContext extends ParserRuleContext {
		public TerminalNode KW_APPLICATION() { return getToken(CQLParser.KW_APPLICATION, 0); }
		public TerminalNode KW_ACTIVE() { return getToken(CQLParser.KW_ACTIVE, 0); }
		public ApplicationNameContext applicationName() {
			return getRuleContext(ApplicationNameContext.class,0);
		}
		public ActiveApplicationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_activeApplication; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterActiveApplication(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitActiveApplication(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitActiveApplication(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ActiveApplicationContext activeApplication() throws RecognitionException {
		ActiveApplicationContext _localctx = new ActiveApplicationContext(_ctx, getState());
		enterRule(_localctx, 82, RULE_activeApplication);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(646); match(KW_ACTIVE);
			setState(647); match(KW_APPLICATION);
			setState(648); applicationName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RebalanceApplicationContext extends ParserRuleContext {
		public TerminalNode KW_REBALANCE() { return getToken(CQLParser.KW_REBALANCE, 0); }
		public TerminalNode KW_APPLICATION() { return getToken(CQLParser.KW_APPLICATION, 0); }
		public TerminalNode KW_WORKER() { return getToken(CQLParser.KW_WORKER, 0); }
		public TerminalNode KW_SET() { return getToken(CQLParser.KW_SET, 0); }
		public ApplicationNameContext applicationName() {
			return getRuleContext(ApplicationNameContext.class,0);
		}
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public RebalanceApplicationContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rebalanceApplication; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRebalanceApplication(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRebalanceApplication(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRebalanceApplication(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RebalanceApplicationContext rebalanceApplication() throws RecognitionException {
		RebalanceApplicationContext _localctx = new RebalanceApplicationContext(_ctx, getState());
		enterRule(_localctx, 84, RULE_rebalanceApplication);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(650); match(KW_REBALANCE);
			setState(651); match(KW_APPLICATION);
			setState(652); applicationName();
			setState(653); match(KW_SET);
			setState(654); match(KW_WORKER);
			setState(655); constIntegerValue();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public LogicExpressionOrContext logicExpressionOr() {
			return getRuleContext(LogicExpressionOrContext.class,0);
		}
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 86, RULE_expression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(657); logicExpressionOr();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LogicExpressionOrContext extends ParserRuleContext {
		public LogicExpressionAndContext logicExpressionAnd(int i) {
			return getRuleContext(LogicExpressionAndContext.class,i);
		}
		public TerminalNode KW_OR(int i) {
			return getToken(CQLParser.KW_OR, i);
		}
		public List<TerminalNode> KW_OR() { return getTokens(CQLParser.KW_OR); }
		public List<LogicExpressionAndContext> logicExpressionAnd() {
			return getRuleContexts(LogicExpressionAndContext.class);
		}
		public LogicExpressionOrContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logicExpressionOr; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterLogicExpressionOr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitLogicExpressionOr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitLogicExpressionOr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LogicExpressionOrContext logicExpressionOr() throws RecognitionException {
		LogicExpressionOrContext _localctx = new LogicExpressionOrContext(_ctx, getState());
		enterRule(_localctx, 88, RULE_logicExpressionOr);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(659); logicExpressionAnd();
			setState(664);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==KW_OR) {
				{
				{
				setState(660); match(KW_OR);
				setState(661); logicExpressionAnd();
				}
				}
				setState(666);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LogicExpressionAndContext extends ParserRuleContext {
		public LogicExpressionNotContext logicExpressionNot(int i) {
			return getRuleContext(LogicExpressionNotContext.class,i);
		}
		public List<LogicExpressionNotContext> logicExpressionNot() {
			return getRuleContexts(LogicExpressionNotContext.class);
		}
		public List<TerminalNode> KW_AND() { return getTokens(CQLParser.KW_AND); }
		public TerminalNode KW_AND(int i) {
			return getToken(CQLParser.KW_AND, i);
		}
		public LogicExpressionAndContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logicExpressionAnd; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterLogicExpressionAnd(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitLogicExpressionAnd(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitLogicExpressionAnd(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LogicExpressionAndContext logicExpressionAnd() throws RecognitionException {
		LogicExpressionAndContext _localctx = new LogicExpressionAndContext(_ctx, getState());
		enterRule(_localctx, 90, RULE_logicExpressionAnd);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(667); logicExpressionNot();
			setState(672);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==KW_AND) {
				{
				{
				setState(668); match(KW_AND);
				setState(669); logicExpressionNot();
				}
				}
				setState(674);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LogicExpressionNotContext extends ParserRuleContext {
		public EqualRelationExpressionContext equalRelationExpression() {
			return getRuleContext(EqualRelationExpressionContext.class,0);
		}
		public IdentifierNotContext identifierNot() {
			return getRuleContext(IdentifierNotContext.class,0);
		}
		public LogicExpressionNotContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logicExpressionNot; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterLogicExpressionNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitLogicExpressionNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitLogicExpressionNot(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LogicExpressionNotContext logicExpressionNot() throws RecognitionException {
		LogicExpressionNotContext _localctx = new LogicExpressionNotContext(_ctx, getState());
		enterRule(_localctx, 92, RULE_logicExpressionNot);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(676);
			_la = _input.LA(1);
			if (_la==KW_NOT) {
				{
				setState(675); identifierNot();
				}
			}

			setState(678); equalRelationExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class EqualRelationExpressionContext extends ParserRuleContext {
		public ExpressionExistsContext expressionExists() {
			return getRuleContext(ExpressionExistsContext.class,0);
		}
		public IsNullLikeInExpressionsContext isNullLikeInExpressions() {
			return getRuleContext(IsNullLikeInExpressionsContext.class,0);
		}
		public EqualRelationExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_equalRelationExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterEqualRelationExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitEqualRelationExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitEqualRelationExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final EqualRelationExpressionContext equalRelationExpression() throws RecognitionException {
		EqualRelationExpressionContext _localctx = new EqualRelationExpressionContext(_ctx, getState());
		enterRule(_localctx, 94, RULE_equalRelationExpression);
		try {
			setState(682);
			switch (_input.LA(1)) {
			case KW_CASE:
			case KW_CAST:
			case KW_FALSE:
			case KW_NULL:
			case KW_TRUE:
			case KW_PREVIOUS:
			case KW_OPERATOR:
			case KW_DAY:
			case KW_HOUR:
			case KW_MINUTES:
			case KW_SECONDS:
			case KW_MILLISECONDS:
			case KW_BOOLEAN:
			case KW_INT:
			case KW_LONG:
			case KW_FLOAT:
			case KW_DOUBLE:
			case KW_STRING:
			case KW_TIMESTAMP:
			case KW_DATE:
			case KW_TIME:
			case KW_DECIMAL:
			case LPAREN:
			case PLUS:
			case MINUS:
			case TILDE:
			case LongLiteral:
			case FloatLiteral:
			case DoubleLiteral:
			case DecimalLiteral:
			case StringLiteral:
			case IntegerNumber:
			case Identifier:
				enterOuterAlt(_localctx, 1);
				{
				setState(680); isNullLikeInExpressions();
				}
				break;
			case KW_EXISTS:
				enterOuterAlt(_localctx, 2);
				{
				setState(681); expressionExists();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IsNullLikeInExpressionsContext extends ParserRuleContext {
		public BinaryExpressionContext binaryExpression() {
			return getRuleContext(BinaryExpressionContext.class,0);
		}
		public NullConditionContext nullCondition() {
			return getRuleContext(NullConditionContext.class,0);
		}
		public TerminalNode KW_IS() { return getToken(CQLParser.KW_IS, 0); }
		public ExpressionInContext expressionIn() {
			return getRuleContext(ExpressionInContext.class,0);
		}
		public ExpressionLikeContext expressionLike() {
			return getRuleContext(ExpressionLikeContext.class,0);
		}
		public ExpressionBetweenContext expressionBetween() {
			return getRuleContext(ExpressionBetweenContext.class,0);
		}
		public IsNullLikeInExpressionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_isNullLikeInExpressions; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterIsNullLikeInExpressions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitIsNullLikeInExpressions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitIsNullLikeInExpressions(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IsNullLikeInExpressionsContext isNullLikeInExpressions() throws RecognitionException {
		IsNullLikeInExpressionsContext _localctx = new IsNullLikeInExpressionsContext(_ctx, getState());
		enterRule(_localctx, 96, RULE_isNullLikeInExpressions);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(684); binaryExpression();
			setState(690);
			switch ( getInterpreter().adaptivePredict(_input,36,_ctx) ) {
			case 1:
				{
				setState(685); match(KW_IS);
				setState(686); nullCondition();
				}
				break;

			case 2:
				{
				setState(687); expressionLike();
				}
				break;

			case 3:
				{
				setState(688); expressionBetween();
				}
				break;

			case 4:
				{
				setState(689); expressionIn();
				}
				break;
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionExistsContext extends ParserRuleContext {
		public TerminalNode KW_EXISTS() { return getToken(CQLParser.KW_EXISTS, 0); }
		public SubQueryExpressionContext subQueryExpression() {
			return getRuleContext(SubQueryExpressionContext.class,0);
		}
		public ExpressionExistsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionExists; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpressionExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpressionExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpressionExists(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionExistsContext expressionExists() throws RecognitionException {
		ExpressionExistsContext _localctx = new ExpressionExistsContext(_ctx, getState());
		enterRule(_localctx, 98, RULE_expressionExists);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(692); match(KW_EXISTS);
			setState(693); subQueryExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubQueryExpressionContext extends ParserRuleContext {
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public SelectStatementContext selectStatement() {
			return getRuleContext(SelectStatementContext.class,0);
		}
		public SubQueryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subQueryExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSubQueryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSubQueryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSubQueryExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubQueryExpressionContext subQueryExpression() throws RecognitionException {
		SubQueryExpressionContext _localctx = new SubQueryExpressionContext(_ctx, getState());
		enterRule(_localctx, 100, RULE_subQueryExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(695); match(LPAREN);
			setState(696); selectStatement();
			setState(697); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionBetweenContext extends ParserRuleContext {
		public TerminalNode KW_BETWEEN() { return getToken(CQLParser.KW_BETWEEN, 0); }
		public ExpressionBetweenMaxValueContext expressionBetweenMaxValue() {
			return getRuleContext(ExpressionBetweenMaxValueContext.class,0);
		}
		public IdentifierNotContext identifierNot() {
			return getRuleContext(IdentifierNotContext.class,0);
		}
		public ExpressionBetweenMinValueContext expressionBetweenMinValue() {
			return getRuleContext(ExpressionBetweenMinValueContext.class,0);
		}
		public TerminalNode KW_AND() { return getToken(CQLParser.KW_AND, 0); }
		public ExpressionBetweenContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionBetween; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpressionBetween(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpressionBetween(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpressionBetween(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionBetweenContext expressionBetween() throws RecognitionException {
		ExpressionBetweenContext _localctx = new ExpressionBetweenContext(_ctx, getState());
		enterRule(_localctx, 102, RULE_expressionBetween);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(700);
			_la = _input.LA(1);
			if (_la==KW_NOT) {
				{
				setState(699); identifierNot();
				}
			}

			setState(702); match(KW_BETWEEN);
			setState(703); expressionBetweenMinValue();
			setState(704); match(KW_AND);
			setState(705); expressionBetweenMaxValue();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BinaryExpressionContext extends ParserRuleContext {
		public RelationExpressionContext relationExpression(int i) {
			return getRuleContext(RelationExpressionContext.class,i);
		}
		public List<RelationExpressionContext> relationExpression() {
			return getRuleContexts(RelationExpressionContext.class);
		}
		public BitExpressionContext bitExpression() {
			return getRuleContext(BitExpressionContext.class,0);
		}
		public BinaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_binaryExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterBinaryExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitBinaryExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitBinaryExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BinaryExpressionContext binaryExpression() throws RecognitionException {
		BinaryExpressionContext _localctx = new BinaryExpressionContext(_ctx, getState());
		enterRule(_localctx, 104, RULE_binaryExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(707); bitExpression();
			setState(711);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & ((1L << (EQUAL - 129)) | (1L << (EQUAL_NS - 129)) | (1L << (NOTEQUAL - 129)) | (1L << (LESSTHANOREQUALTO - 129)) | (1L << (LESSTHAN - 129)) | (1L << (GREATERTHANOREQUALTO - 129)) | (1L << (GREATERTHAN - 129)))) != 0)) {
				{
				{
				setState(708); relationExpression();
				}
				}
				setState(713);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationExpressionContext extends ParserRuleContext {
		public RelationOperatorContext relationOperator() {
			return getRuleContext(RelationOperatorContext.class,0);
		}
		public BitExpressionContext bitExpression() {
			return getRuleContext(BitExpressionContext.class,0);
		}
		public RelationExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRelationExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRelationExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRelationExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationExpressionContext relationExpression() throws RecognitionException {
		RelationExpressionContext _localctx = new RelationExpressionContext(_ctx, getState());
		enterRule(_localctx, 106, RULE_relationExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(714); relationOperator();
			setState(715); bitExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationOperatorContext extends ParserRuleContext {
		public TerminalNode EQUAL() { return getToken(CQLParser.EQUAL, 0); }
		public TerminalNode GREATERTHAN() { return getToken(CQLParser.GREATERTHAN, 0); }
		public TerminalNode EQUAL_NS() { return getToken(CQLParser.EQUAL_NS, 0); }
		public TerminalNode GREATERTHANOREQUALTO() { return getToken(CQLParser.GREATERTHANOREQUALTO, 0); }
		public TerminalNode LESSTHAN() { return getToken(CQLParser.LESSTHAN, 0); }
		public TerminalNode NOTEQUAL() { return getToken(CQLParser.NOTEQUAL, 0); }
		public TerminalNode LESSTHANOREQUALTO() { return getToken(CQLParser.LESSTHANOREQUALTO, 0); }
		public RelationOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRelationOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRelationOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRelationOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationOperatorContext relationOperator() throws RecognitionException {
		RelationOperatorContext _localctx = new RelationOperatorContext(_ctx, getState());
		enterRule(_localctx, 108, RULE_relationOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(717);
			_la = _input.LA(1);
			if ( !(((((_la - 129)) & ~0x3f) == 0 && ((1L << (_la - 129)) & ((1L << (EQUAL - 129)) | (1L << (EQUAL_NS - 129)) | (1L << (NOTEQUAL - 129)) | (1L << (LESSTHANOREQUALTO - 129)) | (1L << (LESSTHAN - 129)) | (1L << (GREATERTHANOREQUALTO - 129)) | (1L << (GREATERTHAN - 129)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionPreviousContext extends ParserRuleContext {
		public TerminalNode KW_PREVIOUS() { return getToken(CQLParser.KW_PREVIOUS, 0); }
		public ExpressionsContext expressions() {
			return getRuleContext(ExpressionsContext.class,0);
		}
		public ExpressionPreviousContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionPrevious; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpressionPrevious(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpressionPrevious(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpressionPrevious(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionPreviousContext expressionPrevious() throws RecognitionException {
		ExpressionPreviousContext _localctx = new ExpressionPreviousContext(_ctx, getState());
		enterRule(_localctx, 110, RULE_expressionPrevious);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(719); match(KW_PREVIOUS);
			setState(720); expressions();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionInContext extends ParserRuleContext {
		public TerminalNode KW_IN() { return getToken(CQLParser.KW_IN, 0); }
		public ExpressionsContext expressions() {
			return getRuleContext(ExpressionsContext.class,0);
		}
		public IdentifierNotContext identifierNot() {
			return getRuleContext(IdentifierNotContext.class,0);
		}
		public ExpressionInContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionIn; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpressionIn(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpressionIn(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpressionIn(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionInContext expressionIn() throws RecognitionException {
		ExpressionInContext _localctx = new ExpressionInContext(_ctx, getState());
		enterRule(_localctx, 112, RULE_expressionIn);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(723);
			_la = _input.LA(1);
			if (_la==KW_NOT) {
				{
				setState(722); identifierNot();
				}
			}

			setState(725); match(KW_IN);
			setState(726); expressions();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionLikeContext extends ParserRuleContext {
		public PrecedenceEqualNegatableOperatorContext precedenceEqualNegatableOperator() {
			return getRuleContext(PrecedenceEqualNegatableOperatorContext.class,0);
		}
		public IdentifierNotContext identifierNot() {
			return getRuleContext(IdentifierNotContext.class,0);
		}
		public BitExpressionContext bitExpression() {
			return getRuleContext(BitExpressionContext.class,0);
		}
		public ExpressionLikeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionLike; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpressionLike(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpressionLike(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpressionLike(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionLikeContext expressionLike() throws RecognitionException {
		ExpressionLikeContext _localctx = new ExpressionLikeContext(_ctx, getState());
		enterRule(_localctx, 114, RULE_expressionLike);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(729);
			_la = _input.LA(1);
			if (_la==KW_NOT) {
				{
				setState(728); identifierNot();
				}
			}

			setState(731); precedenceEqualNegatableOperator();
			setState(732); bitExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PrecedenceEqualNegatableOperatorContext extends ParserRuleContext {
		public TerminalNode KW_RLIKE() { return getToken(CQLParser.KW_RLIKE, 0); }
		public TerminalNode KW_LIKE() { return getToken(CQLParser.KW_LIKE, 0); }
		public TerminalNode KW_REGEXP() { return getToken(CQLParser.KW_REGEXP, 0); }
		public PrecedenceEqualNegatableOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_precedenceEqualNegatableOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterPrecedenceEqualNegatableOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitPrecedenceEqualNegatableOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitPrecedenceEqualNegatableOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrecedenceEqualNegatableOperatorContext precedenceEqualNegatableOperator() throws RecognitionException {
		PrecedenceEqualNegatableOperatorContext _localctx = new PrecedenceEqualNegatableOperatorContext(_ctx, getState());
		enterRule(_localctx, 116, RULE_precedenceEqualNegatableOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(734);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << KW_LIKE) | (1L << KW_RLIKE) | (1L << KW_REGEXP))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionBetweenMinValueContext extends ParserRuleContext {
		public BitExpressionContext bitExpression() {
			return getRuleContext(BitExpressionContext.class,0);
		}
		public ExpressionBetweenMinValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionBetweenMinValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpressionBetweenMinValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpressionBetweenMinValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpressionBetweenMinValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionBetweenMinValueContext expressionBetweenMinValue() throws RecognitionException {
		ExpressionBetweenMinValueContext _localctx = new ExpressionBetweenMinValueContext(_ctx, getState());
		enterRule(_localctx, 118, RULE_expressionBetweenMinValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(736); bitExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionBetweenMaxValueContext extends ParserRuleContext {
		public BitExpressionContext bitExpression() {
			return getRuleContext(BitExpressionContext.class,0);
		}
		public ExpressionBetweenMaxValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionBetweenMaxValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpressionBetweenMaxValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpressionBetweenMaxValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpressionBetweenMaxValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionBetweenMaxValueContext expressionBetweenMaxValue() throws RecognitionException {
		ExpressionBetweenMaxValueContext _localctx = new ExpressionBetweenMaxValueContext(_ctx, getState());
		enterRule(_localctx, 120, RULE_expressionBetweenMaxValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(738); bitExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BitExpressionContext extends ParserRuleContext {
		public List<BitOperatorContext> bitOperator() {
			return getRuleContexts(BitOperatorContext.class);
		}
		public List<ArithmeticPlusMinusExpressionContext> arithmeticPlusMinusExpression() {
			return getRuleContexts(ArithmeticPlusMinusExpressionContext.class);
		}
		public ArithmeticPlusMinusExpressionContext arithmeticPlusMinusExpression(int i) {
			return getRuleContext(ArithmeticPlusMinusExpressionContext.class,i);
		}
		public BitOperatorContext bitOperator(int i) {
			return getRuleContext(BitOperatorContext.class,i);
		}
		public BitExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bitExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterBitExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitBitExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitBitExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BitExpressionContext bitExpression() throws RecognitionException {
		BitExpressionContext _localctx = new BitExpressionContext(_ctx, getState());
		enterRule(_localctx, 122, RULE_bitExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(740); arithmeticPlusMinusExpression();
			setState(746);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (((((_la - 144)) & ~0x3f) == 0 && ((1L << (_la - 144)) & ((1L << (BITWISEOR - 144)) | (1L << (AMPERSAND - 144)) | (1L << (BITWISEXOR - 144)))) != 0)) {
				{
				{
				setState(741); bitOperator();
				setState(742); arithmeticPlusMinusExpression();
				}
				}
				setState(748);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BitOperatorContext extends ParserRuleContext {
		public TerminalNode BITWISEOR() { return getToken(CQLParser.BITWISEOR, 0); }
		public TerminalNode BITWISEXOR() { return getToken(CQLParser.BITWISEXOR, 0); }
		public TerminalNode AMPERSAND() { return getToken(CQLParser.AMPERSAND, 0); }
		public BitOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bitOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterBitOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitBitOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitBitOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BitOperatorContext bitOperator() throws RecognitionException {
		BitOperatorContext _localctx = new BitOperatorContext(_ctx, getState());
		enterRule(_localctx, 124, RULE_bitOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(749);
			_la = _input.LA(1);
			if ( !(((((_la - 144)) & ~0x3f) == 0 && ((1L << (_la - 144)) & ((1L << (BITWISEOR - 144)) | (1L << (AMPERSAND - 144)) | (1L << (BITWISEXOR - 144)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticPlusMinusExpressionContext extends ParserRuleContext {
		public ArithmeticPlusOperatorContext arithmeticPlusOperator(int i) {
			return getRuleContext(ArithmeticPlusOperatorContext.class,i);
		}
		public ArithmeticStarExpressionContext arithmeticStarExpression(int i) {
			return getRuleContext(ArithmeticStarExpressionContext.class,i);
		}
		public List<ArithmeticStarExpressionContext> arithmeticStarExpression() {
			return getRuleContexts(ArithmeticStarExpressionContext.class);
		}
		public List<ArithmeticPlusOperatorContext> arithmeticPlusOperator() {
			return getRuleContexts(ArithmeticPlusOperatorContext.class);
		}
		public ArithmeticPlusMinusExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticPlusMinusExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterArithmeticPlusMinusExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitArithmeticPlusMinusExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitArithmeticPlusMinusExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticPlusMinusExpressionContext arithmeticPlusMinusExpression() throws RecognitionException {
		ArithmeticPlusMinusExpressionContext _localctx = new ArithmeticPlusMinusExpressionContext(_ctx, getState());
		enterRule(_localctx, 126, RULE_arithmeticPlusMinusExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(751); arithmeticStarExpression();
			setState(757);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & ((1L << (PLUS - 137)) | (1L << (MINUS - 137)) | (1L << (CONCATENATION - 137)))) != 0)) {
				{
				{
				setState(752); arithmeticPlusOperator();
				setState(753); arithmeticStarExpression();
				}
				}
				setState(759);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticPlusOperatorContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(CQLParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(CQLParser.MINUS, 0); }
		public TerminalNode CONCATENATION() { return getToken(CQLParser.CONCATENATION, 0); }
		public ArithmeticPlusOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticPlusOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterArithmeticPlusOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitArithmeticPlusOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitArithmeticPlusOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticPlusOperatorContext arithmeticPlusOperator() throws RecognitionException {
		ArithmeticPlusOperatorContext _localctx = new ArithmeticPlusOperatorContext(_ctx, getState());
		enterRule(_localctx, 128, RULE_arithmeticPlusOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(760);
			_la = _input.LA(1);
			if ( !(((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & ((1L << (PLUS - 137)) | (1L << (MINUS - 137)) | (1L << (CONCATENATION - 137)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticStarExpressionContext extends ParserRuleContext {
		public List<ArithmeticStarOperatorContext> arithmeticStarOperator() {
			return getRuleContexts(ArithmeticStarOperatorContext.class);
		}
		public List<FieldExpressionContext> fieldExpression() {
			return getRuleContexts(FieldExpressionContext.class);
		}
		public FieldExpressionContext fieldExpression(int i) {
			return getRuleContext(FieldExpressionContext.class,i);
		}
		public ArithmeticStarOperatorContext arithmeticStarOperator(int i) {
			return getRuleContext(ArithmeticStarOperatorContext.class,i);
		}
		public ArithmeticStarExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticStarExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterArithmeticStarExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitArithmeticStarExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitArithmeticStarExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticStarExpressionContext arithmeticStarExpression() throws RecognitionException {
		ArithmeticStarExpressionContext _localctx = new ArithmeticStarExpressionContext(_ctx, getState());
		enterRule(_localctx, 130, RULE_arithmeticStarExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(762); fieldExpression();
			setState(768);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (((((_la - 136)) & ~0x3f) == 0 && ((1L << (_la - 136)) & ((1L << (DIVIDE - 136)) | (1L << (STAR - 136)) | (1L << (MOD - 136)) | (1L << (DIV - 136)))) != 0)) {
				{
				{
				setState(763); arithmeticStarOperator();
				setState(764); fieldExpression();
				}
				}
				setState(770);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FieldExpressionContext extends ParserRuleContext {
		public TerminalNode DOT() { return getToken(CQLParser.DOT, 0); }
		public AtomExpressionContext atomExpression() {
			return getRuleContext(AtomExpressionContext.class,0);
		}
		public StreamNameOrAliasContext streamNameOrAlias() {
			return getRuleContext(StreamNameOrAliasContext.class,0);
		}
		public FieldExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fieldExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterFieldExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitFieldExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitFieldExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FieldExpressionContext fieldExpression() throws RecognitionException {
		FieldExpressionContext _localctx = new FieldExpressionContext(_ctx, getState());
		enterRule(_localctx, 132, RULE_fieldExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(774);
			switch ( getInterpreter().adaptivePredict(_input,44,_ctx) ) {
			case 1:
				{
				setState(771); streamNameOrAlias();
				setState(772); match(DOT);
				}
				break;
			}
			setState(776); atomExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArithmeticStarOperatorContext extends ParserRuleContext {
		public TerminalNode DIVIDE() { return getToken(CQLParser.DIVIDE, 0); }
		public TerminalNode STAR() { return getToken(CQLParser.STAR, 0); }
		public TerminalNode MOD() { return getToken(CQLParser.MOD, 0); }
		public TerminalNode DIV() { return getToken(CQLParser.DIV, 0); }
		public ArithmeticStarOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticStarOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterArithmeticStarOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitArithmeticStarOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitArithmeticStarOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticStarOperatorContext arithmeticStarOperator() throws RecognitionException {
		ArithmeticStarOperatorContext _localctx = new ArithmeticStarOperatorContext(_ctx, getState());
		enterRule(_localctx, 134, RULE_arithmeticStarOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(778);
			_la = _input.LA(1);
			if ( !(((((_la - 136)) & ~0x3f) == 0 && ((1L << (_la - 136)) & ((1L << (DIVIDE - 136)) | (1L << (STAR - 136)) | (1L << (MOD - 136)) | (1L << (DIV - 136)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AtomExpressionContext extends ParserRuleContext {
		public ConstantContext constant() {
			return getRuleContext(ConstantContext.class,0);
		}
		public CastExpressionContext castExpression() {
			return getRuleContext(CastExpressionContext.class,0);
		}
		public WhenExpressionContext whenExpression() {
			return getRuleContext(WhenExpressionContext.class,0);
		}
		public ExpressionWithLaparenContext expressionWithLaparen() {
			return getRuleContext(ExpressionWithLaparenContext.class,0);
		}
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public ConstNullContext constNull() {
			return getRuleContext(ConstNullContext.class,0);
		}
		public FunctionContext function() {
			return getRuleContext(FunctionContext.class,0);
		}
		public CaseExpressionContext caseExpression() {
			return getRuleContext(CaseExpressionContext.class,0);
		}
		public ExpressionPreviousContext expressionPrevious() {
			return getRuleContext(ExpressionPreviousContext.class,0);
		}
		public AtomExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_atomExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterAtomExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitAtomExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitAtomExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AtomExpressionContext atomExpression() throws RecognitionException {
		AtomExpressionContext _localctx = new AtomExpressionContext(_ctx, getState());
		enterRule(_localctx, 136, RULE_atomExpression);
		try {
			setState(789);
			switch ( getInterpreter().adaptivePredict(_input,45,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(780); constNull();
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(781); constant();
				}
				break;

			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(782); expressionPrevious();
				}
				break;

			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(783); function();
				}
				break;

			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(784); castExpression();
				}
				break;

			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(785); caseExpression();
				}
				break;

			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(786); whenExpression();
				}
				break;

			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(787); columnName();
				}
				break;

			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(788); expressionWithLaparen();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionWithLaparenContext extends ParserRuleContext {
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ExpressionWithLaparenContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressionWithLaparen; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpressionWithLaparen(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpressionWithLaparen(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpressionWithLaparen(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionWithLaparenContext expressionWithLaparen() throws RecognitionException {
		ExpressionWithLaparenContext _localctx = new ExpressionWithLaparenContext(_ctx, getState());
		enterRule(_localctx, 138, RULE_expressionWithLaparen);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(791); match(LPAREN);
			setState(792); expression();
			setState(793); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstantContext extends ParserRuleContext {
		public ConstLongValueContext constLongValue() {
			return getRuleContext(ConstLongValueContext.class,0);
		}
		public ConstStingValueContext constStingValue() {
			return getRuleContext(ConstStingValueContext.class,0);
		}
		public ConstFloatValueContext constFloatValue() {
			return getRuleContext(ConstFloatValueContext.class,0);
		}
		public ConstDoubleValueContext constDoubleValue() {
			return getRuleContext(ConstDoubleValueContext.class,0);
		}
		public UnaryOperatorContext unaryOperator() {
			return getRuleContext(UnaryOperatorContext.class,0);
		}
		public BooleanValueContext booleanValue() {
			return getRuleContext(BooleanValueContext.class,0);
		}
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public ConstBigDecimalValueContext constBigDecimalValue() {
			return getRuleContext(ConstBigDecimalValueContext.class,0);
		}
		public ConstantContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constant; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConstant(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConstant(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConstant(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstantContext constant() throws RecognitionException {
		ConstantContext _localctx = new ConstantContext(_ctx, getState());
		enterRule(_localctx, 140, RULE_constant);
		int _la;
		try {
			setState(807);
			switch (_input.LA(1)) {
			case PLUS:
			case MINUS:
			case TILDE:
			case LongLiteral:
			case FloatLiteral:
			case DoubleLiteral:
			case DecimalLiteral:
			case IntegerNumber:
				enterOuterAlt(_localctx, 1);
				{
				setState(796);
				_la = _input.LA(1);
				if (((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & ((1L << (PLUS - 137)) | (1L << (MINUS - 137)) | (1L << (TILDE - 137)))) != 0)) {
					{
					setState(795); unaryOperator();
					}
				}

				setState(803);
				switch (_input.LA(1)) {
				case IntegerNumber:
					{
					setState(798); constIntegerValue();
					}
					break;
				case LongLiteral:
					{
					setState(799); constLongValue();
					}
					break;
				case FloatLiteral:
					{
					setState(800); constFloatValue();
					}
					break;
				case DoubleLiteral:
					{
					setState(801); constDoubleValue();
					}
					break;
				case DecimalLiteral:
					{
					setState(802); constBigDecimalValue();
					}
					break;
				default:
					throw new NoViableAltException(this);
				}
				}
				break;
			case StringLiteral:
				enterOuterAlt(_localctx, 2);
				{
				setState(805); constStingValue();
				}
				break;
			case KW_FALSE:
			case KW_TRUE:
				enterOuterAlt(_localctx, 3);
				{
				setState(806); booleanValue();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstStingValueContext extends ParserRuleContext {
		public TerminalNode StringLiteral() { return getToken(CQLParser.StringLiteral, 0); }
		public ConstStingValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constStingValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConstStingValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConstStingValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConstStingValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstStingValueContext constStingValue() throws RecognitionException {
		ConstStingValueContext _localctx = new ConstStingValueContext(_ctx, getState());
		enterRule(_localctx, 142, RULE_constStingValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(809); match(StringLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstIntegerValueContext extends ParserRuleContext {
		public TerminalNode IntegerNumber() { return getToken(CQLParser.IntegerNumber, 0); }
		public ConstIntegerValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constIntegerValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConstIntegerValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConstIntegerValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConstIntegerValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstIntegerValueContext constIntegerValue() throws RecognitionException {
		ConstIntegerValueContext _localctx = new ConstIntegerValueContext(_ctx, getState());
		enterRule(_localctx, 144, RULE_constIntegerValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(811); match(IntegerNumber);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstLongValueContext extends ParserRuleContext {
		public TerminalNode LongLiteral() { return getToken(CQLParser.LongLiteral, 0); }
		public ConstLongValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constLongValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConstLongValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConstLongValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConstLongValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstLongValueContext constLongValue() throws RecognitionException {
		ConstLongValueContext _localctx = new ConstLongValueContext(_ctx, getState());
		enterRule(_localctx, 146, RULE_constLongValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(813); match(LongLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstFloatValueContext extends ParserRuleContext {
		public TerminalNode FloatLiteral() { return getToken(CQLParser.FloatLiteral, 0); }
		public ConstFloatValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constFloatValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConstFloatValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConstFloatValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConstFloatValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstFloatValueContext constFloatValue() throws RecognitionException {
		ConstFloatValueContext _localctx = new ConstFloatValueContext(_ctx, getState());
		enterRule(_localctx, 148, RULE_constFloatValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(815); match(FloatLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstDoubleValueContext extends ParserRuleContext {
		public TerminalNode DoubleLiteral() { return getToken(CQLParser.DoubleLiteral, 0); }
		public ConstDoubleValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constDoubleValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConstDoubleValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConstDoubleValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConstDoubleValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstDoubleValueContext constDoubleValue() throws RecognitionException {
		ConstDoubleValueContext _localctx = new ConstDoubleValueContext(_ctx, getState());
		enterRule(_localctx, 150, RULE_constDoubleValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(817); match(DoubleLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstBigDecimalValueContext extends ParserRuleContext {
		public TerminalNode DecimalLiteral() { return getToken(CQLParser.DecimalLiteral, 0); }
		public ConstBigDecimalValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constBigDecimalValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConstBigDecimalValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConstBigDecimalValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConstBigDecimalValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstBigDecimalValueContext constBigDecimalValue() throws RecognitionException {
		ConstBigDecimalValueContext _localctx = new ConstBigDecimalValueContext(_ctx, getState());
		enterRule(_localctx, 152, RULE_constBigDecimalValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(819); match(DecimalLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionContext extends ParserRuleContext {
		public FunctionNameContext functionName() {
			return getRuleContext(FunctionNameContext.class,0);
		}
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public DistinctContext distinct() {
			return getRuleContext(DistinctContext.class,0);
		}
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public List<SelectExpressionContext> selectExpression() {
			return getRuleContexts(SelectExpressionContext.class);
		}
		public SelectExpressionContext selectExpression(int i) {
			return getRuleContext(SelectExpressionContext.class,i);
		}
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public FunctionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_function; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterFunction(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitFunction(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitFunction(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionContext function() throws RecognitionException {
		FunctionContext _localctx = new FunctionContext(_ctx, getState());
		enterRule(_localctx, 154, RULE_function);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(821); functionName();
			setState(822); match(LPAREN);
			setState(824);
			_la = _input.LA(1);
			if (_la==KW_DISTINCT) {
				{
				setState(823); distinct();
				}
			}

			setState(834);
			_la = _input.LA(1);
			if (((((_la - 31)) & ~0x3f) == 0 && ((1L << (_la - 31)) & ((1L << (KW_CASE - 31)) | (1L << (KW_CAST - 31)) | (1L << (KW_EXISTS - 31)) | (1L << (KW_FALSE - 31)) | (1L << (KW_NULL - 31)) | (1L << (KW_TRUE - 31)) | (1L << (KW_NOT - 31)) | (1L << (KW_PREVIOUS - 31)))) != 0) || ((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (KW_OPERATOR - 101)) | (1L << (KW_DAY - 101)) | (1L << (KW_HOUR - 101)) | (1L << (KW_MINUTES - 101)) | (1L << (KW_SECONDS - 101)) | (1L << (KW_MILLISECONDS - 101)) | (1L << (KW_BOOLEAN - 101)) | (1L << (KW_INT - 101)) | (1L << (KW_LONG - 101)) | (1L << (KW_FLOAT - 101)) | (1L << (KW_DOUBLE - 101)) | (1L << (KW_STRING - 101)) | (1L << (KW_TIMESTAMP - 101)) | (1L << (KW_DATE - 101)) | (1L << (KW_TIME - 101)) | (1L << (KW_DECIMAL - 101)) | (1L << (LPAREN - 101)) | (1L << (PLUS - 101)) | (1L << (MINUS - 101)) | (1L << (STAR - 101)) | (1L << (TILDE - 101)) | (1L << (LongLiteral - 101)) | (1L << (FloatLiteral - 101)) | (1L << (DoubleLiteral - 101)) | (1L << (DecimalLiteral - 101)) | (1L << (StringLiteral - 101)) | (1L << (IntegerNumber - 101)) | (1L << (Identifier - 101)))) != 0)) {
				{
				setState(826); selectExpression();
				setState(831);
				_errHandler.sync(this);
				_la = _input.LA(1);
				while (_la==COMMA) {
					{
					{
					setState(827); match(COMMA);
					setState(828); selectExpression();
					}
					}
					setState(833);
					_errHandler.sync(this);
					_la = _input.LA(1);
				}
				}
			}

			setState(836); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CastExpressionContext extends ParserRuleContext {
		public TerminalNode KW_AS() { return getToken(CQLParser.KW_AS, 0); }
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public PrimitiveTypeContext primitiveType() {
			return getRuleContext(PrimitiveTypeContext.class,0);
		}
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public TerminalNode KW_CAST() { return getToken(CQLParser.KW_CAST, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public CastExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_castExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCastExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCastExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCastExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CastExpressionContext castExpression() throws RecognitionException {
		CastExpressionContext _localctx = new CastExpressionContext(_ctx, getState());
		enterRule(_localctx, 156, RULE_castExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(838); match(KW_CAST);
			setState(839); match(LPAREN);
			setState(840); expression();
			setState(841); match(KW_AS);
			setState(842); primitiveType();
			setState(843); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CaseExpressionContext extends ParserRuleContext {
		public List<CaseWhenBodyWhenBodyContext> caseWhenBodyWhenBody() {
			return getRuleContexts(CaseWhenBodyWhenBodyContext.class);
		}
		public CaseWhenBodyWhenBodyContext caseWhenBodyWhenBody(int i) {
			return getRuleContext(CaseWhenBodyWhenBodyContext.class,i);
		}
		public List<CaseWhenBodyThenBodyContext> caseWhenBodyThenBody() {
			return getRuleContexts(CaseWhenBodyThenBodyContext.class);
		}
		public TerminalNode KW_CASE() { return getToken(CQLParser.KW_CASE, 0); }
		public TerminalNode KW_END() { return getToken(CQLParser.KW_END, 0); }
		public CaseWhenBodyThenBodyContext caseWhenBodyThenBody(int i) {
			return getRuleContext(CaseWhenBodyThenBodyContext.class,i);
		}
		public CaseWhenElseContext caseWhenElse() {
			return getRuleContext(CaseWhenElseContext.class,0);
		}
		public CaseHeadExpressionContext caseHeadExpression() {
			return getRuleContext(CaseHeadExpressionContext.class,0);
		}
		public CaseExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCaseExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCaseExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCaseExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CaseExpressionContext caseExpression() throws RecognitionException {
		CaseExpressionContext _localctx = new CaseExpressionContext(_ctx, getState());
		enterRule(_localctx, 158, RULE_caseExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(845); match(KW_CASE);
			setState(846); caseHeadExpression();
			setState(850); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(847); caseWhenBodyWhenBody();
				setState(848); caseWhenBodyThenBody();
				}
				}
				setState(852); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==KW_WHEN );
			setState(855);
			_la = _input.LA(1);
			if (_la==KW_ELSE) {
				{
				setState(854); caseWhenElse();
				}
			}

			setState(857); match(KW_END);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhenExpressionContext extends ParserRuleContext {
		public List<CaseWhenBodyWhenBodyContext> caseWhenBodyWhenBody() {
			return getRuleContexts(CaseWhenBodyWhenBodyContext.class);
		}
		public CaseWhenBodyWhenBodyContext caseWhenBodyWhenBody(int i) {
			return getRuleContext(CaseWhenBodyWhenBodyContext.class,i);
		}
		public List<CaseWhenBodyThenBodyContext> caseWhenBodyThenBody() {
			return getRuleContexts(CaseWhenBodyThenBodyContext.class);
		}
		public TerminalNode KW_CASE() { return getToken(CQLParser.KW_CASE, 0); }
		public TerminalNode KW_END() { return getToken(CQLParser.KW_END, 0); }
		public CaseWhenBodyThenBodyContext caseWhenBodyThenBody(int i) {
			return getRuleContext(CaseWhenBodyThenBodyContext.class,i);
		}
		public CaseWhenElseContext caseWhenElse() {
			return getRuleContext(CaseWhenElseContext.class,0);
		}
		public WhenExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whenExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterWhenExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitWhenExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitWhenExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhenExpressionContext whenExpression() throws RecognitionException {
		WhenExpressionContext _localctx = new WhenExpressionContext(_ctx, getState());
		enterRule(_localctx, 160, RULE_whenExpression);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(859); match(KW_CASE);
			setState(863); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(860); caseWhenBodyWhenBody();
				setState(861); caseWhenBodyThenBody();
				}
				}
				setState(865); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==KW_WHEN );
			setState(868);
			_la = _input.LA(1);
			if (_la==KW_ELSE) {
				{
				setState(867); caseWhenElse();
				}
			}

			setState(870); match(KW_END);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CaseHeadExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public CaseHeadExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseHeadExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCaseHeadExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCaseHeadExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCaseHeadExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CaseHeadExpressionContext caseHeadExpression() throws RecognitionException {
		CaseHeadExpressionContext _localctx = new CaseHeadExpressionContext(_ctx, getState());
		enterRule(_localctx, 162, RULE_caseHeadExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(872); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CaseWhenBodyWhenBodyContext extends ParserRuleContext {
		public TerminalNode KW_WHEN() { return getToken(CQLParser.KW_WHEN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public CaseWhenBodyWhenBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseWhenBodyWhenBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCaseWhenBodyWhenBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCaseWhenBodyWhenBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCaseWhenBodyWhenBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CaseWhenBodyWhenBodyContext caseWhenBodyWhenBody() throws RecognitionException {
		CaseWhenBodyWhenBodyContext _localctx = new CaseWhenBodyWhenBodyContext(_ctx, getState());
		enterRule(_localctx, 164, RULE_caseWhenBodyWhenBody);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(874); match(KW_WHEN);
			setState(875); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CaseWhenBodyThenBodyContext extends ParserRuleContext {
		public TerminalNode KW_THEN() { return getToken(CQLParser.KW_THEN, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public CaseWhenBodyThenBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseWhenBodyThenBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCaseWhenBodyThenBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCaseWhenBodyThenBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCaseWhenBodyThenBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CaseWhenBodyThenBodyContext caseWhenBodyThenBody() throws RecognitionException {
		CaseWhenBodyThenBodyContext _localctx = new CaseWhenBodyThenBodyContext(_ctx, getState());
		enterRule(_localctx, 166, RULE_caseWhenBodyThenBody);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(877); match(KW_THEN);
			setState(878); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CaseWhenElseContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode KW_ELSE() { return getToken(CQLParser.KW_ELSE, 0); }
		public CaseWhenElseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_caseWhenElse; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCaseWhenElse(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCaseWhenElse(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCaseWhenElse(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CaseWhenElseContext caseWhenElse() throws RecognitionException {
		CaseWhenElseContext _localctx = new CaseWhenElseContext(_ctx, getState());
		enterRule(_localctx, 168, RULE_caseWhenElse);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(880); match(KW_ELSE);
			setState(881); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BooleanValueContext extends ParserRuleContext {
		public TerminalNode KW_TRUE() { return getToken(CQLParser.KW_TRUE, 0); }
		public TerminalNode KW_FALSE() { return getToken(CQLParser.KW_FALSE, 0); }
		public BooleanValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_booleanValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterBooleanValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitBooleanValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitBooleanValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final BooleanValueContext booleanValue() throws RecognitionException {
		BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
		enterRule(_localctx, 170, RULE_booleanValue);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(883);
			_la = _input.LA(1);
			if ( !(_la==KW_FALSE || _la==KW_TRUE) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionsContext extends ParserRuleContext {
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public ExpressionsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expressions; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExpressions(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExpressions(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExpressions(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionsContext expressions() throws RecognitionException {
		ExpressionsContext _localctx = new ExpressionsContext(_ctx, getState());
		enterRule(_localctx, 172, RULE_expressions);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(885); match(LPAREN);
			setState(886); expression();
			setState(891);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(887); match(COMMA);
				setState(888); expression();
				}
				}
				setState(893);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(894); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertStatementContext extends ParserRuleContext {
		public SelectStatementContext selectStatement() {
			return getRuleContext(SelectStatementContext.class,0);
		}
		public InsertClauseContext insertClause() {
			return getRuleContext(InsertClauseContext.class,0);
		}
		public InsertStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterInsertStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitInsertStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitInsertStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertStatementContext insertStatement() throws RecognitionException {
		InsertStatementContext _localctx = new InsertStatementContext(_ctx, getState());
		enterRule(_localctx, 174, RULE_insertStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(896); insertClause();
			setState(897); selectStatement();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertUserOperatorStatementContext extends ParserRuleContext {
		public UsingStatementContext usingStatement() {
			return getRuleContext(UsingStatementContext.class,0);
		}
		public InsertClauseContext insertClause() {
			return getRuleContext(InsertClauseContext.class,0);
		}
		public InsertUserOperatorStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertUserOperatorStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterInsertUserOperatorStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitInsertUserOperatorStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitInsertUserOperatorStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertUserOperatorStatementContext insertUserOperatorStatement() throws RecognitionException {
		InsertUserOperatorStatementContext _localctx = new InsertUserOperatorStatementContext(_ctx, getState());
		enterRule(_localctx, 176, RULE_insertUserOperatorStatement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(899); insertClause();
			setState(900); usingStatement();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UsingStatementContext extends ParserRuleContext {
		public TerminalNode KW_OPERATOR() { return getToken(CQLParser.KW_OPERATOR, 0); }
		public ParallelClauseContext parallelClause() {
			return getRuleContext(ParallelClauseContext.class,0);
		}
		public DistributeClauseContext distributeClause() {
			return getRuleContext(DistributeClauseContext.class,0);
		}
		public OperatorNameContext operatorName() {
			return getRuleContext(OperatorNameContext.class,0);
		}
		public TerminalNode KW_FROM() { return getToken(CQLParser.KW_FROM, 0); }
		public StreamNameContext streamName() {
			return getRuleContext(StreamNameContext.class,0);
		}
		public TerminalNode KW_USING() { return getToken(CQLParser.KW_USING, 0); }
		public UsingStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_usingStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterUsingStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitUsingStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitUsingStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UsingStatementContext usingStatement() throws RecognitionException {
		UsingStatementContext _localctx = new UsingStatementContext(_ctx, getState());
		enterRule(_localctx, 178, RULE_usingStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(902); match(KW_USING);
			setState(903); match(KW_OPERATOR);
			setState(904); operatorName();
			setState(905); match(KW_FROM);
			setState(906); streamName();
			setState(908);
			_la = _input.LA(1);
			if (_la==KW_DISTRIBUTE) {
				{
				setState(907); distributeClause();
				}
			}

			setState(911);
			_la = _input.LA(1);
			if (_la==KW_PARALLEL) {
				{
				setState(910); parallelClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DistributeClauseContext extends ParserRuleContext {
		public TerminalNode KW_BY() { return getToken(CQLParser.KW_BY, 0); }
		public TerminalNode KW_DISTRIBUTE() { return getToken(CQLParser.KW_DISTRIBUTE, 0); }
		public ColumnNameContext columnName() {
			return getRuleContext(ColumnNameContext.class,0);
		}
		public DistributeClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_distributeClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDistributeClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDistributeClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDistributeClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DistributeClauseContext distributeClause() throws RecognitionException {
		DistributeClauseContext _localctx = new DistributeClauseContext(_ctx, getState());
		enterRule(_localctx, 180, RULE_distributeClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(913); match(KW_DISTRIBUTE);
			setState(914); match(KW_BY);
			setState(915); columnName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiInsertStatementContext extends ParserRuleContext {
		public MultiInsertContext multiInsert(int i) {
			return getRuleContext(MultiInsertContext.class,i);
		}
		public ParallelClauseContext parallelClause() {
			return getRuleContext(ParallelClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public List<MultiInsertContext> multiInsert() {
			return getRuleContexts(MultiInsertContext.class);
		}
		public MultiInsertStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiInsertStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterMultiInsertStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitMultiInsertStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitMultiInsertStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiInsertStatementContext multiInsertStatement() throws RecognitionException {
		MultiInsertStatementContext _localctx = new MultiInsertStatementContext(_ctx, getState());
		enterRule(_localctx, 182, RULE_multiInsertStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(917); fromClause();
			setState(919); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(918); multiInsert();
				}
				}
				setState(921); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==KW_INSERT );
			setState(924);
			_la = _input.LA(1);
			if (_la==KW_PARALLEL) {
				{
				setState(923); parallelClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectStatementContext extends ParserRuleContext {
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public ParallelClauseContext parallelClause() {
			return getRuleContext(ParallelClauseContext.class,0);
		}
		public OrderByClauseContext orderByClause() {
			return getRuleContext(OrderByClauseContext.class,0);
		}
		public LimitClauseContext limitClause() {
			return getRuleContext(LimitClauseContext.class,0);
		}
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public FromClauseContext fromClause() {
			return getRuleContext(FromClauseContext.class,0);
		}
		public GroupByClauseContext groupByClause() {
			return getRuleContext(GroupByClauseContext.class,0);
		}
		public SelectStatementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectStatement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSelectStatement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSelectStatement(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSelectStatement(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectStatementContext selectStatement() throws RecognitionException {
		SelectStatementContext _localctx = new SelectStatementContext(_ctx, getState());
		enterRule(_localctx, 184, RULE_selectStatement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(926); selectClause();
			setState(927); fromClause();
			setState(929);
			_la = _input.LA(1);
			if (_la==KW_WHERE) {
				{
				setState(928); whereClause();
				}
			}

			setState(932);
			_la = _input.LA(1);
			if (_la==KW_GROUP) {
				{
				setState(931); groupByClause();
				}
			}

			setState(935);
			_la = _input.LA(1);
			if (_la==KW_HAVING) {
				{
				setState(934); havingClause();
				}
			}

			setState(938);
			_la = _input.LA(1);
			if (_la==KW_ORDER) {
				{
				setState(937); orderByClause();
				}
			}

			setState(941);
			_la = _input.LA(1);
			if (_la==KW_LIMIT) {
				{
				setState(940); limitClause();
				}
			}

			setState(944);
			_la = _input.LA(1);
			if (_la==KW_PARALLEL) {
				{
				setState(943); parallelClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiInsertContext extends ParserRuleContext {
		public InsertClauseContext insertClause() {
			return getRuleContext(InsertClauseContext.class,0);
		}
		public MultiSelectContext multiSelect() {
			return getRuleContext(MultiSelectContext.class,0);
		}
		public MultiInsertContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiInsert; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterMultiInsert(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitMultiInsert(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitMultiInsert(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiInsertContext multiInsert() throws RecognitionException {
		MultiInsertContext _localctx = new MultiInsertContext(_ctx, getState());
		enterRule(_localctx, 186, RULE_multiInsert);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(946); insertClause();
			setState(947); multiSelect();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultiSelectContext extends ParserRuleContext {
		public HavingClauseContext havingClause() {
			return getRuleContext(HavingClauseContext.class,0);
		}
		public OrderByClauseContext orderByClause() {
			return getRuleContext(OrderByClauseContext.class,0);
		}
		public LimitClauseContext limitClause() {
			return getRuleContext(LimitClauseContext.class,0);
		}
		public SelectClauseContext selectClause() {
			return getRuleContext(SelectClauseContext.class,0);
		}
		public WhereClauseContext whereClause() {
			return getRuleContext(WhereClauseContext.class,0);
		}
		public GroupByClauseContext groupByClause() {
			return getRuleContext(GroupByClauseContext.class,0);
		}
		public MultiSelectContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multiSelect; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterMultiSelect(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitMultiSelect(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitMultiSelect(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultiSelectContext multiSelect() throws RecognitionException {
		MultiSelectContext _localctx = new MultiSelectContext(_ctx, getState());
		enterRule(_localctx, 188, RULE_multiSelect);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(949); selectClause();
			setState(951);
			_la = _input.LA(1);
			if (_la==KW_WHERE) {
				{
				setState(950); whereClause();
				}
			}

			setState(954);
			_la = _input.LA(1);
			if (_la==KW_GROUP) {
				{
				setState(953); groupByClause();
				}
			}

			setState(957);
			_la = _input.LA(1);
			if (_la==KW_HAVING) {
				{
				setState(956); havingClause();
				}
			}

			setState(960);
			_la = _input.LA(1);
			if (_la==KW_ORDER) {
				{
				setState(959); orderByClause();
				}
			}

			setState(963);
			_la = _input.LA(1);
			if (_la==KW_LIMIT) {
				{
				setState(962); limitClause();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ParallelClauseContext extends ParserRuleContext {
		public TerminalNode KW_PARALLEL() { return getToken(CQLParser.KW_PARALLEL, 0); }
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public ParallelClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_parallelClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterParallelClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitParallelClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitParallelClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ParallelClauseContext parallelClause() throws RecognitionException {
		ParallelClauseContext _localctx = new ParallelClauseContext(_ctx, getState());
		enterRule(_localctx, 190, RULE_parallelClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(965); match(KW_PARALLEL);
			setState(966); constIntegerValue();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InsertClauseContext extends ParserRuleContext {
		public TerminalNode KW_INTO() { return getToken(CQLParser.KW_INTO, 0); }
		public TerminalNode KW_STREAM() { return getToken(CQLParser.KW_STREAM, 0); }
		public TerminalNode KW_INSERT() { return getToken(CQLParser.KW_INSERT, 0); }
		public StreamNameContext streamName() {
			return getRuleContext(StreamNameContext.class,0);
		}
		public InsertClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_insertClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterInsertClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitInsertClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitInsertClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InsertClauseContext insertClause() throws RecognitionException {
		InsertClauseContext _localctx = new InsertClauseContext(_ctx, getState());
		enterRule(_localctx, 192, RULE_insertClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(968); match(KW_INSERT);
			setState(969); match(KW_INTO);
			setState(971);
			_la = _input.LA(1);
			if (_la==KW_STREAM) {
				{
				setState(970); match(KW_STREAM);
				}
			}

			setState(973); streamName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromClauseContext extends ParserRuleContext {
		public JoinSourceContext joinSource() {
			return getRuleContext(JoinSourceContext.class,0);
		}
		public TerminalNode KW_FROM() { return getToken(CQLParser.KW_FROM, 0); }
		public CombineConditionContext combineCondition() {
			return getRuleContext(CombineConditionContext.class,0);
		}
		public FromClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterFromClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitFromClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitFromClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromClauseContext fromClause() throws RecognitionException {
		FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
		enterRule(_localctx, 194, RULE_fromClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(975); match(KW_FROM);
			setState(976); joinSource();
			setState(978);
			_la = _input.LA(1);
			if (_la==KW_COMBINE) {
				{
				setState(977); combineCondition();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinSourceContext extends ParserRuleContext {
		public List<JoinRigthBodyContext> joinRigthBody() {
			return getRuleContexts(JoinRigthBodyContext.class);
		}
		public JoinRigthBodyContext joinRigthBody(int i) {
			return getRuleContext(JoinRigthBodyContext.class,i);
		}
		public FromSourceContext fromSource() {
			return getRuleContext(FromSourceContext.class,0);
		}
		public JoinSourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinSource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterJoinSource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitJoinSource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitJoinSource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinSourceContext joinSource() throws RecognitionException {
		JoinSourceContext _localctx = new JoinSourceContext(_ctx, getState());
		enterRule(_localctx, 196, RULE_joinSource);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(980); fromSource();
			setState(984);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << KW_JOIN) | (1L << KW_FULL) | (1L << KW_CROSS) | (1L << KW_LEFT) | (1L << KW_INNER))) != 0) || ((((_la - 64)) & ~0x3f) == 0 && ((1L << (_la - 64)) & ((1L << (KW_NATURAL - 64)) | (1L << (KW_RIGHT - 64)) | (1L << (COMMA - 64)))) != 0)) {
				{
				{
				setState(981); joinRigthBody();
				}
				}
				setState(986);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinRigthBodyContext extends ParserRuleContext {
		public JoinTokenContext joinToken() {
			return getRuleContext(JoinTokenContext.class,0);
		}
		public FromSourceContext fromSource() {
			return getRuleContext(FromSourceContext.class,0);
		}
		public OnConditionContext onCondition() {
			return getRuleContext(OnConditionContext.class,0);
		}
		public JoinRigthBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinRigthBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterJoinRigthBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitJoinRigthBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitJoinRigthBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinRigthBodyContext joinRigthBody() throws RecognitionException {
		JoinRigthBodyContext _localctx = new JoinRigthBodyContext(_ctx, getState());
		enterRule(_localctx, 198, RULE_joinRigthBody);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(987); joinToken();
			setState(988); fromSource();
			setState(990);
			_la = _input.LA(1);
			if (_la==KW_ON) {
				{
				setState(989); onCondition();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OnConditionContext extends ParserRuleContext {
		public TerminalNode KW_ON() { return getToken(CQLParser.KW_ON, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public OnConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_onCondition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterOnCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitOnCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitOnCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OnConditionContext onCondition() throws RecognitionException {
		OnConditionContext _localctx = new OnConditionContext(_ctx, getState());
		enterRule(_localctx, 200, RULE_onCondition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(992); match(KW_ON);
			setState(993); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CombineConditionContext extends ParserRuleContext {
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public TerminalNode KW_COMBINE() { return getToken(CQLParser.KW_COMBINE, 0); }
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public CombineConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_combineCondition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCombineCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCombineCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCombineCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CombineConditionContext combineCondition() throws RecognitionException {
		CombineConditionContext _localctx = new CombineConditionContext(_ctx, getState());
		enterRule(_localctx, 202, RULE_combineCondition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(995); match(KW_COMBINE);
			setState(996); match(LPAREN);
			setState(997); expression();
			setState(1000); 
			_errHandler.sync(this);
			_la = _input.LA(1);
			do {
				{
				{
				setState(998); match(COMMA);
				setState(999); expression();
				}
				}
				setState(1002); 
				_errHandler.sync(this);
				_la = _input.LA(1);
			} while ( _la==COMMA );
			setState(1004); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class JoinTokenContext extends ParserRuleContext {
		public RightJoinContext rightJoin() {
			return getRuleContext(RightJoinContext.class,0);
		}
		public InnerJoinContext innerJoin() {
			return getRuleContext(InnerJoinContext.class,0);
		}
		public CrossJoinContext crossJoin() {
			return getRuleContext(CrossJoinContext.class,0);
		}
		public LeftJoinContext leftJoin() {
			return getRuleContext(LeftJoinContext.class,0);
		}
		public FullJoinContext fullJoin() {
			return getRuleContext(FullJoinContext.class,0);
		}
		public NaturalJoinContext naturalJoin() {
			return getRuleContext(NaturalJoinContext.class,0);
		}
		public JoinTokenContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_joinToken; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterJoinToken(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitJoinToken(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitJoinToken(this);
			else return visitor.visitChildren(this);
		}
	}

	public final JoinTokenContext joinToken() throws RecognitionException {
		JoinTokenContext _localctx = new JoinTokenContext(_ctx, getState());
		enterRule(_localctx, 204, RULE_joinToken);
		try {
			setState(1012);
			switch (_input.LA(1)) {
			case KW_JOIN:
			case KW_INNER:
				enterOuterAlt(_localctx, 1);
				{
				setState(1006); innerJoin();
				}
				break;
			case KW_LEFT:
				enterOuterAlt(_localctx, 2);
				{
				setState(1007); leftJoin();
				}
				break;
			case KW_RIGHT:
				enterOuterAlt(_localctx, 3);
				{
				setState(1008); rightJoin();
				}
				break;
			case KW_FULL:
				enterOuterAlt(_localctx, 4);
				{
				setState(1009); fullJoin();
				}
				break;
			case KW_CROSS:
			case COMMA:
				enterOuterAlt(_localctx, 5);
				{
				setState(1010); crossJoin();
				}
				break;
			case KW_NATURAL:
				enterOuterAlt(_localctx, 6);
				{
				setState(1011); naturalJoin();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InnerJoinContext extends ParserRuleContext {
		public TerminalNode KW_JOIN() { return getToken(CQLParser.KW_JOIN, 0); }
		public TerminalNode KW_INNER() { return getToken(CQLParser.KW_INNER, 0); }
		public InnerJoinContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_innerJoin; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterInnerJoin(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitInnerJoin(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitInnerJoin(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InnerJoinContext innerJoin() throws RecognitionException {
		InnerJoinContext _localctx = new InnerJoinContext(_ctx, getState());
		enterRule(_localctx, 206, RULE_innerJoin);
		try {
			setState(1017);
			switch (_input.LA(1)) {
			case KW_JOIN:
				enterOuterAlt(_localctx, 1);
				{
				setState(1014); match(KW_JOIN);
				}
				break;
			case KW_INNER:
				enterOuterAlt(_localctx, 2);
				{
				setState(1015); match(KW_INNER);
				setState(1016); match(KW_JOIN);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LeftJoinContext extends ParserRuleContext {
		public TerminalNode KW_LEFT() { return getToken(CQLParser.KW_LEFT, 0); }
		public TerminalNode KW_JOIN() { return getToken(CQLParser.KW_JOIN, 0); }
		public TerminalNode KW_OUTER() { return getToken(CQLParser.KW_OUTER, 0); }
		public LeftJoinContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_leftJoin; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterLeftJoin(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitLeftJoin(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitLeftJoin(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LeftJoinContext leftJoin() throws RecognitionException {
		LeftJoinContext _localctx = new LeftJoinContext(_ctx, getState());
		enterRule(_localctx, 208, RULE_leftJoin);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1019); match(KW_LEFT);
			setState(1021);
			_la = _input.LA(1);
			if (_la==KW_OUTER) {
				{
				setState(1020); match(KW_OUTER);
				}
			}

			setState(1023); match(KW_JOIN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RightJoinContext extends ParserRuleContext {
		public TerminalNode KW_JOIN() { return getToken(CQLParser.KW_JOIN, 0); }
		public TerminalNode KW_RIGHT() { return getToken(CQLParser.KW_RIGHT, 0); }
		public TerminalNode KW_OUTER() { return getToken(CQLParser.KW_OUTER, 0); }
		public RightJoinContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rightJoin; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRightJoin(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRightJoin(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRightJoin(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RightJoinContext rightJoin() throws RecognitionException {
		RightJoinContext _localctx = new RightJoinContext(_ctx, getState());
		enterRule(_localctx, 210, RULE_rightJoin);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1025); match(KW_RIGHT);
			setState(1027);
			_la = _input.LA(1);
			if (_la==KW_OUTER) {
				{
				setState(1026); match(KW_OUTER);
				}
			}

			setState(1029); match(KW_JOIN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FullJoinContext extends ParserRuleContext {
		public TerminalNode KW_FULL() { return getToken(CQLParser.KW_FULL, 0); }
		public TerminalNode KW_JOIN() { return getToken(CQLParser.KW_JOIN, 0); }
		public TerminalNode KW_OUTER() { return getToken(CQLParser.KW_OUTER, 0); }
		public FullJoinContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fullJoin; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterFullJoin(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitFullJoin(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitFullJoin(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FullJoinContext fullJoin() throws RecognitionException {
		FullJoinContext _localctx = new FullJoinContext(_ctx, getState());
		enterRule(_localctx, 212, RULE_fullJoin);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1031); match(KW_FULL);
			setState(1033);
			_la = _input.LA(1);
			if (_la==KW_OUTER) {
				{
				setState(1032); match(KW_OUTER);
				}
			}

			setState(1035); match(KW_JOIN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CrossJoinContext extends ParserRuleContext {
		public TerminalNode COMMA() { return getToken(CQLParser.COMMA, 0); }
		public TerminalNode KW_JOIN() { return getToken(CQLParser.KW_JOIN, 0); }
		public TerminalNode KW_CROSS() { return getToken(CQLParser.KW_CROSS, 0); }
		public CrossJoinContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_crossJoin; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCrossJoin(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCrossJoin(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCrossJoin(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CrossJoinContext crossJoin() throws RecognitionException {
		CrossJoinContext _localctx = new CrossJoinContext(_ctx, getState());
		enterRule(_localctx, 214, RULE_crossJoin);
		try {
			setState(1040);
			switch (_input.LA(1)) {
			case COMMA:
				enterOuterAlt(_localctx, 1);
				{
				setState(1037); match(COMMA);
				}
				break;
			case KW_CROSS:
				enterOuterAlt(_localctx, 2);
				{
				setState(1038); match(KW_CROSS);
				setState(1039); match(KW_JOIN);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NaturalJoinContext extends ParserRuleContext {
		public TerminalNode KW_NATURAL() { return getToken(CQLParser.KW_NATURAL, 0); }
		public TerminalNode KW_JOIN() { return getToken(CQLParser.KW_JOIN, 0); }
		public NaturalJoinContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_naturalJoin; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterNaturalJoin(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitNaturalJoin(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitNaturalJoin(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NaturalJoinContext naturalJoin() throws RecognitionException {
		NaturalJoinContext _localctx = new NaturalJoinContext(_ctx, getState());
		enterRule(_localctx, 216, RULE_naturalJoin);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1042); match(KW_NATURAL);
			setState(1043); match(KW_JOIN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FromSourceContext extends ParserRuleContext {
		public DatasourceBodyContext datasourceBody() {
			return getRuleContext(DatasourceBodyContext.class,0);
		}
		public StreamBodyContext streamBody() {
			return getRuleContext(StreamBodyContext.class,0);
		}
		public FromSourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_fromSource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterFromSource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitFromSource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitFromSource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FromSourceContext fromSource() throws RecognitionException {
		FromSourceContext _localctx = new FromSourceContext(_ctx, getState());
		enterRule(_localctx, 218, RULE_fromSource);
		try {
			setState(1047);
			switch (_input.LA(1)) {
			case KW_OPERATOR:
			case KW_DAY:
			case KW_HOUR:
			case KW_MINUTES:
			case KW_SECONDS:
			case KW_MILLISECONDS:
			case KW_BOOLEAN:
			case KW_INT:
			case KW_LONG:
			case KW_FLOAT:
			case KW_DOUBLE:
			case KW_STRING:
			case KW_TIMESTAMP:
			case KW_DATE:
			case KW_TIME:
			case KW_DECIMAL:
			case LPAREN:
			case Identifier:
				enterOuterAlt(_localctx, 1);
				{
				setState(1045); streamBody();
				}
				break;
			case KW_DATASOURCE:
				enterOuterAlt(_localctx, 2);
				{
				setState(1046); datasourceBody();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StreamBodyContext extends ParserRuleContext {
		public StreamSourceContext streamSource() {
			return getRuleContext(StreamSourceContext.class,0);
		}
		public SourceAliasContext sourceAlias() {
			return getRuleContext(SourceAliasContext.class,0);
		}
		public UnidirectionContext unidirection() {
			return getRuleContext(UnidirectionContext.class,0);
		}
		public FilterBeforeWindowContext filterBeforeWindow() {
			return getRuleContext(FilterBeforeWindowContext.class,0);
		}
		public WindowSourceContext windowSource() {
			return getRuleContext(WindowSourceContext.class,0);
		}
		public StreamBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_streamBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStreamBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStreamBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStreamBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StreamBodyContext streamBody() throws RecognitionException {
		StreamBodyContext _localctx = new StreamBodyContext(_ctx, getState());
		enterRule(_localctx, 220, RULE_streamBody);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1049); streamSource();
			setState(1051);
			_la = _input.LA(1);
			if (_la==LPAREN) {
				{
				setState(1050); filterBeforeWindow();
				}
			}

			setState(1054);
			_la = _input.LA(1);
			if (_la==LSQUARE) {
				{
				setState(1053); windowSource();
				}
			}

			setState(1057);
			_la = _input.LA(1);
			if (_la==KW_AS || ((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (KW_OPERATOR - 101)) | (1L << (KW_DAY - 101)) | (1L << (KW_HOUR - 101)) | (1L << (KW_MINUTES - 101)) | (1L << (KW_SECONDS - 101)) | (1L << (KW_MILLISECONDS - 101)) | (1L << (KW_BOOLEAN - 101)) | (1L << (KW_INT - 101)) | (1L << (KW_LONG - 101)) | (1L << (KW_FLOAT - 101)) | (1L << (KW_DOUBLE - 101)) | (1L << (KW_STRING - 101)) | (1L << (KW_TIMESTAMP - 101)) | (1L << (KW_DATE - 101)) | (1L << (KW_TIME - 101)) | (1L << (KW_DECIMAL - 101)) | (1L << (Identifier - 101)))) != 0)) {
				{
				setState(1056); sourceAlias();
				}
			}

			setState(1060);
			_la = _input.LA(1);
			if (_la==KW_UNIDIRECTION) {
				{
				setState(1059); unidirection();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatasourceBodyContext extends ParserRuleContext {
		public DatasourceArgumentsContext datasourceArguments() {
			return getRuleContext(DatasourceArgumentsContext.class,0);
		}
		public TerminalNode KW_DATASOURCE() { return getToken(CQLParser.KW_DATASOURCE, 0); }
		public SourceAliasContext sourceAlias() {
			return getRuleContext(SourceAliasContext.class,0);
		}
		public DataSourceNameContext dataSourceName() {
			return getRuleContext(DataSourceNameContext.class,0);
		}
		public DatasourceBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasourceBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDatasourceBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDatasourceBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDatasourceBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatasourceBodyContext datasourceBody() throws RecognitionException {
		DatasourceBodyContext _localctx = new DatasourceBodyContext(_ctx, getState());
		enterRule(_localctx, 222, RULE_datasourceBody);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1062); match(KW_DATASOURCE);
			setState(1063); dataSourceName();
			setState(1064); datasourceArguments();
			setState(1066);
			_la = _input.LA(1);
			if (_la==KW_AS || ((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (KW_OPERATOR - 101)) | (1L << (KW_DAY - 101)) | (1L << (KW_HOUR - 101)) | (1L << (KW_MINUTES - 101)) | (1L << (KW_SECONDS - 101)) | (1L << (KW_MILLISECONDS - 101)) | (1L << (KW_BOOLEAN - 101)) | (1L << (KW_INT - 101)) | (1L << (KW_LONG - 101)) | (1L << (KW_FLOAT - 101)) | (1L << (KW_DOUBLE - 101)) | (1L << (KW_STRING - 101)) | (1L << (KW_TIMESTAMP - 101)) | (1L << (KW_DATE - 101)) | (1L << (KW_TIME - 101)) | (1L << (KW_DECIMAL - 101)) | (1L << (Identifier - 101)))) != 0)) {
				{
				setState(1065); sourceAlias();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatasourceArgumentsContext extends ParserRuleContext {
		public TerminalNode RSQUARE() { return getToken(CQLParser.RSQUARE, 0); }
		public DatasourceSchemaContext datasourceSchema() {
			return getRuleContext(DatasourceSchemaContext.class,0);
		}
		public TerminalNode LSQUARE() { return getToken(CQLParser.LSQUARE, 0); }
		public TerminalNode COMMA() { return getToken(CQLParser.COMMA, 0); }
		public DatasourceQueryContext datasourceQuery() {
			return getRuleContext(DatasourceQueryContext.class,0);
		}
		public DatasourceArgumentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasourceArguments; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDatasourceArguments(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDatasourceArguments(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDatasourceArguments(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatasourceArgumentsContext datasourceArguments() throws RecognitionException {
		DatasourceArgumentsContext _localctx = new DatasourceArgumentsContext(_ctx, getState());
		enterRule(_localctx, 224, RULE_datasourceArguments);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1068); match(LSQUARE);
			setState(1069); datasourceSchema();
			setState(1070); match(COMMA);
			setState(1071); datasourceQuery();
			setState(1072); match(RSQUARE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatasourceSchemaContext extends ParserRuleContext {
		public TerminalNode KW_SCHEMA() { return getToken(CQLParser.KW_SCHEMA, 0); }
		public ColumnNameTypeListContext columnNameTypeList() {
			return getRuleContext(ColumnNameTypeListContext.class,0);
		}
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public DatasourceSchemaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasourceSchema; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDatasourceSchema(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDatasourceSchema(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDatasourceSchema(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatasourceSchemaContext datasourceSchema() throws RecognitionException {
		DatasourceSchemaContext _localctx = new DatasourceSchemaContext(_ctx, getState());
		enterRule(_localctx, 226, RULE_datasourceSchema);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1074); match(KW_SCHEMA);
			setState(1075); match(LPAREN);
			setState(1076); columnNameTypeList();
			setState(1077); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatasourceQueryContext extends ParserRuleContext {
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public DatasourceQueryArgumentsContext datasourceQueryArguments() {
			return getRuleContext(DatasourceQueryArgumentsContext.class,0);
		}
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public TerminalNode KW_QUERY() { return getToken(CQLParser.KW_QUERY, 0); }
		public DatasourceQueryContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasourceQuery; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDatasourceQuery(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDatasourceQuery(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDatasourceQuery(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatasourceQueryContext datasourceQuery() throws RecognitionException {
		DatasourceQueryContext _localctx = new DatasourceQueryContext(_ctx, getState());
		enterRule(_localctx, 228, RULE_datasourceQuery);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1079); match(KW_QUERY);
			setState(1080); match(LPAREN);
			setState(1082);
			_la = _input.LA(1);
			if (((((_la - 31)) & ~0x3f) == 0 && ((1L << (_la - 31)) & ((1L << (KW_CASE - 31)) | (1L << (KW_CAST - 31)) | (1L << (KW_EXISTS - 31)) | (1L << (KW_FALSE - 31)) | (1L << (KW_NULL - 31)) | (1L << (KW_TRUE - 31)) | (1L << (KW_NOT - 31)) | (1L << (KW_PREVIOUS - 31)))) != 0) || ((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (KW_OPERATOR - 101)) | (1L << (KW_DAY - 101)) | (1L << (KW_HOUR - 101)) | (1L << (KW_MINUTES - 101)) | (1L << (KW_SECONDS - 101)) | (1L << (KW_MILLISECONDS - 101)) | (1L << (KW_BOOLEAN - 101)) | (1L << (KW_INT - 101)) | (1L << (KW_LONG - 101)) | (1L << (KW_FLOAT - 101)) | (1L << (KW_DOUBLE - 101)) | (1L << (KW_STRING - 101)) | (1L << (KW_TIMESTAMP - 101)) | (1L << (KW_DATE - 101)) | (1L << (KW_TIME - 101)) | (1L << (KW_DECIMAL - 101)) | (1L << (LPAREN - 101)) | (1L << (PLUS - 101)) | (1L << (MINUS - 101)) | (1L << (TILDE - 101)) | (1L << (LongLiteral - 101)) | (1L << (FloatLiteral - 101)) | (1L << (DoubleLiteral - 101)) | (1L << (DecimalLiteral - 101)) | (1L << (StringLiteral - 101)) | (1L << (IntegerNumber - 101)) | (1L << (Identifier - 101)))) != 0)) {
				{
				setState(1081); datasourceQueryArguments();
				}
			}

			setState(1084); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DatasourceQueryArgumentsContext extends ParserRuleContext {
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public ExpressionContext expression(int i) {
			return getRuleContext(ExpressionContext.class,i);
		}
		public List<ExpressionContext> expression() {
			return getRuleContexts(ExpressionContext.class);
		}
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public DatasourceQueryArgumentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_datasourceQueryArguments; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDatasourceQueryArguments(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDatasourceQueryArguments(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDatasourceQueryArguments(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DatasourceQueryArgumentsContext datasourceQueryArguments() throws RecognitionException {
		DatasourceQueryArgumentsContext _localctx = new DatasourceQueryArgumentsContext(_ctx, getState());
		enterRule(_localctx, 230, RULE_datasourceQueryArguments);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1086); expression();
			setState(1091);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1087); match(COMMA);
				setState(1088); expression();
				}
				}
				setState(1093);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StreamSourceContext extends ParserRuleContext {
		public SubQuerySourceContext subQuerySource() {
			return getRuleContext(SubQuerySourceContext.class,0);
		}
		public StreamNameContext streamName() {
			return getRuleContext(StreamNameContext.class,0);
		}
		public StreamSourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_streamSource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStreamSource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStreamSource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStreamSource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StreamSourceContext streamSource() throws RecognitionException {
		StreamSourceContext _localctx = new StreamSourceContext(_ctx, getState());
		enterRule(_localctx, 232, RULE_streamSource);
		try {
			setState(1096);
			switch (_input.LA(1)) {
			case KW_OPERATOR:
			case KW_DAY:
			case KW_HOUR:
			case KW_MINUTES:
			case KW_SECONDS:
			case KW_MILLISECONDS:
			case KW_BOOLEAN:
			case KW_INT:
			case KW_LONG:
			case KW_FLOAT:
			case KW_DOUBLE:
			case KW_STRING:
			case KW_TIMESTAMP:
			case KW_DATE:
			case KW_TIME:
			case KW_DECIMAL:
			case Identifier:
				enterOuterAlt(_localctx, 1);
				{
				setState(1094); streamName();
				}
				break;
			case LPAREN:
				enterOuterAlt(_localctx, 2);
				{
				setState(1095); subQuerySource();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnidirectionContext extends ParserRuleContext {
		public TerminalNode KW_UNIDIRECTION() { return getToken(CQLParser.KW_UNIDIRECTION, 0); }
		public UnidirectionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unidirection; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterUnidirection(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitUnidirection(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitUnidirection(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnidirectionContext unidirection() throws RecognitionException {
		UnidirectionContext _localctx = new UnidirectionContext(_ctx, getState());
		enterRule(_localctx, 234, RULE_unidirection);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1098); match(KW_UNIDIRECTION);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FilterBeforeWindowContext extends ParserRuleContext {
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public FilterBeforeWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_filterBeforeWindow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterFilterBeforeWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitFilterBeforeWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitFilterBeforeWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FilterBeforeWindowContext filterBeforeWindow() throws RecognitionException {
		FilterBeforeWindowContext _localctx = new FilterBeforeWindowContext(_ctx, getState());
		enterRule(_localctx, 236, RULE_filterBeforeWindow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1100); match(LPAREN);
			setState(1101); searchCondition();
			setState(1102); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowSourceContext extends ParserRuleContext {
		public TerminalNode RSQUARE() { return getToken(CQLParser.RSQUARE, 0); }
		public TerminalNode LSQUARE() { return getToken(CQLParser.LSQUARE, 0); }
		public WindowBodyContext windowBody() {
			return getRuleContext(WindowBodyContext.class,0);
		}
		public WindowSourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowSource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterWindowSource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitWindowSource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitWindowSource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowSourceContext windowSource() throws RecognitionException {
		WindowSourceContext _localctx = new WindowSourceContext(_ctx, getState());
		enterRule(_localctx, 238, RULE_windowSource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1104); match(LSQUARE);
			setState(1105); windowBody();
			setState(1106); match(RSQUARE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowBodyContext extends ParserRuleContext {
		public RangeTodayContext rangeToday() {
			return getRuleContext(RangeTodayContext.class,0);
		}
		public RowsWindowContext rowsWindow() {
			return getRuleContext(RowsWindowContext.class,0);
		}
		public RangeWindowContext rangeWindow() {
			return getRuleContext(RangeWindowContext.class,0);
		}
		public WindowBodyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowBody; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterWindowBody(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitWindowBody(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitWindowBody(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowBodyContext windowBody() throws RecognitionException {
		WindowBodyContext _localctx = new WindowBodyContext(_ctx, getState());
		enterRule(_localctx, 240, RULE_windowBody);
		try {
			setState(1111);
			switch ( getInterpreter().adaptivePredict(_input,92,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1108); rangeWindow();
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1109); rowsWindow();
				}
				break;

			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(1110); rangeToday();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RowsWindowContext extends ParserRuleContext {
		public WindowDeterminerContext windowDeterminer() {
			return getRuleContext(WindowDeterminerContext.class,0);
		}
		public TerminalNode KW_ROWS() { return getToken(CQLParser.KW_ROWS, 0); }
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public WindowPropertiesContext windowProperties() {
			return getRuleContext(WindowPropertiesContext.class,0);
		}
		public RowsWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rowsWindow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRowsWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRowsWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRowsWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RowsWindowContext rowsWindow() throws RecognitionException {
		RowsWindowContext _localctx = new RowsWindowContext(_ctx, getState());
		enterRule(_localctx, 242, RULE_rowsWindow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1113); match(KW_ROWS);
			setState(1114); constIntegerValue();
			setState(1115); windowProperties();
			setState(1116); windowDeterminer();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeWindowContext extends ParserRuleContext {
		public WindowDeterminerContext windowDeterminer() {
			return getRuleContext(WindowDeterminerContext.class,0);
		}
		public RangeBoundContext rangeBound() {
			return getRuleContext(RangeBoundContext.class,0);
		}
		public TerminalNode KW_RANGE() { return getToken(CQLParser.KW_RANGE, 0); }
		public WindowPropertiesContext windowProperties() {
			return getRuleContext(WindowPropertiesContext.class,0);
		}
		public RangeWindowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeWindow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeWindow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeWindow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeWindow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeWindowContext rangeWindow() throws RecognitionException {
		RangeWindowContext _localctx = new RangeWindowContext(_ctx, getState());
		enterRule(_localctx, 244, RULE_rangeWindow);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1118); match(KW_RANGE);
			setState(1119); rangeBound();
			setState(1121);
            Token token = _input.LT(1);
			_la = token.getType();
			if (_la==KW_SLIDE || _la==KW_BATCH || "ACCUMBATCH".equals(token.getText())) {
				{
				setState(1120); windowProperties();
				}
			}

			setState(1123); windowDeterminer();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeTodayContext extends ParserRuleContext {
		public WindowDeterminerContext windowDeterminer() {
			return getRuleContext(WindowDeterminerContext.class,0);
		}
		public TerminalNode KW_TODAY() { return getToken(CQLParser.KW_TODAY, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode KW_RANGE() { return getToken(CQLParser.KW_RANGE, 0); }
		public RangeTodayContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeToday; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeToday(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeToday(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeToday(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeTodayContext rangeToday() throws RecognitionException {
		RangeTodayContext _localctx = new RangeTodayContext(_ctx, getState());
		enterRule(_localctx, 246, RULE_rangeToday);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1125); match(KW_RANGE);
			setState(1126); match(KW_TODAY);
			setState(1127); expression();
			setState(1128); windowDeterminer();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeBoundContext extends ParserRuleContext {
		public RangeTimeContext rangeTime() {
			return getRuleContext(RangeTimeContext.class,0);
		}
		public RangeUnBoundContext rangeUnBound() {
			return getRuleContext(RangeUnBoundContext.class,0);
		}
		public RangeBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeBound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeBoundContext rangeBound() throws RecognitionException {
		RangeBoundContext _localctx = new RangeBoundContext(_ctx, getState());
		enterRule(_localctx, 248, RULE_rangeBound);
		try {
			setState(1132);
			switch (_input.LA(1)) {
			case KW_SORT:
			case KW_TRIGGER:
			case KW_PARTITION:
			case KW_SLIDE:
			case KW_BATCH:
			case KW_ACCUMBATCH:
			case KW_EXCLUDE:
			case RSQUARE:
			case IntegerNumber:
				enterOuterAlt(_localctx, 1);
				{
				setState(1130); rangeTime();
				}
				break;
			case KW_UNBOUNDED:
				enterOuterAlt(_localctx, 2);
				{
				setState(1131); rangeUnBound();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeUnBoundContext extends ParserRuleContext {
		public TerminalNode KW_UNBOUNDED() { return getToken(CQLParser.KW_UNBOUNDED, 0); }
		public RangeUnBoundContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeUnBound; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeUnBound(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeUnBound(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeUnBound(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeUnBoundContext rangeUnBound() throws RecognitionException {
		RangeUnBoundContext _localctx = new RangeUnBoundContext(_ctx, getState());
		enterRule(_localctx, 250, RULE_rangeUnBound);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1134); match(KW_UNBOUNDED);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeTimeContext extends ParserRuleContext {
		public RangeSecondsContext rangeSeconds() {
			return getRuleContext(RangeSecondsContext.class,0);
		}
		public RangeMinutesContext rangeMinutes() {
			return getRuleContext(RangeMinutesContext.class,0);
		}
		public RangeDayContext rangeDay() {
			return getRuleContext(RangeDayContext.class,0);
		}
		public RangeMilliSecondsContext rangeMilliSeconds() {
			return getRuleContext(RangeMilliSecondsContext.class,0);
		}
		public RangeHourContext rangeHour() {
			return getRuleContext(RangeHourContext.class,0);
		}
		public RangeTimeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeTime; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeTime(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeTime(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeTime(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeTimeContext rangeTime() throws RecognitionException {
		RangeTimeContext _localctx = new RangeTimeContext(_ctx, getState());
		enterRule(_localctx, 252, RULE_rangeTime);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1137);
			switch ( getInterpreter().adaptivePredict(_input,95,_ctx) ) {
			case 1:
				{
				setState(1136); rangeDay();
				}
				break;
			}
			setState(1140);
			switch ( getInterpreter().adaptivePredict(_input,96,_ctx) ) {
			case 1:
				{
				setState(1139); rangeHour();
				}
				break;
			}
			setState(1143);
			switch ( getInterpreter().adaptivePredict(_input,97,_ctx) ) {
			case 1:
				{
				setState(1142); rangeMinutes();
				}
				break;
			}
			setState(1146);
			switch ( getInterpreter().adaptivePredict(_input,98,_ctx) ) {
			case 1:
				{
				setState(1145); rangeSeconds();
				}
				break;
			}
			setState(1149);
			_la = _input.LA(1);
			if (_la==IntegerNumber) {
				{
				setState(1148); rangeMilliSeconds();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeDayContext extends ParserRuleContext {
		public TerminalNode KW_DAY() { return getToken(CQLParser.KW_DAY, 0); }
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public RangeDayContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeDay; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeDay(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeDay(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeDay(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeDayContext rangeDay() throws RecognitionException {
		RangeDayContext _localctx = new RangeDayContext(_ctx, getState());
		enterRule(_localctx, 254, RULE_rangeDay);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1151); constIntegerValue();
			setState(1152); match(KW_DAY);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeHourContext extends ParserRuleContext {
		public TerminalNode KW_HOUR() { return getToken(CQLParser.KW_HOUR, 0); }
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public RangeHourContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeHour; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeHour(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeHour(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeHour(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeHourContext rangeHour() throws RecognitionException {
		RangeHourContext _localctx = new RangeHourContext(_ctx, getState());
		enterRule(_localctx, 256, RULE_rangeHour);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1154); constIntegerValue();
			setState(1155); match(KW_HOUR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeMinutesContext extends ParserRuleContext {
		public TerminalNode KW_MINUTES() { return getToken(CQLParser.KW_MINUTES, 0); }
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public RangeMinutesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeMinutes; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeMinutes(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeMinutes(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeMinutes(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeMinutesContext rangeMinutes() throws RecognitionException {
		RangeMinutesContext _localctx = new RangeMinutesContext(_ctx, getState());
		enterRule(_localctx, 258, RULE_rangeMinutes);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1157); constIntegerValue();
			setState(1158); match(KW_MINUTES);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeSecondsContext extends ParserRuleContext {
		public TerminalNode KW_SECONDS() { return getToken(CQLParser.KW_SECONDS, 0); }
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public RangeSecondsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeSeconds; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeSeconds(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeSeconds(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeSeconds(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeSecondsContext rangeSeconds() throws RecognitionException {
		RangeSecondsContext _localctx = new RangeSecondsContext(_ctx, getState());
		enterRule(_localctx, 260, RULE_rangeSeconds);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1160); constIntegerValue();
			setState(1161); match(KW_SECONDS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RangeMilliSecondsContext extends ParserRuleContext {
		public TerminalNode KW_MILLISECONDS() { return getToken(CQLParser.KW_MILLISECONDS, 0); }
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public RangeMilliSecondsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_rangeMilliSeconds; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterRangeMilliSeconds(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitRangeMilliSeconds(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitRangeMilliSeconds(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RangeMilliSecondsContext rangeMilliSeconds() throws RecognitionException {
		RangeMilliSecondsContext _localctx = new RangeMilliSecondsContext(_ctx, getState());
		enterRule(_localctx, 262, RULE_rangeMilliSeconds);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1163); constIntegerValue();
			setState(1164); match(KW_MILLISECONDS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowPropertiesContext extends ParserRuleContext {
		public TerminalNode KW_SLIDE() { return getToken(CQLParser.KW_SLIDE, 0); }
		public TerminalNode KW_BATCH() { return getToken(CQLParser.KW_BATCH, 0); }
        public TerminalNode KW_ACCUMBATCH() { return getToken(CQLParser.KW_ACCUMBATCH, 0); }
		public WindowPropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowProperties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterWindowProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitWindowProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitWindowProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowPropertiesContext windowProperties() throws RecognitionException {
		WindowPropertiesContext _localctx = new WindowPropertiesContext(_ctx, getState());
		enterRule(_localctx, 264, RULE_windowProperties);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1166);

            Token token = _input.LT(1);
             _la = token.getType();

			if ( !(_la==KW_SLIDE || _la==KW_BATCH || _la==KW_ACCUMBATCH || "ACCUMBATCH".equals(token.getText())) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowDeterminerContext extends ParserRuleContext {
		public ExcludeNowDeterminerContext excludeNowDeterminer() {
			return getRuleContext(ExcludeNowDeterminerContext.class,0);
		}
		public PartitionbyDeterminerContext partitionbyDeterminer() {
			return getRuleContext(PartitionbyDeterminerContext.class,0);
		}
		public SortbyDeterminerContext sortbyDeterminer() {
			return getRuleContext(SortbyDeterminerContext.class,0);
		}
		public TriggerbyDeterminerContext triggerbyDeterminer() {
			return getRuleContext(TriggerbyDeterminerContext.class,0);
		}
		public WindowDeterminerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowDeterminer; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterWindowDeterminer(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitWindowDeterminer(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitWindowDeterminer(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowDeterminerContext windowDeterminer() throws RecognitionException {
		WindowDeterminerContext _localctx = new WindowDeterminerContext(_ctx, getState());
		enterRule(_localctx, 266, RULE_windowDeterminer);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1169);
			_la = _input.LA(1);
			if (_la==KW_PARTITION) {
				{
				setState(1168); partitionbyDeterminer();
				}
			}

			setState(1172);
			_la = _input.LA(1);
			if (_la==KW_SORT) {
				{
				setState(1171); sortbyDeterminer();
				}
			}

			setState(1175);
			_la = _input.LA(1);
			if (_la==KW_TRIGGER) {
				{
				setState(1174); triggerbyDeterminer();
				}
			}

			setState(1178);
			_la = _input.LA(1);
			if (_la==KW_EXCLUDE) {
				{
				setState(1177); excludeNowDeterminer();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PartitionbyDeterminerContext extends ParserRuleContext {
		public TerminalNode KW_BY() { return getToken(CQLParser.KW_BY, 0); }
		public TerminalNode KW_PARTITION() { return getToken(CQLParser.KW_PARTITION, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public PartitionbyDeterminerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_partitionbyDeterminer; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterPartitionbyDeterminer(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitPartitionbyDeterminer(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitPartitionbyDeterminer(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PartitionbyDeterminerContext partitionbyDeterminer() throws RecognitionException {
		PartitionbyDeterminerContext _localctx = new PartitionbyDeterminerContext(_ctx, getState());
		enterRule(_localctx, 268, RULE_partitionbyDeterminer);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1180); match(KW_PARTITION);
			setState(1181); match(KW_BY);
			setState(1182); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SortbyDeterminerContext extends ParserRuleContext {
		public TerminalNode KW_SORT() { return getToken(CQLParser.KW_SORT, 0); }
		public TerminalNode KW_BY() { return getToken(CQLParser.KW_BY, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SortbyDeterminerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sortbyDeterminer; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSortbyDeterminer(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSortbyDeterminer(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSortbyDeterminer(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SortbyDeterminerContext sortbyDeterminer() throws RecognitionException {
		SortbyDeterminerContext _localctx = new SortbyDeterminerContext(_ctx, getState());
		enterRule(_localctx, 270, RULE_sortbyDeterminer);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1184); match(KW_SORT);
			setState(1185); match(KW_BY);
			setState(1186); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TriggerbyDeterminerContext extends ParserRuleContext {
		public TerminalNode KW_BY() { return getToken(CQLParser.KW_BY, 0); }
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public TerminalNode KW_TRIGGER() { return getToken(CQLParser.KW_TRIGGER, 0); }
		public TriggerbyDeterminerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_triggerbyDeterminer; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterTriggerbyDeterminer(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitTriggerbyDeterminer(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitTriggerbyDeterminer(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TriggerbyDeterminerContext triggerbyDeterminer() throws RecognitionException {
		TriggerbyDeterminerContext _localctx = new TriggerbyDeterminerContext(_ctx, getState());
		enterRule(_localctx, 272, RULE_triggerbyDeterminer);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1188); match(KW_TRIGGER);
			setState(1189); match(KW_BY);
			setState(1190); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExcludeNowDeterminerContext extends ParserRuleContext {
		public TerminalNode KW_EXCLUDE() { return getToken(CQLParser.KW_EXCLUDE, 0); }
		public TerminalNode KW_NOW() { return getToken(CQLParser.KW_NOW, 0); }
		public ExcludeNowDeterminerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_excludeNowDeterminer; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExcludeNowDeterminer(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExcludeNowDeterminer(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExcludeNowDeterminer(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExcludeNowDeterminerContext excludeNowDeterminer() throws RecognitionException {
		ExcludeNowDeterminerContext _localctx = new ExcludeNowDeterminerContext(_ctx, getState());
		enterRule(_localctx, 274, RULE_excludeNowDeterminer);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1192); match(KW_EXCLUDE);
			setState(1193); match(KW_NOW);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubQuerySourceContext extends ParserRuleContext {
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public SelectStatementContext selectStatement() {
			return getRuleContext(SelectStatementContext.class,0);
		}
		public SubQuerySourceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subQuerySource; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSubQuerySource(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSubQuerySource(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSubQuerySource(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubQuerySourceContext subQuerySource() throws RecognitionException {
		SubQuerySourceContext _localctx = new SubQuerySourceContext(_ctx, getState());
		enterRule(_localctx, 276, RULE_subQuerySource);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1195); match(LPAREN);
			setState(1196); selectStatement();
			setState(1197); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SourceAliasContext extends ParserRuleContext {
		public TerminalNode KW_AS() { return getToken(CQLParser.KW_AS, 0); }
		public StreamAliasContext streamAlias() {
			return getRuleContext(StreamAliasContext.class,0);
		}
		public SourceAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_sourceAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSourceAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSourceAlias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSourceAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SourceAliasContext sourceAlias() throws RecognitionException {
		SourceAliasContext _localctx = new SourceAliasContext(_ctx, getState());
		enterRule(_localctx, 278, RULE_sourceAlias);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1200);
			_la = _input.LA(1);
			if (_la==KW_AS) {
				{
				setState(1199); match(KW_AS);
				}
			}

			setState(1202); streamAlias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupByClauseContext extends ParserRuleContext {
		public TerminalNode KW_BY() { return getToken(CQLParser.KW_BY, 0); }
		public TerminalNode KW_GROUP() { return getToken(CQLParser.KW_GROUP, 0); }
		public GroupByListContext groupByList() {
			return getRuleContext(GroupByListContext.class,0);
		}
		public GroupByClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupByClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterGroupByClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitGroupByClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitGroupByClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupByClauseContext groupByClause() throws RecognitionException {
		GroupByClauseContext _localctx = new GroupByClauseContext(_ctx, getState());
		enterRule(_localctx, 280, RULE_groupByClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1204); match(KW_GROUP);
			setState(1205); match(KW_BY);
			setState(1206); groupByList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupByListContext extends ParserRuleContext {
		public List<GroupByExpressionContext> groupByExpression() {
			return getRuleContexts(GroupByExpressionContext.class);
		}
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public GroupByExpressionContext groupByExpression(int i) {
			return getRuleContext(GroupByExpressionContext.class,i);
		}
		public GroupByListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupByList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterGroupByList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitGroupByList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitGroupByList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupByListContext groupByList() throws RecognitionException {
		GroupByListContext _localctx = new GroupByListContext(_ctx, getState());
		enterRule(_localctx, 282, RULE_groupByList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1208); groupByExpression();
			setState(1213);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1209); match(COMMA);
				setState(1210); groupByExpression();
				}
				}
				setState(1215);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class GroupByExpressionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public GroupByExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_groupByExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterGroupByExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitGroupByExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitGroupByExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final GroupByExpressionContext groupByExpression() throws RecognitionException {
		GroupByExpressionContext _localctx = new GroupByExpressionContext(_ctx, getState());
		enterRule(_localctx, 284, RULE_groupByExpression);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1216); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HavingClauseContext extends ParserRuleContext {
		public HavingConditionContext havingCondition() {
			return getRuleContext(HavingConditionContext.class,0);
		}
		public TerminalNode KW_HAVING() { return getToken(CQLParser.KW_HAVING, 0); }
		public HavingClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterHavingClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitHavingClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitHavingClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HavingClauseContext havingClause() throws RecognitionException {
		HavingClauseContext _localctx = new HavingClauseContext(_ctx, getState());
		enterRule(_localctx, 286, RULE_havingClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1218); match(KW_HAVING);
			setState(1219); havingCondition();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class HavingConditionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public HavingConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_havingCondition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterHavingCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitHavingCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitHavingCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final HavingConditionContext havingCondition() throws RecognitionException {
		HavingConditionContext _localctx = new HavingConditionContext(_ctx, getState());
		enterRule(_localctx, 288, RULE_havingCondition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1221); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrderByClauseContext extends ParserRuleContext {
		public TerminalNode KW_BY() { return getToken(CQLParser.KW_BY, 0); }
		public TerminalNode KW_ORDER() { return getToken(CQLParser.KW_ORDER, 0); }
		public ColumnNameOrderListContext columnNameOrderList() {
			return getRuleContext(ColumnNameOrderListContext.class,0);
		}
		public OrderByClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_orderByClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterOrderByClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitOrderByClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitOrderByClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrderByClauseContext orderByClause() throws RecognitionException {
		OrderByClauseContext _localctx = new OrderByClauseContext(_ctx, getState());
		enterRule(_localctx, 290, RULE_orderByClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1223); match(KW_ORDER);
			setState(1224); match(KW_BY);
			setState(1225); columnNameOrderList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LimitClauseContext extends ParserRuleContext {
		public TerminalNode KW_LIMIT() { return getToken(CQLParser.KW_LIMIT, 0); }
		public LimitRowContext limitRow() {
			return getRuleContext(LimitRowContext.class,0);
		}
		public LimitClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterLimitClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitLimitClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitLimitClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LimitClauseContext limitClause() throws RecognitionException {
		LimitClauseContext _localctx = new LimitClauseContext(_ctx, getState());
		enterRule(_localctx, 292, RULE_limitClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1227); match(KW_LIMIT);
			setState(1228); limitRow();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LimitAllContext extends ParserRuleContext {
		public TerminalNode KW_ALL() { return getToken(CQLParser.KW_ALL, 0); }
		public LimitAllContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitAll; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterLimitAll(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitLimitAll(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitLimitAll(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LimitAllContext limitAll() throws RecognitionException {
		LimitAllContext _localctx = new LimitAllContext(_ctx, getState());
		enterRule(_localctx, 294, RULE_limitAll);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1230); match(KW_ALL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LimitRowContext extends ParserRuleContext {
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public LimitRowContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_limitRow; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterLimitRow(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitLimitRow(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitLimitRow(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LimitRowContext limitRow() throws RecognitionException {
		LimitRowContext _localctx = new LimitRowContext(_ctx, getState());
		enterRule(_localctx, 296, RULE_limitRow);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1232); constIntegerValue();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DistinctContext extends ParserRuleContext {
		public TerminalNode KW_DISTINCT() { return getToken(CQLParser.KW_DISTINCT, 0); }
		public DistinctContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_distinct; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDistinct(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDistinct(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDistinct(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DistinctContext distinct() throws RecognitionException {
		DistinctContext _localctx = new DistinctContext(_ctx, getState());
		enterRule(_localctx, 298, RULE_distinct);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1234); match(KW_DISTINCT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectClauseContext extends ParserRuleContext {
		public SubSelectClauseContext subSelectClause() {
			return getRuleContext(SubSelectClauseContext.class,0);
		}
		public TerminalNode KW_SELECT() { return getToken(CQLParser.KW_SELECT, 0); }
		public SelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSelectClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSelectClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSelectClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectClauseContext selectClause() throws RecognitionException {
		SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
		enterRule(_localctx, 300, RULE_selectClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1236); match(KW_SELECT);
			setState(1237); subSelectClause();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SubSelectClauseContext extends ParserRuleContext {
		public DistinctContext distinct() {
			return getRuleContext(DistinctContext.class,0);
		}
		public SelectListContext selectList() {
			return getRuleContext(SelectListContext.class,0);
		}
		public SubSelectClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_subSelectClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSubSelectClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSubSelectClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSubSelectClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SubSelectClauseContext subSelectClause() throws RecognitionException {
		SubSelectClauseContext _localctx = new SubSelectClauseContext(_ctx, getState());
		enterRule(_localctx, 302, RULE_subSelectClause);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1240);
			_la = _input.LA(1);
			if (_la==KW_DISTINCT) {
				{
				setState(1239); distinct();
				}
			}

			setState(1242); selectList();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectListContext extends ParserRuleContext {
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public List<SelectItemContext> selectItem() {
			return getRuleContexts(SelectItemContext.class);
		}
		public SelectItemContext selectItem(int i) {
			return getRuleContext(SelectItemContext.class,i);
		}
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public SelectListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSelectList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSelectList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSelectList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectListContext selectList() throws RecognitionException {
		SelectListContext _localctx = new SelectListContext(_ctx, getState());
		enterRule(_localctx, 304, RULE_selectList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1244); selectItem();
			setState(1249);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1245); match(COMMA);
				setState(1246); selectItem();
				}
				}
				setState(1251);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectItemContext extends ParserRuleContext {
		public SelectExpressionContext selectExpression() {
			return getRuleContext(SelectExpressionContext.class,0);
		}
		public SelectItemContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectItem; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSelectItem(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSelectItem(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSelectItem(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectItemContext selectItem() throws RecognitionException {
		SelectItemContext _localctx = new SelectItemContext(_ctx, getState());
		enterRule(_localctx, 306, RULE_selectItem);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1252); selectExpression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectAliasContext extends ParserRuleContext {
		public MultialiasContext multialias() {
			return getRuleContext(MultialiasContext.class,0);
		}
		public SingleAliasContext singleAlias() {
			return getRuleContext(SingleAliasContext.class,0);
		}
		public SelectAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSelectAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSelectAlias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSelectAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectAliasContext selectAlias() throws RecognitionException {
		SelectAliasContext _localctx = new SelectAliasContext(_ctx, getState());
		enterRule(_localctx, 308, RULE_selectAlias);
		try {
			setState(1256);
			switch ( getInterpreter().adaptivePredict(_input,108,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1254); multialias();
				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1255); singleAlias();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MultialiasContext extends ParserRuleContext {
		public TerminalNode KW_AS() { return getToken(CQLParser.KW_AS, 0); }
		public ColumnALiasContext columnALias(int i) {
			return getRuleContext(ColumnALiasContext.class,i);
		}
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public List<ColumnALiasContext> columnALias() {
			return getRuleContexts(ColumnALiasContext.class);
		}
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public MultialiasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_multialias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterMultialias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitMultialias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitMultialias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MultialiasContext multialias() throws RecognitionException {
		MultialiasContext _localctx = new MultialiasContext(_ctx, getState());
		enterRule(_localctx, 310, RULE_multialias);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1258); match(KW_AS);
			setState(1259); match(LPAREN);
			setState(1260); columnALias();
			setState(1265);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1261); match(COMMA);
				setState(1262); columnALias();
				}
				}
				setState(1267);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(1268); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SingleAliasContext extends ParserRuleContext {
		public TerminalNode KW_AS() { return getToken(CQLParser.KW_AS, 0); }
		public ColumnALiasContext columnALias() {
			return getRuleContext(ColumnALiasContext.class,0);
		}
		public SingleAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_singleAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSingleAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSingleAlias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSingleAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SingleAliasContext singleAlias() throws RecognitionException {
		SingleAliasContext _localctx = new SingleAliasContext(_ctx, getState());
		enterRule(_localctx, 312, RULE_singleAlias);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1271);
			_la = _input.LA(1);
			if (_la==KW_AS) {
				{
				setState(1270); match(KW_AS);
				}
			}

			setState(1273); columnALias();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SelectExpressionContext extends ParserRuleContext {
		public SelectAliasContext selectAlias() {
			return getRuleContext(SelectAliasContext.class,0);
		}
		public StreamAllColumnsContext streamAllColumns() {
			return getRuleContext(StreamAllColumnsContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SelectExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_selectExpression; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSelectExpression(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSelectExpression(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSelectExpression(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SelectExpressionContext selectExpression() throws RecognitionException {
		SelectExpressionContext _localctx = new SelectExpressionContext(_ctx, getState());
		enterRule(_localctx, 314, RULE_selectExpression);
		int _la;
		try {
			setState(1280);
			switch ( getInterpreter().adaptivePredict(_input,112,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(1275); expression();
				setState(1277);
				_la = _input.LA(1);
				if (_la==KW_AS || ((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (KW_OPERATOR - 101)) | (1L << (KW_DAY - 101)) | (1L << (KW_HOUR - 101)) | (1L << (KW_MINUTES - 101)) | (1L << (KW_SECONDS - 101)) | (1L << (KW_MILLISECONDS - 101)) | (1L << (KW_BOOLEAN - 101)) | (1L << (KW_INT - 101)) | (1L << (KW_LONG - 101)) | (1L << (KW_FLOAT - 101)) | (1L << (KW_DOUBLE - 101)) | (1L << (KW_STRING - 101)) | (1L << (KW_TIMESTAMP - 101)) | (1L << (KW_DATE - 101)) | (1L << (KW_TIME - 101)) | (1L << (KW_DECIMAL - 101)) | (1L << (Identifier - 101)))) != 0)) {
					{
					setState(1276); selectAlias();
					}
				}

				}
				break;

			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(1279); streamAllColumns();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnNameOrderListContext extends ParserRuleContext {
		public List<ColumnNameOrderContext> columnNameOrder() {
			return getRuleContexts(ColumnNameOrderContext.class);
		}
		public ColumnNameOrderContext columnNameOrder(int i) {
			return getRuleContext(ColumnNameOrderContext.class,i);
		}
		public List<TerminalNode> COMMA() { return getTokens(CQLParser.COMMA); }
		public TerminalNode COMMA(int i) {
			return getToken(CQLParser.COMMA, i);
		}
		public ColumnNameOrderListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnNameOrderList; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterColumnNameOrderList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitColumnNameOrderList(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitColumnNameOrderList(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnNameOrderListContext columnNameOrderList() throws RecognitionException {
		ColumnNameOrderListContext _localctx = new ColumnNameOrderListContext(_ctx, getState());
		enterRule(_localctx, 316, RULE_columnNameOrderList);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1282); columnNameOrder();
			setState(1287);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==COMMA) {
				{
				{
				setState(1283); match(COMMA);
				setState(1284); columnNameOrder();
				}
				}
				setState(1289);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnNameOrderContext extends ParserRuleContext {
		public ColumnOrderContext columnOrder() {
			return getRuleContext(ColumnOrderContext.class,0);
		}
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ColumnNameOrderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnNameOrder; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterColumnNameOrder(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitColumnNameOrder(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitColumnNameOrder(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnNameOrderContext columnNameOrder() throws RecognitionException {
		ColumnNameOrderContext _localctx = new ColumnNameOrderContext(_ctx, getState());
		enterRule(_localctx, 318, RULE_columnNameOrder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1290); expression();
			setState(1292);
			_la = _input.LA(1);
			if (_la==KW_ASC || _la==KW_DESC) {
				{
				setState(1291); columnOrder();
				}
			}

			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnOrderContext extends ParserRuleContext {
		public TerminalNode KW_DESC() { return getToken(CQLParser.KW_DESC, 0); }
		public TerminalNode KW_ASC() { return getToken(CQLParser.KW_ASC, 0); }
		public ColumnOrderContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnOrder; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterColumnOrder(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitColumnOrder(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitColumnOrder(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnOrderContext columnOrder() throws RecognitionException {
		ColumnOrderContext _localctx = new ColumnOrderContext(_ctx, getState());
		enterRule(_localctx, 320, RULE_columnOrder);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1294);
			_la = _input.LA(1);
			if ( !(_la==KW_ASC || _la==KW_DESC) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WhereClauseContext extends ParserRuleContext {
		public TerminalNode KW_WHERE() { return getToken(CQLParser.KW_WHERE, 0); }
		public SearchConditionContext searchCondition() {
			return getRuleContext(SearchConditionContext.class,0);
		}
		public WhereClauseContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_whereClause; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterWhereClause(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitWhereClause(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitWhereClause(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WhereClauseContext whereClause() throws RecognitionException {
		WhereClauseContext _localctx = new WhereClauseContext(_ctx, getState());
		enterRule(_localctx, 322, RULE_whereClause);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1296); match(KW_WHERE);
			setState(1297); searchCondition();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SearchConditionContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public SearchConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_searchCondition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterSearchCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitSearchCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitSearchCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SearchConditionContext searchCondition() throws RecognitionException {
		SearchConditionContext _localctx = new SearchConditionContext(_ctx, getState());
		enterRule(_localctx, 324, RULE_searchCondition);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1299); expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StreamAllColumnsContext extends ParserRuleContext {
		public TerminalNode DOT() { return getToken(CQLParser.DOT, 0); }
		public TerminalNode STAR() { return getToken(CQLParser.STAR, 0); }
		public StreamNameContext streamName() {
			return getRuleContext(StreamNameContext.class,0);
		}
		public StreamAllColumnsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_streamAllColumns; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStreamAllColumns(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStreamAllColumns(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStreamAllColumns(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StreamAllColumnsContext streamAllColumns() throws RecognitionException {
		StreamAllColumnsContext _localctx = new StreamAllColumnsContext(_ctx, getState());
		enterRule(_localctx, 326, RULE_streamAllColumns);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1304);
			_la = _input.LA(1);
			if (((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (KW_OPERATOR - 101)) | (1L << (KW_DAY - 101)) | (1L << (KW_HOUR - 101)) | (1L << (KW_MINUTES - 101)) | (1L << (KW_SECONDS - 101)) | (1L << (KW_MILLISECONDS - 101)) | (1L << (KW_BOOLEAN - 101)) | (1L << (KW_INT - 101)) | (1L << (KW_LONG - 101)) | (1L << (KW_FLOAT - 101)) | (1L << (KW_DOUBLE - 101)) | (1L << (KW_STRING - 101)) | (1L << (KW_TIMESTAMP - 101)) | (1L << (KW_DATE - 101)) | (1L << (KW_TIME - 101)) | (1L << (KW_DECIMAL - 101)) | (1L << (Identifier - 101)))) != 0)) {
				{
				setState(1301); streamName();
				setState(1302); match(DOT);
				}
			}

			setState(1306); match(STAR);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class CqlIdentifierContext extends ParserRuleContext {
		public TerminalNode KW_SECONDS() { return getToken(CQLParser.KW_SECONDS, 0); }
		public TerminalNode KW_DAY() { return getToken(CQLParser.KW_DAY, 0); }
		public TerminalNode KW_INT() { return getToken(CQLParser.KW_INT, 0); }
		public TerminalNode KW_TIME() { return getToken(CQLParser.KW_TIME, 0); }
		public TerminalNode KW_DATE() { return getToken(CQLParser.KW_DATE, 0); }
		public TerminalNode KW_MILLISECONDS() { return getToken(CQLParser.KW_MILLISECONDS, 0); }
		public TerminalNode KW_LONG() { return getToken(CQLParser.KW_LONG, 0); }
		public TerminalNode KW_STRING() { return getToken(CQLParser.KW_STRING, 0); }
		public TerminalNode KW_OPERATOR() { return getToken(CQLParser.KW_OPERATOR, 0); }
		public TerminalNode Identifier() { return getToken(CQLParser.Identifier, 0); }
		public TerminalNode KW_BOOLEAN() { return getToken(CQLParser.KW_BOOLEAN, 0); }
		public TerminalNode KW_TIMESTAMP() { return getToken(CQLParser.KW_TIMESTAMP, 0); }
		public TerminalNode KW_DOUBLE() { return getToken(CQLParser.KW_DOUBLE, 0); }
		public TerminalNode KW_MINUTES() { return getToken(CQLParser.KW_MINUTES, 0); }
		public TerminalNode KW_HOUR() { return getToken(CQLParser.KW_HOUR, 0); }
		public TerminalNode KW_DECIMAL() { return getToken(CQLParser.KW_DECIMAL, 0); }
		public TerminalNode KW_FLOAT() { return getToken(CQLParser.KW_FLOAT, 0); }
		public CqlIdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_cqlIdentifier; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterCqlIdentifier(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitCqlIdentifier(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitCqlIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final CqlIdentifierContext cqlIdentifier() throws RecognitionException {
		CqlIdentifierContext _localctx = new CqlIdentifierContext(_ctx, getState());
		enterRule(_localctx, 328, RULE_cqlIdentifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1308);
			_la = _input.LA(1);
			if ( !(((((_la - 101)) & ~0x3f) == 0 && ((1L << (_la - 101)) & ((1L << (KW_OPERATOR - 101)) | (1L << (KW_DAY - 101)) | (1L << (KW_HOUR - 101)) | (1L << (KW_MINUTES - 101)) | (1L << (KW_SECONDS - 101)) | (1L << (KW_MILLISECONDS - 101)) | (1L << (KW_BOOLEAN - 101)) | (1L << (KW_INT - 101)) | (1L << (KW_LONG - 101)) | (1L << (KW_FLOAT - 101)) | (1L << (KW_DOUBLE - 101)) | (1L << (KW_STRING - 101)) | (1L << (KW_TIMESTAMP - 101)) | (1L << (KW_DATE - 101)) | (1L << (KW_TIME - 101)) | (1L << (KW_DECIMAL - 101)) | (1L << (Identifier - 101)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StreamPropertiesContext extends ParserRuleContext {
		public TerminalNode KW_PROPERTIES() { return getToken(CQLParser.KW_PROPERTIES, 0); }
		public StreamPropertiesListContext streamPropertiesList() {
			return getRuleContext(StreamPropertiesListContext.class,0);
		}
		public TerminalNode LPAREN() { return getToken(CQLParser.LPAREN, 0); }
		public TerminalNode RPAREN() { return getToken(CQLParser.RPAREN, 0); }
		public StreamPropertiesContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_streamProperties; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStreamProperties(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStreamProperties(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStreamProperties(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StreamPropertiesContext streamProperties() throws RecognitionException {
		StreamPropertiesContext _localctx = new StreamPropertiesContext(_ctx, getState());
		enterRule(_localctx, 330, RULE_streamProperties);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1310); match(KW_PROPERTIES);
			setState(1311); match(LPAREN);
			setState(1312); streamPropertiesList();
			setState(1313); match(RPAREN);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConfNameContext extends ParserRuleContext {
		public TerminalNode StringLiteral() { return getToken(CQLParser.StringLiteral, 0); }
		public ConfNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_confName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConfName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConfName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConfName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConfNameContext confName() throws RecognitionException {
		ConfNameContext _localctx = new ConfNameContext(_ctx, getState());
		enterRule(_localctx, 332, RULE_confName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1315); match(StringLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConfValueContext extends ParserRuleContext {
		public TerminalNode StringLiteral() { return getToken(CQLParser.StringLiteral, 0); }
		public ConfValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_confValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConfValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConfValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConfValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConfValueContext confValue() throws RecognitionException {
		ConfValueContext _localctx = new ConfValueContext(_ctx, getState());
		enterRule(_localctx, 334, RULE_confValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1317); match(StringLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StrValueContext extends ParserRuleContext {
		public TerminalNode StringLiteral() { return getToken(CQLParser.StringLiteral, 0); }
		public StrValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_strValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStrValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStrValue(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStrValue(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StrValueContext strValue() throws RecognitionException {
		StrValueContext _localctx = new StrValueContext(_ctx, getState());
		enterRule(_localctx, 336, RULE_strValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1319); match(StringLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UnaryOperatorContext extends ParserRuleContext {
		public TerminalNode PLUS() { return getToken(CQLParser.PLUS, 0); }
		public TerminalNode MINUS() { return getToken(CQLParser.MINUS, 0); }
		public TerminalNode TILDE() { return getToken(CQLParser.TILDE, 0); }
		public UnaryOperatorContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_unaryOperator; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterUnaryOperator(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitUnaryOperator(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitUnaryOperator(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UnaryOperatorContext unaryOperator() throws RecognitionException {
		UnaryOperatorContext _localctx = new UnaryOperatorContext(_ctx, getState());
		enterRule(_localctx, 338, RULE_unaryOperator);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1321);
			_la = _input.LA(1);
			if ( !(((((_la - 137)) & ~0x3f) == 0 && ((1L << (_la - 137)) & ((1L << (PLUS - 137)) | (1L << (MINUS - 137)) | (1L << (TILDE - 137)))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			consume();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FunctionNameContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public FunctionNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_functionName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterFunctionName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitFunctionName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitFunctionName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final FunctionNameContext functionName() throws RecognitionException {
		FunctionNameContext _localctx = new FunctionNameContext(_ctx, getState());
		enterRule(_localctx, 340, RULE_functionName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1323); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WindowNameContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public WindowNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_windowName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterWindowName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitWindowName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitWindowName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WindowNameContext windowName() throws RecognitionException {
		WindowNameContext _localctx = new WindowNameContext(_ctx, getState());
		enterRule(_localctx, 342, RULE_windowName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1325); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ClassNameContext extends ParserRuleContext {
		public InnerClassNameContext innerClassName() {
			return getRuleContext(InnerClassNameContext.class,0);
		}
		public UserDefinedClassNameContext userDefinedClassName() {
			return getRuleContext(UserDefinedClassNameContext.class,0);
		}
		public ClassNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_className; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterClassName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitClassName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitClassName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ClassNameContext className() throws RecognitionException {
		ClassNameContext _localctx = new ClassNameContext(_ctx, getState());
		enterRule(_localctx, 344, RULE_className);
		try {
			setState(1329);
			switch (_input.LA(1)) {
			case KW_OPERATOR:
			case KW_DAY:
			case KW_HOUR:
			case KW_MINUTES:
			case KW_SECONDS:
			case KW_MILLISECONDS:
			case KW_BOOLEAN:
			case KW_INT:
			case KW_LONG:
			case KW_FLOAT:
			case KW_DOUBLE:
			case KW_STRING:
			case KW_TIMESTAMP:
			case KW_DATE:
			case KW_TIME:
			case KW_DECIMAL:
			case Identifier:
				enterOuterAlt(_localctx, 1);
				{
				setState(1327); innerClassName();
				}
				break;
			case StringLiteral:
				enterOuterAlt(_localctx, 2);
				{
				setState(1328); userDefinedClassName();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class InnerClassNameContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public InnerClassNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_innerClassName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterInnerClassName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitInnerClassName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitInnerClassName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final InnerClassNameContext innerClassName() throws RecognitionException {
		InnerClassNameContext _localctx = new InnerClassNameContext(_ctx, getState());
		enterRule(_localctx, 346, RULE_innerClassName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1331); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class UserDefinedClassNameContext extends ParserRuleContext {
		public TerminalNode StringLiteral() { return getToken(CQLParser.StringLiteral, 0); }
		public UserDefinedClassNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_userDefinedClassName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterUserDefinedClassName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitUserDefinedClassName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitUserDefinedClassName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final UserDefinedClassNameContext userDefinedClassName() throws RecognitionException {
		UserDefinedClassNameContext _localctx = new UserDefinedClassNameContext(_ctx, getState());
		enterRule(_localctx, 348, RULE_userDefinedClassName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1333); match(StringLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PathContext extends ParserRuleContext {
		public TerminalNode StringLiteral() { return getToken(CQLParser.StringLiteral, 0); }
		public PathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitPath(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitPath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathContext path() throws RecognitionException {
		PathContext _localctx = new PathContext(_ctx, getState());
		enterRule(_localctx, 350, RULE_path);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1335); match(StringLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ApplicationNameContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public ConstIntegerValueContext constIntegerValue() {
			return getRuleContext(ConstIntegerValueContext.class,0);
		}
		public ApplicationNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_applicationName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterApplicationName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitApplicationName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitApplicationName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ApplicationNameContext applicationName() throws RecognitionException {
		ApplicationNameContext _localctx = new ApplicationNameContext(_ctx, getState());
		enterRule(_localctx, 352, RULE_applicationName);
		try {
			setState(1339);
			switch (_input.LA(1)) {
			case KW_OPERATOR:
			case KW_DAY:
			case KW_HOUR:
			case KW_MINUTES:
			case KW_SECONDS:
			case KW_MILLISECONDS:
			case KW_BOOLEAN:
			case KW_INT:
			case KW_LONG:
			case KW_FLOAT:
			case KW_DOUBLE:
			case KW_STRING:
			case KW_TIMESTAMP:
			case KW_DATE:
			case KW_TIME:
			case KW_DECIMAL:
			case Identifier:
				enterOuterAlt(_localctx, 1);
				{
				setState(1337); cqlIdentifier();
				}
				break;
			case IntegerNumber:
				enterOuterAlt(_localctx, 2);
				{
				setState(1338); constIntegerValue();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnNameContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public ColumnNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterColumnName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitColumnName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitColumnName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnNameContext columnName() throws RecognitionException {
		ColumnNameContext _localctx = new ColumnNameContext(_ctx, getState());
		enterRule(_localctx, 354, RULE_columnName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1341); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IsForceContext extends ParserRuleContext {
		public TerminalNode KW_FORCE() { return getToken(CQLParser.KW_FORCE, 0); }
		public IsForceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_isForce; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterIsForce(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitIsForce(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitIsForce(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IsForceContext isForce() throws RecognitionException {
		IsForceContext _localctx = new IsForceContext(_ctx, getState());
		enterRule(_localctx, 356, RULE_isForce);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1343); match(KW_FORCE);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IfExistsContext extends ParserRuleContext {
		public TerminalNode KW_EXISTS() { return getToken(CQLParser.KW_EXISTS, 0); }
		public TerminalNode KW_IF() { return getToken(CQLParser.KW_IF, 0); }
		public IfExistsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ifExists; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterIfExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitIfExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitIfExists(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IfExistsContext ifExists() throws RecognitionException {
		IfExistsContext _localctx = new IfExistsContext(_ctx, getState());
		enterRule(_localctx, 358, RULE_ifExists);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1345); match(KW_IF);
			setState(1346); match(KW_EXISTS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IfNotExistsContext extends ParserRuleContext {
		public TerminalNode KW_NOT() { return getToken(CQLParser.KW_NOT, 0); }
		public TerminalNode KW_EXISTS() { return getToken(CQLParser.KW_EXISTS, 0); }
		public TerminalNode KW_IF() { return getToken(CQLParser.KW_IF, 0); }
		public IfNotExistsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ifNotExists; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterIfNotExists(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitIfNotExists(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitIfNotExists(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IfNotExistsContext ifNotExists() throws RecognitionException {
		IfNotExistsContext _localctx = new IfNotExistsContext(_ctx, getState());
		enterRule(_localctx, 360, RULE_ifNotExists);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1348); match(KW_IF);
			setState(1349); match(KW_NOT);
			setState(1350); match(KW_EXISTS);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StreamNameContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public StreamNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_streamName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStreamName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStreamName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStreamName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StreamNameContext streamName() throws RecognitionException {
		StreamNameContext _localctx = new StreamNameContext(_ctx, getState());
		enterRule(_localctx, 362, RULE_streamName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1352); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class DataSourceNameContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public DataSourceNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_dataSourceName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterDataSourceName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitDataSourceName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitDataSourceName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final DataSourceNameContext dataSourceName() throws RecognitionException {
		DataSourceNameContext _localctx = new DataSourceNameContext(_ctx, getState());
		enterRule(_localctx, 364, RULE_dataSourceName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1354); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StreamAliasContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public StreamAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_streamAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStreamAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStreamAlias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStreamAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StreamAliasContext streamAlias() throws RecognitionException {
		StreamAliasContext _localctx = new StreamAliasContext(_ctx, getState());
		enterRule(_localctx, 366, RULE_streamAlias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1356); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StreamNameOrAliasContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public StreamNameOrAliasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_streamNameOrAlias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterStreamNameOrAlias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitStreamNameOrAlias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitStreamNameOrAlias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final StreamNameOrAliasContext streamNameOrAlias() throws RecognitionException {
		StreamNameOrAliasContext _localctx = new StreamNameOrAliasContext(_ctx, getState());
		enterRule(_localctx, 368, RULE_streamNameOrAlias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1358); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ColumnALiasContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public ColumnALiasContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_columnALias; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterColumnALias(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitColumnALias(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitColumnALias(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ColumnALiasContext columnALias() throws RecognitionException {
		ColumnALiasContext _localctx = new ColumnALiasContext(_ctx, getState());
		enterRule(_localctx, 370, RULE_columnALias);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1360); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ConstNullContext extends ParserRuleContext {
		public TerminalNode KW_NULL() { return getToken(CQLParser.KW_NULL, 0); }
		public ConstNullContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_constNull; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterConstNull(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitConstNull(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitConstNull(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ConstNullContext constNull() throws RecognitionException {
		ConstNullContext _localctx = new ConstNullContext(_ctx, getState());
		enterRule(_localctx, 372, RULE_constNull);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1362); match(KW_NULL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExtendedContext extends ParserRuleContext {
		public TerminalNode KW_EXTENDED() { return getToken(CQLParser.KW_EXTENDED, 0); }
		public ExtendedContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_extended; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterExtended(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitExtended(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitExtended(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExtendedContext extended() throws RecognitionException {
		ExtendedContext _localctx = new ExtendedContext(_ctx, getState());
		enterRule(_localctx, 374, RULE_extended);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1364); match(KW_EXTENDED);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierNotContext extends ParserRuleContext {
		public TerminalNode KW_NOT() { return getToken(CQLParser.KW_NOT, 0); }
		public IdentifierNotContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifierNot; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterIdentifierNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitIdentifierNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitIdentifierNot(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierNotContext identifierNot() throws RecognitionException {
		IdentifierNotContext _localctx = new IdentifierNotContext(_ctx, getState());
		enterRule(_localctx, 376, RULE_identifierNot);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1366); match(KW_NOT);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NullConditionContext extends ParserRuleContext {
		public TerminalNode KW_NULL() { return getToken(CQLParser.KW_NULL, 0); }
		public IdentifierNotContext identifierNot() {
			return getRuleContext(IdentifierNotContext.class,0);
		}
		public NullConditionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nullCondition; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterNullCondition(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitNullCondition(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitNullCondition(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NullConditionContext nullCondition() throws RecognitionException {
		NullConditionContext _localctx = new NullConditionContext(_ctx, getState());
		enterRule(_localctx, 378, RULE_nullCondition);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1369);
			_la = _input.LA(1);
			if (_la==KW_NOT) {
				{
				setState(1368); identifierNot();
				}
			}

			setState(1371); match(KW_NULL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OperatorNameContext extends ParserRuleContext {
		public CqlIdentifierContext cqlIdentifier() {
			return getRuleContext(CqlIdentifierContext.class,0);
		}
		public OperatorNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_operatorName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).enterOperatorName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof CQLParserListener ) ((CQLParserListener)listener).exitOperatorName(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof CQLParserVisitor ) return ((CQLParserVisitor<? extends T>)visitor).visitOperatorName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OperatorNameContext operatorName() throws RecognitionException {
		OperatorNameContext _localctx = new OperatorNameContext(_ctx, getState());
		enterRule(_localctx, 380, RULE_operatorName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(1373); cqlIdentifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\uacf5\uee8c\u4f5d\u8b0d\u4a45\u78bd\u1b2f\u3378\3\u00a3\u0562\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
		"\tI\4J\tJ\4K\tK\4L\tL\4M\tM\4N\tN\4O\tO\4P\tP\4Q\tQ\4R\tR\4S\tS\4T\tT"+
		"\4U\tU\4V\tV\4W\tW\4X\tX\4Y\tY\4Z\tZ\4[\t[\4\\\t\\\4]\t]\4^\t^\4_\t_\4"+
		"`\t`\4a\ta\4b\tb\4c\tc\4d\td\4e\te\4f\tf\4g\tg\4h\th\4i\ti\4j\tj\4k\t"+
		"k\4l\tl\4m\tm\4n\tn\4o\to\4p\tp\4q\tq\4r\tr\4s\ts\4t\tt\4u\tu\4v\tv\4"+
		"w\tw\4x\tx\4y\ty\4z\tz\4{\t{\4|\t|\4}\t}\4~\t~\4\177\t\177\4\u0080\t\u0080"+
		"\4\u0081\t\u0081\4\u0082\t\u0082\4\u0083\t\u0083\4\u0084\t\u0084\4\u0085"+
		"\t\u0085\4\u0086\t\u0086\4\u0087\t\u0087\4\u0088\t\u0088\4\u0089\t\u0089"+
		"\4\u008a\t\u008a\4\u008b\t\u008b\4\u008c\t\u008c\4\u008d\t\u008d\4\u008e"+
		"\t\u008e\4\u008f\t\u008f\4\u0090\t\u0090\4\u0091\t\u0091\4\u0092\t\u0092"+
		"\4\u0093\t\u0093\4\u0094\t\u0094\4\u0095\t\u0095\4\u0096\t\u0096\4\u0097"+
		"\t\u0097\4\u0098\t\u0098\4\u0099\t\u0099\4\u009a\t\u009a\4\u009b\t\u009b"+
		"\4\u009c\t\u009c\4\u009d\t\u009d\4\u009e\t\u009e\4\u009f\t\u009f\4\u00a0"+
		"\t\u00a0\4\u00a1\t\u00a1\4\u00a2\t\u00a2\4\u00a3\t\u00a3\4\u00a4\t\u00a4"+
		"\4\u00a5\t\u00a5\4\u00a6\t\u00a6\4\u00a7\t\u00a7\4\u00a8\t\u00a8\4\u00a9"+
		"\t\u00a9\4\u00aa\t\u00aa\4\u00ab\t\u00ab\4\u00ac\t\u00ac\4\u00ad\t\u00ad"+
		"\4\u00ae\t\u00ae\4\u00af\t\u00af\4\u00b0\t\u00b0\4\u00b1\t\u00b1\4\u00b2"+
		"\t\u00b2\4\u00b3\t\u00b3\4\u00b4\t\u00b4\4\u00b5\t\u00b5\4\u00b6\t\u00b6"+
		"\4\u00b7\t\u00b7\4\u00b8\t\u00b8\4\u00b9\t\u00b9\4\u00ba\t\u00ba\4\u00bb"+
		"\t\u00bb\4\u00bc\t\u00bc\4\u00bd\t\u00bd\4\u00be\t\u00be\4\u00bf\t\u00bf"+
		"\4\u00c0\t\u00c0\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2\u018a\n\2\3\3"+
		"\3\3\3\3\3\3\5\3\u0190\n\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4"+
		"\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4\u01a6\n\4\3\5\3\5\3\5\3\5\3\5"+
		"\3\5\3\5\3\5\5\5\u01b0\n\5\3\5\5\5\u01b3\n\5\3\5\5\5\u01b6\n\5\3\5\3\5"+
		"\5\5\u01ba\n\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6\u01c4\n\6\3\6\5\6\u01c7"+
		"\n\6\3\6\5\6\u01ca\n\6\3\6\3\6\5\6\u01ce\n\6\3\7\3\7\3\7\3\7\3\7\3\7\3"+
		"\7\5\7\u01d7\n\7\3\7\5\7\u01da\n\7\3\b\3\b\3\b\3\b\3\b\3\b\5\b\u01e2\n"+
		"\b\3\t\3\t\3\t\3\t\3\t\3\t\3\t\3\t\5\t\u01ec\n\t\3\n\3\n\3\n\3\n\3\n\3"+
		"\13\3\13\3\13\3\13\3\13\3\f\3\f\5\f\u01fa\n\f\3\r\3\r\5\r\u01fe\n\r\3"+
		"\16\3\16\5\16\u0202\n\16\3\17\3\17\3\20\3\20\3\21\3\21\3\21\5\21\u020b"+
		"\n\21\3\22\3\22\3\22\3\23\3\23\3\24\3\24\3\24\7\24\u0215\n\24\f\24\16"+
		"\24\u0218\13\24\3\25\3\25\3\25\7\25\u021d\n\25\f\25\16\25\u0220\13\25"+
		"\3\26\3\26\3\26\3\26\3\27\3\27\3\27\3\30\3\30\3\31\3\31\3\32\3\32\3\32"+
		"\3\33\3\33\3\34\3\34\3\34\3\35\3\35\3\36\3\36\3\36\3\36\3\37\3\37\3\37"+
		"\3\37\3 \3 \3 \3 \3 \3 \5 \u0245\n \3!\3!\3!\5!\u024a\n!\3!\3!\3\"\3\""+
		"\3\"\3#\3#\3#\3#\3#\3$\3$\3$\3$\3%\3%\3%\3%\5%\u025e\n%\3&\3&\3&\5&\u0263"+
		"\n&\3&\3&\3\'\3\'\3\'\5\'\u026a\n\'\3\'\3\'\3\'\5\'\u026f\n\'\5\'\u0271"+
		"\n\'\3(\3(\3(\5(\u0276\n(\3(\3(\3)\3)\3)\5)\u027d\n)\3)\5)\u0280\n)\3"+
		")\5)\u0283\n)\3*\3*\3*\3*\3+\3+\3+\3+\3,\3,\3,\3,\3,\3,\3,\3-\3-\3.\3"+
		".\3.\7.\u0299\n.\f.\16.\u029c\13.\3/\3/\3/\7/\u02a1\n/\f/\16/\u02a4\13"+
		"/\3\60\5\60\u02a7\n\60\3\60\3\60\3\61\3\61\5\61\u02ad\n\61\3\62\3\62\3"+
		"\62\3\62\3\62\3\62\5\62\u02b5\n\62\3\63\3\63\3\63\3\64\3\64\3\64\3\64"+
		"\3\65\5\65\u02bf\n\65\3\65\3\65\3\65\3\65\3\65\3\66\3\66\7\66\u02c8\n"+
		"\66\f\66\16\66\u02cb\13\66\3\67\3\67\3\67\38\38\39\39\39\3:\5:\u02d6\n"+
		":\3:\3:\3:\3;\5;\u02dc\n;\3;\3;\3;\3<\3<\3=\3=\3>\3>\3?\3?\3?\3?\7?\u02eb"+
		"\n?\f?\16?\u02ee\13?\3@\3@\3A\3A\3A\3A\7A\u02f6\nA\fA\16A\u02f9\13A\3"+
		"B\3B\3C\3C\3C\3C\7C\u0301\nC\fC\16C\u0304\13C\3D\3D\3D\5D\u0309\nD\3D"+
		"\3D\3E\3E\3F\3F\3F\3F\3F\3F\3F\3F\3F\5F\u0318\nF\3G\3G\3G\3G\3H\5H\u031f"+
		"\nH\3H\3H\3H\3H\3H\5H\u0326\nH\3H\3H\5H\u032a\nH\3I\3I\3J\3J\3K\3K\3L"+
		"\3L\3M\3M\3N\3N\3O\3O\3O\5O\u033b\nO\3O\3O\3O\7O\u0340\nO\fO\16O\u0343"+
		"\13O\5O\u0345\nO\3O\3O\3P\3P\3P\3P\3P\3P\3P\3Q\3Q\3Q\3Q\3Q\6Q\u0355\n"+
		"Q\rQ\16Q\u0356\3Q\5Q\u035a\nQ\3Q\3Q\3R\3R\3R\3R\6R\u0362\nR\rR\16R\u0363"+
		"\3R\5R\u0367\nR\3R\3R\3S\3S\3T\3T\3T\3U\3U\3U\3V\3V\3V\3W\3W\3X\3X\3X"+
		"\3X\7X\u037c\nX\fX\16X\u037f\13X\3X\3X\3Y\3Y\3Y\3Z\3Z\3Z\3[\3[\3[\3[\3"+
		"[\3[\5[\u038f\n[\3[\5[\u0392\n[\3\\\3\\\3\\\3\\\3]\3]\6]\u039a\n]\r]\16"+
		"]\u039b\3]\5]\u039f\n]\3^\3^\3^\5^\u03a4\n^\3^\5^\u03a7\n^\3^\5^\u03aa"+
		"\n^\3^\5^\u03ad\n^\3^\5^\u03b0\n^\3^\5^\u03b3\n^\3_\3_\3_\3`\3`\5`\u03ba"+
		"\n`\3`\5`\u03bd\n`\3`\5`\u03c0\n`\3`\5`\u03c3\n`\3`\5`\u03c6\n`\3a\3a"+
		"\3a\3b\3b\3b\5b\u03ce\nb\3b\3b\3c\3c\3c\5c\u03d5\nc\3d\3d\7d\u03d9\nd"+
		"\fd\16d\u03dc\13d\3e\3e\3e\5e\u03e1\ne\3f\3f\3f\3g\3g\3g\3g\3g\6g\u03eb"+
		"\ng\rg\16g\u03ec\3g\3g\3h\3h\3h\3h\3h\3h\5h\u03f7\nh\3i\3i\3i\5i\u03fc"+
		"\ni\3j\3j\5j\u0400\nj\3j\3j\3k\3k\5k\u0406\nk\3k\3k\3l\3l\5l\u040c\nl"+
		"\3l\3l\3m\3m\3m\5m\u0413\nm\3n\3n\3n\3o\3o\5o\u041a\no\3p\3p\5p\u041e"+
		"\np\3p\5p\u0421\np\3p\5p\u0424\np\3p\5p\u0427\np\3q\3q\3q\3q\5q\u042d"+
		"\nq\3r\3r\3r\3r\3r\3r\3s\3s\3s\3s\3s\3t\3t\3t\5t\u043d\nt\3t\3t\3u\3u"+
		"\3u\7u\u0444\nu\fu\16u\u0447\13u\3v\3v\5v\u044b\nv\3w\3w\3x\3x\3x\3x\3"+
		"y\3y\3y\3y\3z\3z\3z\5z\u045a\nz\3{\3{\3{\3{\3{\3|\3|\3|\5|\u0464\n|\3"+
		"|\3|\3}\3}\3}\3}\3}\3~\3~\5~\u046f\n~\3\177\3\177\3\u0080\5\u0080\u0474"+
		"\n\u0080\3\u0080\5\u0080\u0477\n\u0080\3\u0080\5\u0080\u047a\n\u0080\3"+
		"\u0080\5\u0080\u047d\n\u0080\3\u0080\5\u0080\u0480\n\u0080\3\u0081\3\u0081"+
		"\3\u0081\3\u0082\3\u0082\3\u0082\3\u0083\3\u0083\3\u0083\3\u0084\3\u0084"+
		"\3\u0084\3\u0085\3\u0085\3\u0085\3\u0086\3\u0086\3\u0087\5\u0087\u0494"+
		"\n\u0087\3\u0087\5\u0087\u0497\n\u0087\3\u0087\5\u0087\u049a\n\u0087\3"+
		"\u0087\5\u0087\u049d\n\u0087\3\u0088\3\u0088\3\u0088\3\u0088\3\u0089\3"+
		"\u0089\3\u0089\3\u0089\3\u008a\3\u008a\3\u008a\3\u008a\3\u008b\3\u008b"+
		"\3\u008b\3\u008c\3\u008c\3\u008c\3\u008c\3\u008d\5\u008d\u04b3\n\u008d"+
		"\3\u008d\3\u008d\3\u008e\3\u008e\3\u008e\3\u008e\3\u008f\3\u008f\3\u008f"+
		"\7\u008f\u04be\n\u008f\f\u008f\16\u008f\u04c1\13\u008f\3\u0090\3\u0090"+
		"\3\u0091\3\u0091\3\u0091\3\u0092\3\u0092\3\u0093\3\u0093\3\u0093\3\u0093"+
		"\3\u0094\3\u0094\3\u0094\3\u0095\3\u0095\3\u0096\3\u0096\3\u0097\3\u0097"+
		"\3\u0098\3\u0098\3\u0098\3\u0099\5\u0099\u04db\n\u0099\3\u0099\3\u0099"+
		"\3\u009a\3\u009a\3\u009a\7\u009a\u04e2\n\u009a\f\u009a\16\u009a\u04e5"+
		"\13\u009a\3\u009b\3\u009b\3\u009c\3\u009c\5\u009c\u04eb\n\u009c\3\u009d"+
		"\3\u009d\3\u009d\3\u009d\3\u009d\7\u009d\u04f2\n\u009d\f\u009d\16\u009d"+
		"\u04f5\13\u009d\3\u009d\3\u009d\3\u009e\5\u009e\u04fa\n\u009e\3\u009e"+
		"\3\u009e\3\u009f\3\u009f\5\u009f\u0500\n\u009f\3\u009f\5\u009f\u0503\n"+
		"\u009f\3\u00a0\3\u00a0\3\u00a0\7\u00a0\u0508\n\u00a0\f\u00a0\16\u00a0"+
		"\u050b\13\u00a0\3\u00a1\3\u00a1\5\u00a1\u050f\n\u00a1\3\u00a2\3\u00a2"+
		"\3\u00a3\3\u00a3\3\u00a3\3\u00a4\3\u00a4\3\u00a5\3\u00a5\3\u00a5\5\u00a5"+
		"\u051b\n\u00a5\3\u00a5\3\u00a5\3\u00a6\3\u00a6\3\u00a7\3\u00a7\3\u00a7"+
		"\3\u00a7\3\u00a7\3\u00a8\3\u00a8\3\u00a9\3\u00a9\3\u00aa\3\u00aa\3\u00ab"+
		"\3\u00ab\3\u00ac\3\u00ac\3\u00ad\3\u00ad\3\u00ae\3\u00ae\5\u00ae\u0534"+
		"\n\u00ae\3\u00af\3\u00af\3\u00b0\3\u00b0\3\u00b1\3\u00b1\3\u00b2\3\u00b2"+
		"\5\u00b2\u053e\n\u00b2\3\u00b3\3\u00b3\3\u00b4\3\u00b4\3\u00b5\3\u00b5"+
		"\3\u00b5\3\u00b6\3\u00b6\3\u00b6\3\u00b6\3\u00b7\3\u00b7\3\u00b8\3\u00b8"+
		"\3\u00b9\3\u00b9\3\u00ba\3\u00ba\3\u00bb\3\u00bb\3\u00bc\3\u00bc\3\u00bd"+
		"\3\u00bd\3\u00be\3\u00be\3\u00bf\5\u00bf\u055c\n\u00bf\3\u00bf\3\u00bf"+
		"\3\u00c0\3\u00c0\3\u00c0\2\u00c1\2\4\6\b\n\f\16\20\22\24\26\30\32\34\36"+
		" \"$&(*,.\60\62\64\668:<>@BDFHJLNPRTVXZ\\^`bdfhjlnprtvxz|~\u0080\u0082"+
		"\u0084\u0086\u0088\u008a\u008c\u008e\u0090\u0092\u0094\u0096\u0098\u009a"+
		"\u009c\u009e\u00a0\u00a2\u00a4\u00a6\u00a8\u00aa\u00ac\u00ae\u00b0\u00b2"+
		"\u00b4\u00b6\u00b8\u00ba\u00bc\u00be\u00c0\u00c2\u00c4\u00c6\u00c8\u00ca"+
		"\u00cc\u00ce\u00d0\u00d2\u00d4\u00d6\u00d8\u00da\u00dc\u00de\u00e0\u00e2"+
		"\u00e4\u00e6\u00e8\u00ea\u00ec\u00ee\u00f0\u00f2\u00f4\u00f6\u00f8\u00fa"+
		"\u00fc\u00fe\u0100\u0102\u0104\u0106\u0108\u010a\u010c\u010e\u0110\u0112"+
		"\u0114\u0116\u0118\u011a\u011c\u011e\u0120\u0122\u0124\u0126\u0128\u012a"+
		"\u012c\u012e\u0130\u0132\u0134\u0136\u0138\u013a\u013c\u013e\u0140\u0142"+
		"\u0144\u0146\u0148\u014a\u014c\u014e\u0150\u0152\u0154\u0156\u0158\u015a"+
		"\u015c\u015e\u0160\u0162\u0164\u0166\u0168\u016a\u016c\u016e\u0170\u0172"+
		"\u0174\u0176\u0178\u017a\u017c\u017e\2\r\3\2ox\3\2\u0083\u0089\3\2\36"+
		" \3\2\u0092\u0094\3\2\u008b\u008d\4\2\u008a\u008a\u008e\u0090\4\2))--"+
		"\3\2WX\3\2\60\61\5\2ggjx\u009f\u009f\4\2\u008b\u008c\u0091\u0091\u0541"+
		"\2\u0189\3\2\2\2\4\u018f\3\2\2\2\6\u01a5\3\2\2\2\b\u01a7\3\2\2\2\n\u01bb"+
		"\3\2\2\2\f\u01cf\3\2\2\2\16\u01db\3\2\2\2\20\u01e3\3\2\2\2\22\u01ed\3"+
		"\2\2\2\24\u01f2\3\2\2\2\26\u01f7\3\2\2\2\30\u01fb\3\2\2\2\32\u01ff\3\2"+
		"\2\2\34\u0203\3\2\2\2\36\u0205\3\2\2\2 \u0207\3\2\2\2\"\u020c\3\2\2\2"+
		"$\u020f\3\2\2\2&\u0211\3\2\2\2(\u0219\3\2\2\2*\u0221\3\2\2\2,\u0225\3"+
		"\2\2\2.\u0228\3\2\2\2\60\u022a\3\2\2\2\62\u022c\3\2\2\2\64\u022f\3\2\2"+
		"\2\66\u0231\3\2\2\28\u0234\3\2\2\2:\u0236\3\2\2\2<\u023a\3\2\2\2>\u023e"+
		"\3\2\2\2@\u0246\3\2\2\2B\u024d\3\2\2\2D\u0250\3\2\2\2F\u0255\3\2\2\2H"+
		"\u0259\3\2\2\2J\u025f\3\2\2\2L\u0270\3\2\2\2N\u0272\3\2\2\2P\u0279\3\2"+
		"\2\2R\u0284\3\2\2\2T\u0288\3\2\2\2V\u028c\3\2\2\2X\u0293\3\2\2\2Z\u0295"+
		"\3\2\2\2\\\u029d\3\2\2\2^\u02a6\3\2\2\2`\u02ac\3\2\2\2b\u02ae\3\2\2\2"+
		"d\u02b6\3\2\2\2f\u02b9\3\2\2\2h\u02be\3\2\2\2j\u02c5\3\2\2\2l\u02cc\3"+
		"\2\2\2n\u02cf\3\2\2\2p\u02d1\3\2\2\2r\u02d5\3\2\2\2t\u02db\3\2\2\2v\u02e0"+
		"\3\2\2\2x\u02e2\3\2\2\2z\u02e4\3\2\2\2|\u02e6\3\2\2\2~\u02ef\3\2\2\2\u0080"+
		"\u02f1\3\2\2\2\u0082\u02fa\3\2\2\2\u0084\u02fc\3\2\2\2\u0086\u0308\3\2"+
		"\2\2\u0088\u030c\3\2\2\2\u008a\u0317\3\2\2\2\u008c\u0319\3\2\2\2\u008e"+
		"\u0329\3\2\2\2\u0090\u032b\3\2\2\2\u0092\u032d\3\2\2\2\u0094\u032f\3\2"+
		"\2\2\u0096\u0331\3\2\2\2\u0098\u0333\3\2\2\2\u009a\u0335\3\2\2\2\u009c"+
		"\u0337\3\2\2\2\u009e\u0348\3\2\2\2\u00a0\u034f\3\2\2\2\u00a2\u035d\3\2"+
		"\2\2\u00a4\u036a\3\2\2\2\u00a6\u036c\3\2\2\2\u00a8\u036f\3\2\2\2\u00aa"+
		"\u0372\3\2\2\2\u00ac\u0375\3\2\2\2\u00ae\u0377\3\2\2\2\u00b0\u0382\3\2"+
		"\2\2\u00b2\u0385\3\2\2\2\u00b4\u0388\3\2\2\2\u00b6\u0393\3\2\2\2\u00b8"+
		"\u0397\3\2\2\2\u00ba\u03a0\3\2\2\2\u00bc\u03b4\3\2\2\2\u00be\u03b7\3\2"+
		"\2\2\u00c0\u03c7\3\2\2\2\u00c2\u03ca\3\2\2\2\u00c4\u03d1\3\2\2\2\u00c6"+
		"\u03d6\3\2\2\2\u00c8\u03dd\3\2\2\2\u00ca\u03e2\3\2\2\2\u00cc\u03e5\3\2"+
		"\2\2\u00ce\u03f6\3\2\2\2\u00d0\u03fb\3\2\2\2\u00d2\u03fd\3\2\2\2\u00d4"+
		"\u0403\3\2\2\2\u00d6\u0409\3\2\2\2\u00d8\u0412\3\2\2\2\u00da\u0414\3\2"+
		"\2\2\u00dc\u0419\3\2\2\2\u00de\u041b\3\2\2\2\u00e0\u0428\3\2\2\2\u00e2"+
		"\u042e\3\2\2\2\u00e4\u0434\3\2\2\2\u00e6\u0439\3\2\2\2\u00e8\u0440\3\2"+
		"\2\2\u00ea\u044a\3\2\2\2\u00ec\u044c\3\2\2\2\u00ee\u044e\3\2\2\2\u00f0"+
		"\u0452\3\2\2\2\u00f2\u0459\3\2\2\2\u00f4\u045b\3\2\2\2\u00f6\u0460\3\2"+
		"\2\2\u00f8\u0467\3\2\2\2\u00fa\u046e\3\2\2\2\u00fc\u0470\3\2\2\2\u00fe"+
		"\u0473\3\2\2\2\u0100\u0481\3\2\2\2\u0102\u0484\3\2\2\2\u0104\u0487\3\2"+
		"\2\2\u0106\u048a\3\2\2\2\u0108\u048d\3\2\2\2\u010a\u0490\3\2\2\2\u010c"+
		"\u0493\3\2\2\2\u010e\u049e\3\2\2\2\u0110\u04a2\3\2\2\2\u0112\u04a6\3\2"+
		"\2\2\u0114\u04aa\3\2\2\2\u0116\u04ad\3\2\2\2\u0118\u04b2\3\2\2\2\u011a"+
		"\u04b6\3\2\2\2\u011c\u04ba\3\2\2\2\u011e\u04c2\3\2\2\2\u0120\u04c4\3\2"+
		"\2\2\u0122\u04c7\3\2\2\2\u0124\u04c9\3\2\2\2\u0126\u04cd\3\2\2\2\u0128"+
		"\u04d0\3\2\2\2\u012a\u04d2\3\2\2\2\u012c\u04d4\3\2\2\2\u012e\u04d6\3\2"+
		"\2\2\u0130\u04da\3\2\2\2\u0132\u04de\3\2\2\2\u0134\u04e6\3\2\2\2\u0136"+
		"\u04ea\3\2\2\2\u0138\u04ec\3\2\2\2\u013a\u04f9\3\2\2\2\u013c\u0502\3\2"+
		"\2\2\u013e\u0504\3\2\2\2\u0140\u050c\3\2\2\2\u0142\u0510\3\2\2\2\u0144"+
		"\u0512\3\2\2\2\u0146\u0515\3\2\2\2\u0148\u051a\3\2\2\2\u014a\u051e\3\2"+
		"\2\2\u014c\u0520\3\2\2\2\u014e\u0525\3\2\2\2\u0150\u0527\3\2\2\2\u0152"+
		"\u0529\3\2\2\2\u0154\u052b\3\2\2\2\u0156\u052d\3\2\2\2\u0158\u052f\3\2"+
		"\2\2\u015a\u0533\3\2\2\2\u015c\u0535\3\2\2\2\u015e\u0537\3\2\2\2\u0160"+
		"\u0539\3\2\2\2\u0162\u053d\3\2\2\2\u0164\u053f\3\2\2\2\u0166\u0541\3\2"+
		"\2\2\u0168\u0543\3\2\2\2\u016a\u0546\3\2\2\2\u016c\u054a\3\2\2\2\u016e"+
		"\u054c\3\2\2\2\u0170\u054e\3\2\2\2\u0172\u0550\3\2\2\2\u0174\u0552\3\2"+
		"\2\2\u0176\u0554\3\2\2\2\u0178\u0556\3\2\2\2\u017a\u0558\3\2\2\2\u017c"+
		"\u055b\3\2\2\2\u017e\u055f\3\2\2\2\u0180\u0181\5\6\4\2\u0181\u0182\7\2"+
		"\2\3\u0182\u018a\3\2\2\2\u0183\u0184\5\4\3\2\u0184\u0185\7\2\2\3\u0185"+
		"\u018a\3\2\2\2\u0186\u0187\5X-\2\u0187\u0188\7\2\2\3\u0188\u018a\3\2\2"+
		"\2\u0189\u0180\3\2\2\2\u0189\u0183\3\2\2\2\u0189\u0186\3\2\2\2\u018a\3"+
		"\3\2\2\2\u018b\u0190\5\u00b0Y\2\u018c\u0190\5\u00b2Z\2\u018d\u0190\5\u00b8"+
		"]\2\u018e\u0190\5\u00ba^\2\u018f\u018b\3\2\2\2\u018f\u018c\3\2\2\2\u018f"+
		"\u018d\3\2\2\2\u018f\u018e\3\2\2\2\u0190\5\3\2\2\2\u0191\u01a6\5\b\5\2"+
		"\u0192\u01a6\5\n\6\2\u0193\u01a6\5\f\7\2\u0194\u01a6\5\16\b\2\u0195\u01a6"+
		"\5\20\t\2\u0196\u01a6\5N(\2\u0197\u01a6\5P)\2\u0198\u01a6\5J&\2\u0199"+
		"\u01a6\5L\'\2\u019a\u01a6\5F$\2\u019b\u01a6\5H%\2\u019c\u01a6\5B\"\2\u019d"+
		"\u01a6\5D#\2\u019e\u01a6\5> \2\u019f\u01a6\5@!\2\u01a0\u01a6\5:\36\2\u01a1"+
		"\u01a6\5<\37\2\u01a2\u01a6\5R*\2\u01a3\u01a6\5T+\2\u01a4\u01a6\5V,\2\u01a5"+
		"\u0191\3\2\2\2\u01a5\u0192\3\2\2\2\u01a5\u0193\3\2\2\2\u01a5\u0194\3\2"+
		"\2\2\u01a5\u0195\3\2\2\2\u01a5\u0196\3\2\2\2\u01a5\u0197\3\2\2\2\u01a5"+
		"\u0198\3\2\2\2\u01a5\u0199\3\2\2\2\u01a5\u019a\3\2\2\2\u01a5\u019b\3\2"+
		"\2\2\u01a5\u019c\3\2\2\2\u01a5\u019d\3\2\2\2\u01a5\u019e\3\2\2\2\u01a5"+
		"\u019f\3\2\2\2\u01a5\u01a0\3\2\2\2\u01a5\u01a1\3\2\2\2\u01a5\u01a2\3\2"+
		"\2\2\u01a5\u01a3\3\2\2\2\u01a5\u01a4\3\2\2\2\u01a6\7\3\2\2\2\u01a7\u01a8"+
		"\7\3\2\2\u01a8\u01a9\7\23\2\2\u01a9\u01aa\7\24\2\2\u01aa\u01af\5\u016c"+
		"\u00b7\2\u01ab\u01ac\7}\2\2\u01ac\u01ad\5&\24\2\u01ad\u01ae\7~\2\2\u01ae"+
		"\u01b0\3\2\2\2\u01af\u01ab\3\2\2\2\u01af\u01b0\3\2\2\2\u01b0\u01b2\3\2"+
		"\2\2\u01b1\u01b3\5\"\22\2\u01b2\u01b1\3\2\2\2\u01b2\u01b3\3\2\2\2\u01b3"+
		"\u01b5\3\2\2\2\u01b4\u01b6\5\26\f\2\u01b5\u01b4\3\2\2\2\u01b5\u01b6\3"+
		"\2\2\2\u01b6\u01b7\3\2\2\2\u01b7\u01b9\5\30\r\2\u01b8\u01ba\5\u00c0a\2"+
		"\u01b9\u01b8\3\2\2\2\u01b9\u01ba\3\2\2\2\u01ba\t\3\2\2\2\u01bb\u01bc\7"+
		"\3\2\2\u01bc\u01bd\7\25\2\2\u01bd\u01be\7\24\2\2\u01be\u01c3\5\u016c\u00b7"+
		"\2\u01bf\u01c0\7}\2\2\u01c0\u01c1\5&\24\2\u01c1\u01c2\7~\2\2\u01c2\u01c4"+
		"\3\2\2\2\u01c3\u01bf\3\2\2\2\u01c3\u01c4\3\2\2\2\u01c4\u01c6\3\2\2\2\u01c5"+
		"\u01c7\5\"\22\2\u01c6\u01c5\3\2\2\2\u01c6\u01c7\3\2\2\2\u01c7\u01c9\3"+
		"\2\2\2\u01c8\u01ca\5\26\f\2\u01c9\u01c8\3\2\2\2\u01c9\u01ca\3\2\2\2\u01ca"+
		"\u01cb\3\2\2\2\u01cb\u01cd\5\32\16\2\u01cc\u01ce\5\u00c0a\2\u01cd\u01cc"+
		"\3\2\2\2\u01cd\u01ce\3\2\2\2\u01ce\13\3\2\2\2\u01cf\u01d0\7\3\2\2\u01d0"+
		"\u01d1\7\24\2\2\u01d1\u01d6\5\u016c\u00b7\2\u01d2\u01d3\7}\2\2\u01d3\u01d4"+
		"\5&\24\2\u01d4\u01d5\7~\2\2\u01d5\u01d7\3\2\2\2\u01d6\u01d2\3\2\2\2\u01d6"+
		"\u01d7\3\2\2\2\u01d7\u01d9\3\2\2\2\u01d8\u01da\5\"\22\2\u01d9\u01d8\3"+
		"\2\2\2\u01d9\u01da\3\2\2\2\u01da\r\3\2\2\2\u01db\u01dc\7\3\2\2\u01dc\u01dd"+
		"\7`\2\2\u01dd\u01de\5\u016e\u00b8\2\u01de\u01df\7\22\2\2\u01df\u01e1\5"+
		"\u015a\u00ae\2\u01e0\u01e2\5\60\31\2\u01e1\u01e0\3\2\2\2\u01e1\u01e2\3"+
		"\2\2\2\u01e2\17\3\2\2\2\u01e3\u01e4\7\3\2\2\u01e4\u01e5\7g\2\2\u01e5\u01e6"+
		"\5\u017e\u00c0\2\u01e6\u01e7\7*\2\2\u01e7\u01e8\5\u015a\u00ae\2\u01e8"+
		"\u01e9\5\22\n\2\u01e9\u01eb\5\24\13\2\u01ea\u01ec\5\u014c\u00a7\2\u01eb"+
		"\u01ea\3\2\2\2\u01eb\u01ec\3\2\2\2\u01ec\21\3\2\2\2\u01ed\u01ee\7\23\2"+
		"\2\u01ee\u01ef\7}\2\2\u01ef\u01f0\5&\24\2\u01f0\u01f1\7~\2\2\u01f1\23"+
		"\3\2\2\2\u01f2\u01f3\7\25\2\2\u01f3\u01f4\7}\2\2\u01f4\u01f5\5&\24\2\u01f5"+
		"\u01f6\7~\2\2\u01f6\25\3\2\2\2\u01f7\u01f9\5,\27\2\u01f8\u01fa\5.\30\2"+
		"\u01f9\u01f8\3\2\2\2\u01f9\u01fa\3\2\2\2\u01fa\27\3\2\2\2\u01fb\u01fd"+
		"\5\66\34\2\u01fc\u01fe\58\35\2\u01fd\u01fc\3\2\2\2\u01fd\u01fe\3\2\2\2"+
		"\u01fe\31\3\2\2\2\u01ff\u0201\5\62\32\2\u0200\u0202\5\64\33\2\u0201\u0200"+
		"\3\2\2\2\u0201\u0202\3\2\2\2\u0202\33\3\2\2\2\u0203\u0204\t\2\2\2\u0204"+
		"\35\3\2\2\2\u0205\u0206\5\34\17\2\u0206\37\3\2\2\2\u0207\u0208\5\u0164"+
		"\u00b3\2\u0208\u020a\5\36\20\2\u0209\u020b\5\"\22\2\u020a\u0209\3\2\2"+
		"\2\u020a\u020b\3\2\2\2\u020b!\3\2\2\2\u020c\u020d\7\r\2\2\u020d\u020e"+
		"\5$\23\2\u020e#\3\2\2\2\u020f\u0210\7\u009b\2\2\u0210%\3\2\2\2\u0211\u0216"+
		"\5 \21\2\u0212\u0213\7{\2\2\u0213\u0215\5 \21\2\u0214\u0212\3\2\2\2\u0215"+
		"\u0218\3\2\2\2\u0216\u0214\3\2\2\2\u0216\u0217\3\2\2\2\u0217\'\3\2\2\2"+
		"\u0218\u0216\3\2\2\2\u0219\u021e\5*\26\2\u021a\u021b\7{\2\2\u021b\u021d"+
		"\5*\26\2\u021c\u021a\3\2\2\2\u021d\u0220\3\2\2\2\u021e\u021c\3\2\2\2\u021e"+
		"\u021f\3\2\2\2\u021f)\3\2\2\2\u0220\u021e\3\2\2\2\u0221\u0222\5\u014e"+
		"\u00a8\2\u0222\u0223\7\u0083\2\2\u0223\u0224\5\u0150\u00a9\2\u0224+\3"+
		"\2\2\2\u0225\u0226\7\17\2\2\u0226\u0227\5\u015a\u00ae\2\u0227-\3\2\2\2"+
		"\u0228\u0229\5\u014c\u00a7\2\u0229/\3\2\2\2\u022a\u022b\5\u014c\u00a7"+
		"\2\u022b\61\3\2\2\2\u022c\u022d\7\26\2\2\u022d\u022e\5\u015a\u00ae\2\u022e"+
		"\63\3\2\2\2\u022f\u0230\5\u014c\u00a7\2\u0230\65\3\2\2\2\u0231\u0232\7"+
		"\22\2\2\u0232\u0233\5\u015a\u00ae\2\u0233\67\3\2\2\2\u0234\u0235\5\u014c"+
		"\u00a7\2\u02359\3\2\2\2\u0236\u0237\7\13\2\2\u0237\u0238\7M\2\2\u0238"+
		"\u0239\5\u0160\u00b1\2\u0239;\3\2\2\2\u023a\u023b\7\13\2\2\u023b\u023c"+
		"\7P\2\2\u023c\u023d\5\u0160\u00b1\2\u023d=\3\2\2\2\u023e\u023f\7\3\2\2"+
		"\u023f\u0240\7Q\2\2\u0240\u0241\5\u0156\u00ac\2\u0241\u0242\7*\2\2\u0242"+
		"\u0244\5\u015e\u00b0\2\u0243\u0245\5\u014c\u00a7\2\u0244\u0243\3\2\2\2"+
		"\u0244\u0245\3\2\2\2\u0245?\3\2\2\2\u0246\u0247\7\n\2\2\u0247\u0249\7"+
		"Q\2\2\u0248\u024a\5\u0168\u00b5\2\u0249\u0248\3\2\2\2\u0249\u024a\3\2"+
		"\2\2\u024a\u024b\3\2\2\2\u024b\u024c\5\u0156\u00ac\2\u024cA\3\2\2\2\u024d"+
		"\u024e\7\7\2\2\u024e\u024f\5\u014e\u00a8\2\u024fC\3\2\2\2\u0250\u0251"+
		"\7\6\2\2\u0251\u0252\5\u014e\u00a8\2\u0252\u0253\7\u0083\2\2\u0253\u0254"+
		"\5\u0150\u00a9\2\u0254E\3\2\2\2\u0255\u0256\7\b\2\2\u0256\u0257\7\30\2"+
		"\2\u0257\u0258\5\u0160\u00b1\2\u0258G\3\2\2\2\u0259\u025a\7\5\2\2\u025a"+
		"\u025b\7\30\2\2\u025b\u025d\5\u0162\u00b2\2\u025c\u025e\5\u0160\u00b1"+
		"\2\u025d\u025c\3\2\2\2\u025d\u025e\3\2\2\2\u025eI\3\2\2\2\u025f\u0260"+
		"\7\4\2\2\u0260\u0262\7L\2\2\u0261\u0263\5\u0178\u00bd\2\u0262\u0261\3"+
		"\2\2\2\u0262\u0263\3\2\2\2\u0263\u0264\3\2\2\2\u0264\u0265\5\u0156\u00ac"+
		"\2\u0265K\3\2\2\2\u0266\u0267\7\4\2\2\u0267\u0269\7I\2\2\u0268\u026a\5"+
		"\u0162\u00b2\2\u0269\u0268\3\2\2\2\u0269\u026a\3\2\2\2\u026a\u0271\3\2"+
		"\2\2\u026b\u026c\7\4\2\2\u026c\u026e\7I\2\2\u026d\u026f\5\u0152\u00aa"+
		"\2\u026e\u026d\3\2\2\2\u026e\u026f\3\2\2\2\u026f\u0271\3\2\2\2\u0270\u0266"+
		"\3\2\2\2\u0270\u026b\3\2\2\2\u0271M\3\2\2\2\u0272\u0273\7\n\2\2\u0273"+
		"\u0275\7\30\2\2\u0274\u0276\5\u0168\u00b5\2\u0275\u0274\3\2\2\2\u0275"+
		"\u0276\3\2\2\2\u0276\u0277\3\2\2\2\u0277\u0278\5\u0162\u00b2\2\u0278O"+
		"\3\2\2\2\u0279\u027a\7\27\2\2\u027a\u027c\7\30\2\2\u027b\u027d\5\u0166"+
		"\u00b4\2\u027c\u027b\3\2\2\2\u027c\u027d\3\2\2\2\u027d\u027f\3\2\2\2\u027e"+
		"\u0280\5\u0162\u00b2\2\u027f\u027e\3\2\2\2\u027f\u0280\3\2\2\2\u0280\u0282"+
		"\3\2\2\2\u0281\u0283\5\u0160\u00b1\2\u0282\u0281\3\2\2\2\u0282\u0283\3"+
		"\2\2\2\u0283Q\3\2\2\2\u0284\u0285\7c\2\2\u0285\u0286\7\30\2\2\u0286\u0287"+
		"\5\u0162\u00b2\2\u0287S\3\2\2\2\u0288\u0289\7d\2\2\u0289\u028a\7\30\2"+
		"\2\u028a\u028b\5\u0162\u00b2\2\u028bU\3\2\2\2\u028c\u028d\7f\2\2\u028d"+
		"\u028e\7\30\2\2\u028e\u028f\5\u0162\u00b2\2\u028f\u0290\7\6\2\2\u0290"+
		"\u0291\7e\2\2\u0291\u0292\5\u0092J\2\u0292W\3\2\2\2\u0293\u0294\5Z.\2"+
		"\u0294Y\3\2\2\2\u0295\u029a\5\\/\2\u0296\u0297\7\33\2\2\u0297\u0299\5"+
		"\\/\2\u0298\u0296\3\2\2\2\u0299\u029c\3\2\2\2\u029a\u0298\3\2\2\2\u029a"+
		"\u029b\3\2\2\2\u029b[\3\2\2\2\u029c\u029a\3\2\2\2\u029d\u02a2\5^\60\2"+
		"\u029e\u029f\7\32\2\2\u029f\u02a1\5^\60\2\u02a0\u029e\3\2\2\2\u02a1\u02a4"+
		"\3\2\2\2\u02a2\u02a0\3\2\2\2\u02a2\u02a3\3\2\2\2\u02a3]\3\2\2\2\u02a4"+
		"\u02a2\3\2\2\2\u02a5\u02a7\5\u017a\u00be\2\u02a6\u02a5\3\2\2\2\u02a6\u02a7"+
		"\3\2\2\2\u02a7\u02a8\3\2\2\2\u02a8\u02a9\5`\61\2\u02a9_\3\2\2\2\u02aa"+
		"\u02ad\5b\62\2\u02ab\u02ad\5d\63\2\u02ac\u02aa\3\2\2\2\u02ac\u02ab\3\2"+
		"\2\2\u02ada\3\2\2\2\u02ae\u02b4\5j\66\2\u02af\u02b0\7,\2\2\u02b0\u02b5"+
		"\5\u017c\u00bf\2\u02b1\u02b5\5t;\2\u02b2\u02b5\5h\65\2\u02b3\u02b5\5r"+
		":\2\u02b4\u02af\3\2\2\2\u02b4\u02b1\3\2\2\2\u02b4\u02b2\3\2\2\2\u02b4"+
		"\u02b3\3\2\2\2\u02b4\u02b5\3\2\2\2\u02b5c\3\2\2\2\u02b6\u02b7\7\'\2\2"+
		"\u02b7\u02b8\5f\64\2\u02b8e\3\2\2\2\u02b9\u02ba\7}\2\2\u02ba\u02bb\5\u00ba"+
		"^\2\u02bb\u02bc\7~\2\2\u02bcg\3\2\2\2\u02bd\u02bf\5\u017a\u00be\2\u02be"+
		"\u02bd\3\2\2\2\u02be\u02bf\3\2\2\2\u02bf\u02c0\3\2\2\2\u02c0\u02c1\7\34"+
		"\2\2\u02c1\u02c2\5x=\2\u02c2\u02c3\7\32\2\2\u02c3\u02c4\5z>\2\u02c4i\3"+
		"\2\2\2\u02c5\u02c9\5|?\2\u02c6\u02c8\5l\67\2\u02c7\u02c6\3\2\2\2\u02c8"+
		"\u02cb\3\2\2\2\u02c9\u02c7\3\2\2\2\u02c9\u02ca\3\2\2\2\u02cak\3\2\2\2"+
		"\u02cb\u02c9\3\2\2\2\u02cc\u02cd\5n8\2\u02cd\u02ce\5|?\2\u02cem\3\2\2"+
		"\2\u02cf\u02d0\t\3\2\2\u02d0o\3\2\2\2\u02d1\u02d2\7_\2\2\u02d2\u02d3\5"+
		"\u00aeX\2\u02d3q\3\2\2\2\u02d4\u02d6\5\u017a\u00be\2\u02d5\u02d4\3\2\2"+
		"\2\u02d5\u02d6\3\2\2\2\u02d6\u02d7\3\2\2\2\u02d7\u02d8\7\35\2\2\u02d8"+
		"\u02d9\5\u00aeX\2\u02d9s\3\2\2\2\u02da\u02dc\5\u017a\u00be\2\u02db\u02da"+
		"\3\2\2\2\u02db\u02dc\3\2\2\2\u02dc\u02dd\3\2\2\2\u02dd\u02de\5v<\2\u02de"+
		"\u02df\5|?\2\u02dfu\3\2\2\2\u02e0\u02e1\t\4\2\2\u02e1w\3\2\2\2\u02e2\u02e3"+
		"\5|?\2\u02e3y\3\2\2\2\u02e4\u02e5\5|?\2\u02e5{\3\2\2\2\u02e6\u02ec\5\u0080"+
		"A\2\u02e7\u02e8\5~@\2\u02e8\u02e9\5\u0080A\2\u02e9\u02eb\3\2\2\2\u02ea"+
		"\u02e7\3\2\2\2\u02eb\u02ee\3\2\2\2\u02ec\u02ea\3\2\2\2\u02ec\u02ed\3\2"+
		"\2\2\u02ed}\3\2\2\2\u02ee\u02ec\3\2\2\2\u02ef\u02f0\t\5\2\2\u02f0\177"+
		"\3\2\2\2\u02f1\u02f7\5\u0084C\2\u02f2\u02f3\5\u0082B\2\u02f3\u02f4\5\u0084"+
		"C\2\u02f4\u02f6\3\2\2\2\u02f5\u02f2\3\2\2\2\u02f6\u02f9\3\2\2\2\u02f7"+
		"\u02f5\3\2\2\2\u02f7\u02f8\3\2\2\2\u02f8\u0081\3\2\2\2\u02f9\u02f7\3\2"+
		"\2\2\u02fa\u02fb\t\6\2\2\u02fb\u0083\3\2\2\2\u02fc\u0302\5\u0086D\2\u02fd"+
		"\u02fe\5\u0088E\2\u02fe\u02ff\5\u0086D\2\u02ff\u0301\3\2\2\2\u0300\u02fd"+
		"\3\2\2\2\u0301\u0304\3\2\2\2\u0302\u0300\3\2\2\2\u0302\u0303\3\2\2\2\u0303"+
		"\u0085\3\2\2\2\u0304\u0302\3\2\2\2\u0305\u0306\5\u0172\u00ba\2\u0306\u0307"+
		"\7y\2\2\u0307\u0309\3\2\2\2\u0308\u0305\3\2\2\2\u0308\u0309\3\2\2\2\u0309"+
		"\u030a\3\2\2\2\u030a\u030b\5\u008aF\2\u030b\u0087\3\2\2\2\u030c\u030d"+
		"\t\7\2\2\u030d\u0089\3\2\2\2\u030e\u0318\5\u0176\u00bc\2\u030f\u0318\5"+
		"\u008eH\2\u0310\u0318\5p9\2\u0311\u0318\5\u009cO\2\u0312\u0318\5\u009e"+
		"P\2\u0313\u0318\5\u00a0Q\2\u0314\u0318\5\u00a2R\2\u0315\u0318\5\u0164"+
		"\u00b3\2\u0316\u0318\5\u008cG\2\u0317\u030e\3\2\2\2\u0317\u030f\3\2\2"+
		"\2\u0317\u0310\3\2\2\2\u0317\u0311\3\2\2\2\u0317\u0312\3\2\2\2\u0317\u0313"+
		"\3\2\2\2\u0317\u0314\3\2\2\2\u0317\u0315\3\2\2\2\u0317\u0316\3\2\2\2\u0318"+
		"\u008b\3\2\2\2\u0319\u031a\7}\2\2\u031a\u031b\5X-\2\u031b\u031c\7~\2\2"+
		"\u031c\u008d\3\2\2\2\u031d\u031f\5\u0154\u00ab\2\u031e\u031d\3\2\2\2\u031e"+
		"\u031f\3\2\2\2\u031f\u0325\3\2\2\2\u0320\u0326\5\u0092J\2\u0321\u0326"+
		"\5\u0094K\2\u0322\u0326\5\u0096L\2\u0323\u0326\5\u0098M\2\u0324\u0326"+
		"\5\u009aN\2\u0325\u0320\3\2\2\2\u0325\u0321\3\2\2\2\u0325\u0322\3\2\2"+
		"\2\u0325\u0323\3\2\2\2\u0325\u0324\3\2\2\2\u0326\u032a\3\2\2\2\u0327\u032a"+
		"\5\u0090I\2\u0328\u032a\5\u00acW\2\u0329\u031e\3\2\2\2\u0329\u0327\3\2"+
		"\2\2\u0329\u0328\3\2\2\2\u032a\u008f\3\2\2\2\u032b\u032c\7\u009b\2\2\u032c"+
		"\u0091\3\2\2\2\u032d\u032e\7\u009d\2\2\u032e\u0093\3\2\2\2\u032f\u0330"+
		"\7\u0097\2\2\u0330\u0095\3\2\2\2\u0331\u0332\7\u0098\2\2\u0332\u0097\3"+
		"\2\2\2\u0333\u0334\7\u0099\2\2\u0334\u0099\3\2\2\2\u0335\u0336\7\u009a"+
		"\2\2\u0336\u009b\3\2\2\2\u0337\u0338\5\u0156\u00ac\2\u0338\u033a\7}\2"+
		"\2\u0339\u033b\5\u012c\u0097\2\u033a\u0339\3\2\2\2\u033a\u033b\3\2\2\2"+
		"\u033b\u0344\3\2\2\2\u033c\u0341\5\u013c\u009f\2\u033d\u033e\7{\2\2\u033e"+
		"\u0340\5\u013c\u009f\2\u033f\u033d\3\2\2\2\u0340\u0343\3\2\2\2\u0341\u033f"+
		"\3\2\2\2\u0341\u0342\3\2\2\2\u0342\u0345\3\2\2\2\u0343\u0341\3\2\2\2\u0344"+
		"\u033c\3\2\2\2\u0344\u0345\3\2\2\2\u0345\u0346\3\2\2\2\u0346\u0347\7~"+
		"\2\2\u0347\u009d\3\2\2\2\u0348\u0349\7&\2\2\u0349\u034a\7}\2\2\u034a\u034b"+
		"\5X-\2\u034b\u034c\7*\2\2\u034c\u034d\5\34\17\2\u034d\u034e\7~\2\2\u034e"+
		"\u009f\3\2\2\2\u034f\u0350\7!\2\2\u0350\u0354\5\u00a4S\2\u0351\u0352\5"+
		"\u00a6T\2\u0352\u0353\5\u00a8U\2\u0353\u0355\3\2\2\2\u0354\u0351\3\2\2"+
		"\2\u0355\u0356\3\2\2\2\u0356\u0354\3\2\2\2\u0356\u0357\3\2\2\2\u0357\u0359"+
		"\3\2\2\2\u0358\u035a\5\u00aaV\2\u0359\u0358\3\2\2\2\u0359\u035a\3\2\2"+
		"\2\u035a\u035b\3\2\2\2\u035b\u035c\7%\2\2\u035c\u00a1\3\2\2\2\u035d\u0361"+
		"\7!\2\2\u035e\u035f\5\u00a6T\2\u035f\u0360\5\u00a8U\2\u0360\u0362\3\2"+
		"\2\2\u0361\u035e\3\2\2\2\u0362\u0363\3\2\2\2\u0363\u0361\3\2\2\2\u0363"+
		"\u0364\3\2\2\2\u0364\u0366\3\2\2\2\u0365\u0367\5\u00aaV\2\u0366\u0365"+
		"\3\2\2\2\u0366\u0367\3\2\2\2\u0367\u0368\3\2\2\2\u0368\u0369\7%\2\2\u0369"+
		"\u00a3\3\2\2\2\u036a\u036b\5X-\2\u036b\u00a5\3\2\2\2\u036c\u036d\7\"\2"+
		"\2\u036d\u036e\5X-\2\u036e\u00a7\3\2\2\2\u036f\u0370\7#\2\2\u0370\u0371"+
		"\5X-\2\u0371\u00a9\3\2\2\2\u0372\u0373\7$\2\2\u0373\u0374\5X-\2\u0374"+
		"\u00ab\3\2\2\2\u0375\u0376\t\b\2\2\u0376\u00ad\3\2\2\2\u0377\u0378\7}"+
		"\2\2\u0378\u037d\5X-\2\u0379\u037a\7{\2\2\u037a\u037c\5X-\2\u037b\u0379"+
		"\3\2\2\2\u037c\u037f\3\2\2\2\u037d\u037b\3\2\2\2\u037d\u037e\3\2\2\2\u037e"+
		"\u0380\3\2\2\2\u037f\u037d\3\2\2\2\u0380\u0381\7~\2\2\u0381\u00af\3\2"+
		"\2\2\u0382\u0383\5\u00c2b\2\u0383\u0384\5\u00ba^\2\u0384\u00b1\3\2\2\2"+
		"\u0385\u0386\5\u00c2b\2\u0386\u0387\5\u00b4[\2\u0387\u00b3\3\2\2\2\u0388"+
		"\u0389\7h\2\2\u0389\u038a\7g\2\2\u038a\u038b\5\u017e\u00c0\2\u038b\u038c"+
		"\78\2\2\u038c\u038e\5\u016c\u00b7\2\u038d\u038f\5\u00b6\\\2\u038e\u038d"+
		"\3\2\2\2\u038e\u038f\3\2\2\2\u038f\u0391\3\2\2\2\u0390\u0392\5\u00c0a"+
		"\2\u0391\u0390\3\2\2\2\u0391\u0392\3\2\2\2\u0392\u00b5\3\2\2\2\u0393\u0394"+
		"\7i\2\2\u0394\u0395\7\65\2\2\u0395\u0396\5\u0164\u00b3\2\u0396\u00b7\3"+
		"\2\2\2\u0397\u0399\5\u00c4c\2\u0398\u039a\5\u00bc_\2\u0399\u0398\3\2\2"+
		"\2\u039a\u039b\3\2\2\2\u039b\u0399\3\2\2\2\u039b\u039c\3\2\2\2\u039c\u039e"+
		"\3\2\2\2\u039d\u039f\5\u00c0a\2\u039e\u039d\3\2\2\2\u039e\u039f\3\2\2"+
		"\2\u039f\u00b9\3\2\2\2\u03a0\u03a1\5\u012e\u0098\2\u03a1\u03a3\5\u00c4"+
		"c\2\u03a2\u03a4\5\u0144\u00a3\2\u03a3\u03a2\3\2\2\2\u03a3\u03a4\3\2\2"+
		"\2\u03a4\u03a6\3\2\2\2\u03a5\u03a7\5\u011a\u008e\2\u03a6\u03a5\3\2\2\2"+
		"\u03a6\u03a7\3\2\2\2\u03a7\u03a9\3\2\2\2\u03a8\u03aa\5\u0120\u0091\2\u03a9"+
		"\u03a8\3\2\2\2\u03a9\u03aa\3\2\2\2\u03aa\u03ac\3\2\2\2\u03ab\u03ad\5\u0124"+
		"\u0093\2\u03ac\u03ab\3\2\2\2\u03ac\u03ad\3\2\2\2\u03ad\u03af\3\2\2\2\u03ae"+
		"\u03b0\5\u0126\u0094\2\u03af\u03ae\3\2\2\2\u03af\u03b0\3\2\2\2\u03b0\u03b2"+
		"\3\2\2\2\u03b1\u03b3\5\u00c0a\2\u03b2\u03b1\3\2\2\2\u03b2\u03b3\3\2\2"+
		"\2\u03b3\u00bb\3\2\2\2\u03b4\u03b5\5\u00c2b\2\u03b5\u03b6\5\u00be`\2\u03b6"+
		"\u00bd\3\2\2\2\u03b7\u03b9\5\u012e\u0098\2\u03b8\u03ba\5\u0144\u00a3\2"+
		"\u03b9\u03b8\3\2\2\2\u03b9\u03ba\3\2\2\2\u03ba\u03bc\3\2\2\2\u03bb\u03bd"+
		"\5\u011a\u008e\2\u03bc\u03bb\3\2\2\2\u03bc\u03bd\3\2\2\2\u03bd\u03bf\3"+
		"\2\2\2\u03be\u03c0\5\u0120\u0091\2\u03bf\u03be\3\2\2\2\u03bf\u03c0\3\2"+
		"\2\2\u03c0\u03c2\3\2\2\2\u03c1\u03c3\5\u0124\u0093\2\u03c2\u03c1\3\2\2"+
		"\2\u03c2\u03c3\3\2\2\2\u03c3\u03c5\3\2\2\2\u03c4\u03c6\5\u0126\u0094\2"+
		"\u03c5\u03c4\3\2\2\2\u03c5\u03c6\3\2\2\2\u03c6\u00bf\3\2\2\2\u03c7\u03c8"+
		"\7T\2\2\u03c8\u03c9\5\u0092J\2\u03c9\u00c1\3\2\2\2\u03ca\u03cb\7E\2\2"+
		"\u03cb\u03cd\7D\2\2\u03cc\u03ce\7\24\2\2\u03cd\u03cc\3\2\2\2\u03cd\u03ce"+
		"\3\2\2\2\u03ce\u03cf\3\2\2\2\u03cf\u03d0\5\u016c\u00b7\2\u03d0\u00c3\3"+
		"\2\2\2\u03d1\u03d2\78\2\2\u03d2\u03d4\5\u00c6d\2\u03d3\u03d5\5\u00ccg"+
		"\2\u03d4\u03d3\3\2\2\2\u03d4\u03d5\3\2\2\2\u03d5\u00c5\3\2\2\2\u03d6\u03da"+
		"\5\u00dco\2\u03d7\u03d9\5\u00c8e\2\u03d8\u03d7\3\2\2\2\u03d9\u03dc\3\2"+
		"\2\2\u03da\u03d8\3\2\2\2\u03da\u03db\3\2\2\2\u03db\u00c7\3\2\2\2\u03dc"+
		"\u03da\3\2\2\2\u03dd\u03de\5\u00ceh\2\u03de\u03e0\5\u00dco\2\u03df\u03e1"+
		"\5\u00caf\2\u03e0\u03df\3\2\2\2\u03e0\u03e1\3\2\2\2\u03e1\u00c9\3\2\2"+
		"\2\u03e2\u03e3\79\2\2\u03e3\u03e4\5X-\2\u03e4\u00cb\3\2\2\2\u03e5\u03e6"+
		"\7R\2\2\u03e6\u03e7\7}\2\2\u03e7\u03ea\5X-\2\u03e8\u03e9\7{\2\2\u03e9"+
		"\u03eb\5X-\2\u03ea\u03e8\3\2\2\2\u03eb\u03ec\3\2\2\2\u03ec\u03ea\3\2\2"+
		"\2\u03ec\u03ed\3\2\2\2\u03ed\u03ee\3\2\2\2\u03ee\u03ef\7~\2\2\u03ef\u00cd"+
		"\3\2\2\2\u03f0\u03f7\5\u00d0i\2\u03f1\u03f7\5\u00d2j\2\u03f2\u03f7\5\u00d4"+
		"k\2\u03f3\u03f7\5\u00d6l\2\u03f4\u03f7\5\u00d8m\2\u03f5\u03f7\5\u00da"+
		"n\2\u03f6\u03f0\3\2\2\2\u03f6\u03f1\3\2\2\2\u03f6\u03f2\3\2\2\2\u03f6"+
		"\u03f3\3\2\2\2\u03f6\u03f4\3\2\2\2\u03f6\u03f5\3\2\2\2\u03f7\u00cf\3\2"+
		"\2\2\u03f8\u03fc\7:\2\2\u03f9\u03fa\7A\2\2\u03fa\u03fc\7:\2\2\u03fb\u03f8"+
		"\3\2\2\2\u03fb\u03f9\3\2\2\2\u03fc\u00d1\3\2\2\2\u03fd\u03ff\7@\2\2\u03fe"+
		"\u0400\7=\2\2\u03ff\u03fe\3\2\2\2\u03ff\u0400\3\2\2\2\u0400\u0401\3\2"+
		"\2\2\u0401\u0402\7:\2\2\u0402\u00d3\3\2\2\2\u0403\u0405\7C\2\2\u0404\u0406"+
		"\7=\2\2\u0405\u0404\3\2\2\2\u0405\u0406\3\2\2\2\u0406\u0407\3\2\2\2\u0407"+
		"\u0408\7:\2\2\u0408\u00d5\3\2\2\2\u0409\u040b\7;\2\2\u040a\u040c\7=\2"+
		"\2\u040b\u040a\3\2\2\2\u040b\u040c\3\2\2\2\u040c\u040d\3\2\2\2\u040d\u040e"+
		"\7:\2\2\u040e\u00d7\3\2\2\2\u040f\u0413\7{\2\2\u0410\u0411\7>\2\2\u0411"+
		"\u0413\7:\2\2\u0412\u040f\3\2\2\2\u0412\u0410\3\2\2\2\u0413\u00d9\3\2"+
		"\2\2\u0414\u0415\7B\2\2\u0415\u0416\7:\2\2\u0416\u00db\3\2\2\2\u0417\u041a"+
		"\5\u00dep\2\u0418\u041a\5\u00e0q\2\u0419\u0417\3\2\2\2\u0419\u0418\3\2"+
		"\2\2\u041a\u00dd\3\2\2\2\u041b\u041d\5\u00eav\2\u041c\u041e\5\u00eex\2"+
		"\u041d\u041c\3\2\2\2\u041d\u041e\3\2\2\2\u041e\u0420\3\2\2\2\u041f\u0421"+
		"\5\u00f0y\2\u0420\u041f\3\2\2\2\u0420\u0421\3\2\2\2\u0421\u0423\3\2\2"+
		"\2\u0422\u0424\5\u0118\u008d\2\u0423\u0422\3\2\2\2\u0423\u0424\3\2\2\2"+
		"\u0424\u0426\3\2\2\2\u0425\u0427\5\u00ecw\2\u0426\u0425\3\2\2\2\u0426"+
		"\u0427\3\2\2\2\u0427\u00df\3\2\2\2\u0428\u0429\7`\2\2\u0429\u042a\5\u016e"+
		"\u00b8\2\u042a\u042c\5\u00e2r\2\u042b\u042d\5\u0118\u008d\2\u042c\u042b"+
		"\3\2\2\2\u042c\u042d\3\2\2\2\u042d\u00e1\3\2\2\2\u042e\u042f\7\177\2\2"+
		"\u042f\u0430\5\u00e4s\2\u0430\u0431\7{\2\2\u0431\u0432\5\u00e6t\2\u0432"+
		"\u0433\7\u0080\2\2\u0433\u00e3\3\2\2\2\u0434\u0435\7a\2\2\u0435\u0436"+
		"\7}\2\2\u0436\u0437\5&\24\2\u0437\u0438\7~\2\2\u0438\u00e5\3\2\2\2\u0439"+
		"\u043a\7b\2\2\u043a\u043c\7}\2\2\u043b\u043d\5\u00e8u\2\u043c\u043b\3"+
		"\2\2\2\u043c\u043d\3\2\2\2\u043d\u043e\3\2\2\2\u043e\u043f\7~\2\2\u043f"+
		"\u00e7\3\2\2\2\u0440\u0445\5X-\2\u0441\u0442\7{\2\2\u0442\u0444\5X-\2"+
		"\u0443\u0441\3\2\2\2\u0444\u0447\3\2\2\2\u0445\u0443\3\2\2\2\u0445\u0446"+
		"\3\2\2\2\u0446\u00e9\3\2\2\2\u0447\u0445\3\2\2\2\u0448\u044b\5\u016c\u00b7"+
		"\2\u0449\u044b\5\u0116\u008c\2\u044a\u0448\3\2\2\2\u044a\u0449\3\2\2\2"+
		"\u044b\u00eb\3\2\2\2\u044c\u044d\7S\2\2\u044d\u00ed\3\2\2\2\u044e\u044f"+
		"\7}\2\2\u044f\u0450\5\u0146\u00a4\2\u0450\u0451\7~\2\2\u0451\u00ef\3\2"+
		"\2\2\u0452\u0453\7\177\2\2\u0453\u0454\5\u00f2z\2\u0454\u0455\7\u0080"+
		"\2\2\u0455\u00f1\3\2\2\2\u0456\u045a\5\u00f6|\2\u0457\u045a\5\u00f4{\2"+
		"\u0458\u045a\5\u00f8}\2\u0459\u0456\3\2\2\2\u0459\u0457\3\2\2\2\u0459"+
		"\u0458\3\2\2\2\u045a\u00f3\3\2\2\2\u045b\u045c\7Z\2\2\u045c\u045d\5\u0092"+
		"J\2\u045d\u045e\5\u010a\u0086\2\u045e\u045f\5\u010c\u0087\2\u045f\u00f5"+
		"\3\2\2\2\u0460\u0461\7Y\2\2\u0461\u0463\5\u00fa~\2\u0462\u0464\5\u010a"+
		"\u0086\2\u0463\u0462\3\2\2\2\u0463\u0464\3\2\2\2\u0464\u0465\3\2\2\2\u0465"+
		"\u0466\5\u010c\u0087\2\u0466\u00f7\3\2\2\2\u0467\u0468\7Y\2\2\u0468\u0469"+
		"\7[\2\2\u0469\u046a\5X-\2\u046a\u046b\5\u010c\u0087\2\u046b\u00f9\3\2"+
		"\2\2\u046c\u046f\5\u00fe\u0080\2\u046d\u046f\5\u00fc\177\2\u046e\u046c"+
		"\3\2\2\2\u046e\u046d\3\2\2\2\u046f\u00fb\3\2\2\2\u0470\u0471\7\\\2\2\u0471"+
		"\u00fd\3\2\2\2\u0472\u0474\5\u0100\u0081\2\u0473\u0472\3\2\2\2\u0473\u0474"+
		"\3\2\2\2\u0474\u0476\3\2\2\2\u0475\u0477\5\u0102\u0082\2\u0476\u0475\3"+
		"\2\2\2\u0476\u0477\3\2\2\2\u0477\u0479\3\2\2\2\u0478\u047a\5\u0104\u0083"+
		"\2\u0479\u0478\3\2\2\2\u0479\u047a\3\2\2\2\u047a\u047c\3\2\2\2\u047b\u047d"+
		"\5\u0106\u0084\2\u047c\u047b\3\2\2\2\u047c\u047d\3\2\2\2\u047d\u047f\3"+
		"\2\2\2\u047e\u0480\5\u0108\u0085\2\u047f\u047e\3\2\2\2\u047f\u0480\3\2"+
		"\2\2\u0480\u00ff\3\2\2\2\u0481\u0482\5\u0092J\2\u0482\u0483\7j\2\2\u0483"+
		"\u0101\3\2\2\2\u0484\u0485\5\u0092J\2\u0485\u0486\7k\2\2\u0486\u0103\3"+
		"\2\2\2\u0487\u0488\5\u0092J\2\u0488\u0489\7l\2\2\u0489\u0105\3\2\2\2\u048a"+
		"\u048b\5\u0092J\2\u048b\u048c\7m\2\2\u048c\u0107\3\2\2\2\u048d\u048e\5"+
		"\u0092J\2\u048e\u048f\7n\2\2\u048f\u0109\3\2\2\2\u0490\u0491\t\t\2\2\u0491"+
		"\u010b\3\2\2\2\u0492\u0494\5\u010e\u0088\2\u0493\u0492\3\2\2\2\u0493\u0494"+
		"\3\2\2\2\u0494\u0496\3\2\2\2\u0495\u0497\5\u0110\u0089\2\u0496\u0495\3"+
		"\2\2\2\u0496\u0497\3\2\2\2\u0497\u0499\3\2\2\2\u0498\u049a\5\u0112\u008a"+
		"\2\u0499\u0498\3\2\2\2\u0499\u049a\3\2\2\2\u049a\u049c\3\2\2\2\u049b\u049d"+
		"\5\u0114\u008b\2\u049c\u049b\3\2\2\2\u049c\u049d\3\2\2\2\u049d\u010d\3"+
		"\2\2\2\u049e\u049f\7V\2\2\u049f\u04a0\7\65\2\2\u04a0\u04a1\5X-\2\u04a1"+
		"\u010f\3\2\2\2\u04a2\u04a3\7\62\2\2\u04a3\u04a4\7\65\2\2\u04a4\u04a5\5"+
		"X-\2\u04a5\u0111\3\2\2\2\u04a6\u04a7\7U\2\2\u04a7\u04a8\7\65\2\2\u04a8"+
		"\u04a9\5X-\2\u04a9\u0113\3\2\2\2\u04aa\u04ab\7]\2\2\u04ab\u04ac\7^\2\2"+
		"\u04ac\u0115\3\2\2\2\u04ad\u04ae\7}\2\2\u04ae\u04af\5\u00ba^\2\u04af\u04b0"+
		"\7~\2\2\u04b0\u0117\3\2\2\2\u04b1\u04b3\7*\2\2\u04b2\u04b1\3\2\2\2\u04b2"+
		"\u04b3\3\2\2\2\u04b3\u04b4\3\2\2\2\u04b4\u04b5\5\u0170\u00b9\2\u04b5\u0119"+
		"\3\2\2\2\u04b6\u04b7\7\64\2\2\u04b7\u04b8\7\65\2\2\u04b8\u04b9\5\u011c"+
		"\u008f\2\u04b9\u011b\3\2\2\2\u04ba\u04bf\5\u011e\u0090\2\u04bb\u04bc\7"+
		"{\2\2\u04bc\u04be\5\u011e\u0090\2\u04bd\u04bb\3\2\2\2\u04be\u04c1\3\2"+
		"\2\2\u04bf\u04bd\3\2\2\2\u04bf\u04c0\3\2\2\2\u04c0\u011d\3\2\2\2\u04c1"+
		"\u04bf\3\2\2\2\u04c2\u04c3\5X-\2\u04c3\u011f\3\2\2\2\u04c4\u04c5\7\66"+
		"\2\2\u04c5\u04c6\5\u0122\u0092\2\u04c6\u0121\3\2\2\2\u04c7\u04c8\5X-\2"+
		"\u04c8\u0123\3\2\2\2\u04c9\u04ca\7\63\2\2\u04ca\u04cb\7\65\2\2\u04cb\u04cc"+
		"\5\u013e\u00a0\2\u04cc\u0125\3\2\2\2\u04cd\u04ce\7G\2\2\u04ce\u04cf\5"+
		"\u012a\u0096\2\u04cf\u0127\3\2\2\2\u04d0\u04d1\7.\2\2\u04d1\u0129\3\2"+
		"\2\2\u04d2\u04d3\5\u0092J\2\u04d3\u012b\3\2\2\2\u04d4\u04d5\7\31\2\2\u04d5"+
		"\u012d\3\2\2\2\u04d6\u04d7\7\f\2\2\u04d7\u04d8\5\u0130\u0099\2\u04d8\u012f"+
		"\3\2\2\2\u04d9\u04db\5\u012c\u0097\2\u04da\u04d9\3\2\2\2\u04da\u04db\3"+
		"\2\2\2\u04db\u04dc\3\2\2\2\u04dc\u04dd\5\u0132\u009a\2\u04dd\u0131\3\2"+
		"\2\2\u04de\u04e3\5\u0134\u009b\2\u04df\u04e0\7{\2\2\u04e0\u04e2\5\u0134"+
		"\u009b\2\u04e1\u04df\3\2\2\2\u04e2\u04e5\3\2\2\2\u04e3\u04e1\3\2\2\2\u04e3"+
		"\u04e4\3\2\2\2\u04e4\u0133\3\2\2\2\u04e5\u04e3\3\2\2\2\u04e6\u04e7\5\u013c"+
		"\u009f\2\u04e7\u0135\3\2\2\2\u04e8\u04eb\5\u0138\u009d\2\u04e9\u04eb\5"+
		"\u013a\u009e\2\u04ea\u04e8\3\2\2\2\u04ea\u04e9\3\2\2\2\u04eb\u0137\3\2"+
		"\2\2\u04ec\u04ed\7*\2\2\u04ed\u04ee\7}\2\2\u04ee\u04f3\5\u0174\u00bb\2"+
		"\u04ef\u04f0\7{\2\2\u04f0\u04f2\5\u0174\u00bb\2\u04f1\u04ef\3\2\2\2\u04f2"+
		"\u04f5\3\2\2\2\u04f3\u04f1\3\2\2\2\u04f3\u04f4\3\2\2\2\u04f4\u04f6\3\2"+
		"\2\2\u04f5\u04f3\3\2\2\2\u04f6\u04f7\7~\2\2\u04f7\u0139\3\2\2\2\u04f8"+
		"\u04fa\7*\2\2\u04f9\u04f8\3\2\2\2\u04f9\u04fa\3\2\2\2\u04fa\u04fb\3\2"+
		"\2\2\u04fb\u04fc\5\u0174\u00bb\2\u04fc\u013b\3\2\2\2\u04fd\u04ff\5X-\2"+
		"\u04fe\u0500\5\u0136\u009c\2\u04ff\u04fe\3\2\2\2\u04ff\u0500\3\2\2\2\u0500"+
		"\u0503\3\2\2\2\u0501\u0503\5\u0148\u00a5\2\u0502\u04fd\3\2\2\2\u0502\u0501"+
		"\3\2\2\2\u0503\u013d\3\2\2\2\u0504\u0509\5\u0140\u00a1\2\u0505\u0506\7"+
		"{\2\2\u0506\u0508\5\u0140\u00a1\2\u0507\u0505\3\2\2\2\u0508\u050b\3\2"+
		"\2\2\u0509\u0507\3\2\2\2\u0509\u050a\3\2\2\2\u050a\u013f\3\2\2\2\u050b"+
		"\u0509\3\2\2\2\u050c\u050e\5X-\2\u050d\u050f\5\u0142\u00a2\2\u050e\u050d"+
		"\3\2\2\2\u050e\u050f\3\2\2\2\u050f\u0141\3\2\2\2\u0510\u0511\t\n\2\2\u0511"+
		"\u0143\3\2\2\2\u0512\u0513\7\67\2\2\u0513\u0514\5\u0146\u00a4\2\u0514"+
		"\u0145\3\2\2\2\u0515\u0516\5X-\2\u0516\u0147\3\2\2\2\u0517\u0518\5\u016c"+
		"\u00b7\2\u0518\u0519\7y\2\2\u0519\u051b\3\2\2\2\u051a\u0517\3\2\2\2\u051a"+
		"\u051b\3\2\2\2\u051b\u051c\3\2\2\2\u051c\u051d\7\u008e\2\2\u051d\u0149"+
		"\3\2\2\2\u051e\u051f\t\13\2\2\u051f\u014b\3\2\2\2\u0520\u0521\7\21\2\2"+
		"\u0521\u0522\7}\2\2\u0522\u0523\5(\25\2\u0523\u0524\7~\2\2\u0524\u014d"+
		"\3\2\2\2\u0525\u0526\7\u009b\2\2\u0526\u014f\3\2\2\2\u0527\u0528\7\u009b"+
		"\2\2\u0528\u0151\3\2\2\2\u0529\u052a\7\u009b\2\2\u052a\u0153\3\2\2\2\u052b"+
		"\u052c\t\f\2\2\u052c\u0155\3\2\2\2\u052d\u052e\5\u014a\u00a6\2\u052e\u0157"+
		"\3\2\2\2\u052f\u0530\5\u014a\u00a6\2\u0530\u0159\3\2\2\2\u0531\u0534\5"+
		"\u015c\u00af\2\u0532\u0534\5\u015e\u00b0\2\u0533\u0531\3\2\2\2\u0533\u0532"+
		"\3\2\2\2\u0534\u015b\3\2\2\2\u0535\u0536\5\u014a\u00a6\2\u0536\u015d\3"+
		"\2\2\2\u0537\u0538\7\u009b\2\2\u0538\u015f\3\2\2\2\u0539\u053a\7\u009b"+
		"\2\2\u053a\u0161\3\2\2\2\u053b\u053e\5\u014a\u00a6\2\u053c\u053e\5\u0092"+
		"J\2\u053d\u053b\3\2\2\2\u053d\u053c\3\2\2\2\u053e\u0163\3\2\2\2\u053f"+
		"\u0540\5\u014a\u00a6\2\u0540\u0165\3\2\2\2\u0541\u0542\7\16\2\2\u0542"+
		"\u0167\3\2\2\2\u0543\u0544\7(\2\2\u0544\u0545\7\'\2\2\u0545\u0169\3\2"+
		"\2\2\u0546\u0547\7(\2\2\u0547\u0548\7/\2\2\u0548\u0549\7\'\2\2\u0549\u016b"+
		"\3\2\2\2\u054a\u054b\5\u014a\u00a6\2\u054b\u016d\3\2\2\2\u054c\u054d\5"+
		"\u014a\u00a6\2\u054d\u016f\3\2\2\2\u054e\u054f\5\u014a\u00a6\2\u054f\u0171"+
		"\3\2\2\2\u0550\u0551\5\u014a\u00a6\2\u0551\u0173\3\2\2\2\u0552\u0553\5"+
		"\u014a\u00a6\2\u0553\u0175\3\2\2\2\u0554\u0555\7+\2\2\u0555\u0177\3\2"+
		"\2\2\u0556\u0557\7K\2\2\u0557\u0179\3\2\2\2\u0558\u0559\7/\2\2\u0559\u017b"+
		"\3\2\2\2\u055a\u055c\5\u017a\u00be\2\u055b\u055a\3\2\2\2\u055b\u055c\3"+
		"\2\2\2\u055c\u055d\3\2\2\2\u055d\u055e\7+\2\2\u055e\u017d\3\2\2\2\u055f"+
		"\u0560\5\u014a\u00a6\2\u0560\u017f\3\2\2\2y\u0189\u018f\u01a5\u01af\u01b2"+
		"\u01b5\u01b9\u01c3\u01c6\u01c9\u01cd\u01d6\u01d9\u01e1\u01eb\u01f9\u01fd"+
		"\u0201\u020a\u0216\u021e\u0244\u0249\u025d\u0262\u0269\u026e\u0270\u0275"+
		"\u027c\u027f\u0282\u029a\u02a2\u02a6\u02ac\u02b4\u02be\u02c9\u02d5\u02db"+
		"\u02ec\u02f7\u0302\u0308\u0317\u031e\u0325\u0329\u033a\u0341\u0344\u0356"+
		"\u0359\u0363\u0366\u037d\u038e\u0391\u039b\u039e\u03a3\u03a6\u03a9\u03ac"+
		"\u03af\u03b2\u03b9\u03bc\u03bf\u03c2\u03c5\u03cd\u03d4\u03da\u03e0\u03ec"+
		"\u03f6\u03fb\u03ff\u0405\u040b\u0412\u0419\u041d\u0420\u0423\u0426\u042c"+
		"\u043c\u0445\u044a\u0459\u0463\u046e\u0473\u0476\u0479\u047c\u047f\u0493"+
		"\u0496\u0499\u049c\u04b2\u04bf\u04da\u04e3\u04ea\u04f3\u04f9\u04ff\u0502"+
		"\u0509\u050e\u051a\u0533\u053d\u055b";
	public static final ATN _ATN =
		ATNSimulator.deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}