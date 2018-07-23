// Generated from CQLParser.g4 by ANTLR 4.1

package com.huawei.streaming.cql.semanticanalyzer.parser;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link CQLParser}.
 *
 * operations with no return type.
 */
public interface CQLParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link CQLParser#joinToken}.
	 */
	T visitJoinToken(@NotNull CQLParser.JoinTokenContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#unaryOperator}.
	 */
	T visitUnaryOperator(@NotNull CQLParser.UnaryOperatorContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#sourceDefine}.
	 */
	T visitSourceDefine(@NotNull CQLParser.SourceDefineContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#fullJoin}.
	 */
	T visitFullJoin(@NotNull CQLParser.FullJoinContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#whenExpression}.
	 */
	T visitWhenExpression(@NotNull CQLParser.WhenExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#multiInsertStatement}.
	 */
	T visitMultiInsertStatement(@NotNull CQLParser.MultiInsertStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#caseWhenElse}.
	 */
	T visitCaseWhenElse(@NotNull CQLParser.CaseWhenElseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeDay}.
	 */
	T visitRangeDay(@NotNull CQLParser.RangeDayContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#innerClassName}.
	 */
	T visitInnerClassName(@NotNull CQLParser.InnerClassNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rebalanceApplication}.
	 */
	T visitRebalanceApplication(@NotNull CQLParser.RebalanceApplicationContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#path}.
	 */
	T visitPath(@NotNull CQLParser.PathContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rightJoin}.
	 */
	T visitRightJoin(@NotNull CQLParser.RightJoinContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#constDoubleValue}.
	 */
	T visitConstDoubleValue(@NotNull CQLParser.ConstDoubleValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#fieldExpression}.
	 */
	T visitFieldExpression(@NotNull CQLParser.FieldExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#ifNotExists}.
	 */
	T visitIfNotExists(@NotNull CQLParser.IfNotExistsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#logicExpressionOr}.
	 */
	T visitLogicExpressionOr(@NotNull CQLParser.LogicExpressionOrContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#orderByClause}.
	 */
	T visitOrderByClause(@NotNull CQLParser.OrderByClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#streamPropertiesList}.
	 */
	T visitStreamPropertiesList(@NotNull CQLParser.StreamPropertiesListContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#datasourceQuery}.
	 */
	T visitDatasourceQuery(@NotNull CQLParser.DatasourceQueryContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#execStatement}.
	 */
	T visitExecStatement(@NotNull CQLParser.ExecStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#inputSchemaStatement}.
	 */
	T visitInputSchemaStatement(@NotNull CQLParser.InputSchemaStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#keyValueProperty}.
	 */
	T visitKeyValueProperty(@NotNull CQLParser.KeyValuePropertyContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#innerJoin}.
	 */
	T visitInnerJoin(@NotNull CQLParser.InnerJoinContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#multialias}.
	 */
	T visitMultialias(@NotNull CQLParser.MultialiasContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#precedenceEqualNegatableOperator}.
	 */
	T visitPrecedenceEqualNegatableOperator(@NotNull CQLParser.PrecedenceEqualNegatableOperatorContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#distributeClause}.
	 */
	T visitDistributeClause(@NotNull CQLParser.DistributeClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#columnNameTypeList}.
	 */
	T visitColumnNameTypeList(@NotNull CQLParser.ColumnNameTypeListContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expressionLike}.
	 */
	T visitExpressionLike(@NotNull CQLParser.ExpressionLikeContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#sortbyDeterminer}.
	 */
	T visitSortbyDeterminer(@NotNull CQLParser.SortbyDeterminerContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#subSelectClause}.
	 */
	T visitSubSelectClause(@NotNull CQLParser.SubSelectClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expressionWithLaparen}.
	 */
	T visitExpressionWithLaparen(@NotNull CQLParser.ExpressionWithLaparenContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#strValue}.
	 */
	T visitStrValue(@NotNull CQLParser.StrValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#havingCondition}.
	 */
	T visitHavingCondition(@NotNull CQLParser.HavingConditionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#outputSchemaStatement}.
	 */
	T visitOutputSchemaStatement(@NotNull CQLParser.OutputSchemaStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#onCondition}.
	 */
	T visitOnCondition(@NotNull CQLParser.OnConditionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#insertStatement}.
	 */
	T visitInsertStatement(@NotNull CQLParser.InsertStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#sourceClause}.
	 */
	T visitSourceClause(@NotNull CQLParser.SourceClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#datasourceSchema}.
	 */
	T visitDatasourceSchema(@NotNull CQLParser.DatasourceSchemaContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#triggerbyDeterminer}.
	 */
	T visitTriggerbyDeterminer(@NotNull CQLParser.TriggerbyDeterminerContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeToday}.
	 */
	T visitRangeToday(@NotNull CQLParser.RangeTodayContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#createInputStreamStatement}.
	 */
	T visitCreateInputStreamStatement(@NotNull CQLParser.CreateInputStreamStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#windowDeterminer}.
	 */
	T visitWindowDeterminer(@NotNull CQLParser.WindowDeterminerContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#ddlStatement}.
	 */
	T visitDdlStatement(@NotNull CQLParser.DdlStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#excludeNowDeterminer}.
	 */
	T visitExcludeNowDeterminer(@NotNull CQLParser.ExcludeNowDeterminerContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#isNullLikeInExpressions}.
	 */
	T visitIsNullLikeInExpressions(@NotNull CQLParser.IsNullLikeInExpressionsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#fromSource}.
	 */
	T visitFromSource(@NotNull CQLParser.FromSourceContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#serdeDefine}.
	 */
	T visitSerdeDefine(@NotNull CQLParser.SerdeDefineContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#multiSelect}.
	 */
	T visitMultiSelect(@NotNull CQLParser.MultiSelectContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#userDefinedClassName}.
	 */
	T visitUserDefinedClassName(@NotNull CQLParser.UserDefinedClassNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#streamNameOrAlias}.
	 */
	T visitStreamNameOrAlias(@NotNull CQLParser.StreamNameOrAliasContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#columnALias}.
	 */
	T visitColumnALias(@NotNull CQLParser.ColumnALiasContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#sourceProperties}.
	 */
	T visitSourceProperties(@NotNull CQLParser.SourcePropertiesContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#nullCondition}.
	 */
	T visitNullCondition(@NotNull CQLParser.NullConditionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#arithmeticStarExpression}.
	 */
	T visitArithmeticStarExpression(@NotNull CQLParser.ArithmeticStarExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#constNull}.
	 */
	T visitConstNull(@NotNull CQLParser.ConstNullContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#columnNameOrder}.
	 */
	T visitColumnNameOrder(@NotNull CQLParser.ColumnNameOrderContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#constLongValue}.
	 */
	T visitConstLongValue(@NotNull CQLParser.ConstLongValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#multiInsert}.
	 */
	T visitMultiInsert(@NotNull CQLParser.MultiInsertContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#relationExpression}.
	 */
	T visitRelationExpression(@NotNull CQLParser.RelationExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#className}.
	 */
	T visitClassName(@NotNull CQLParser.ClassNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#operatorName}.
	 */
	T visitOperatorName(@NotNull CQLParser.OperatorNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#windowBody}.
	 */
	T visitWindowBody(@NotNull CQLParser.WindowBodyContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#extended}.
	 */
	T visitExtended(@NotNull CQLParser.ExtendedContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#logicExpressionNot}.
	 */
	T visitLogicExpressionNot(@NotNull CQLParser.LogicExpressionNotContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#confValue}.
	 */
	T visitConfValue(@NotNull CQLParser.ConfValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#constStingValue}.
	 */
	T visitConstStingValue(@NotNull CQLParser.ConstStingValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#selectClause}.
	 */
	T visitSelectClause(@NotNull CQLParser.SelectClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#setStatement}.
	 */
	T visitSetStatement(@NotNull CQLParser.SetStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#atomExpression}.
	 */
	T visitAtomExpression(@NotNull CQLParser.AtomExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#colType}.
	 */
	T visitColType(@NotNull CQLParser.ColTypeContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#constFloatValue}.
	 */
	T visitConstFloatValue(@NotNull CQLParser.ConstFloatValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#usingStatement}.
	 */
	T visitUsingStatement(@NotNull CQLParser.UsingStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#isForce}.
	 */
	T visitIsForce(@NotNull CQLParser.IsForceContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#serdeProperties}.
	 */
	T visitSerdeProperties(@NotNull CQLParser.SerdePropertiesContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#subQueryExpression}.
	 */
	T visitSubQueryExpression(@NotNull CQLParser.SubQueryExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#applicationName}.
	 */
	T visitApplicationName(@NotNull CQLParser.ApplicationNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#havingClause}.
	 */
	T visitHavingClause(@NotNull CQLParser.HavingClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expression}.
	 */
	T visitExpression(@NotNull CQLParser.ExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#bitOperator}.
	 */
	T visitBitOperator(@NotNull CQLParser.BitOperatorContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#fromClause}.
	 */
	T visitFromClause(@NotNull CQLParser.FromClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#windowSource}.
	 */
	T visitWindowSource(@NotNull CQLParser.WindowSourceContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#streamProperties}.
	 */
	T visitStreamProperties(@NotNull CQLParser.StreamPropertiesContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#searchCondition}.
	 */
	T visitSearchCondition(@NotNull CQLParser.SearchConditionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#selectItem}.
	 */
	T visitSelectItem(@NotNull CQLParser.SelectItemContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#createOperatorStatement}.
	 */
	T visitCreateOperatorStatement(@NotNull CQLParser.CreateOperatorStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#joinRigthBody}.
	 */
	T visitJoinRigthBody(@NotNull CQLParser.JoinRigthBodyContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expressionBetween}.
	 */
	T visitExpressionBetween(@NotNull CQLParser.ExpressionBetweenContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#insertUserOperatorStatement}.
	 */
	T visitInsertUserOperatorStatement(@NotNull CQLParser.InsertUserOperatorStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#dataSourceName}.
	 */
	T visitDataSourceName(@NotNull CQLParser.DataSourceNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#logicExpressionAnd}.
	 */
	T visitLogicExpressionAnd(@NotNull CQLParser.LogicExpressionAndContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#datasourceProperties}.
	 */
	T visitDatasourceProperties(@NotNull CQLParser.DatasourcePropertiesContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeBound}.
	 */
	T visitRangeBound(@NotNull CQLParser.RangeBoundContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#limitRow}.
	 */
	T visitLimitRow(@NotNull CQLParser.LimitRowContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expressionPrevious}.
	 */
	T visitExpressionPrevious(@NotNull CQLParser.ExpressionPreviousContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#ifExists}.
	 */
	T visitIfExists(@NotNull CQLParser.IfExistsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#serdeClass}.
	 */
	T visitSerdeClass(@NotNull CQLParser.SerdeClassContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#createPipeStreamStatement}.
	 */
	T visitCreatePipeStreamStatement(@NotNull CQLParser.CreatePipeStreamStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#showFunctions}.
	 */
	T visitShowFunctions(@NotNull CQLParser.ShowFunctionsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#equalRelationExpression}.
	 */
	T visitEqualRelationExpression(@NotNull CQLParser.EqualRelationExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#windowProperties}.
	 */
	T visitWindowProperties(@NotNull CQLParser.WindowPropertiesContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeWindow}.
	 */
	T visitRangeWindow(@NotNull CQLParser.RangeWindowContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#columnName}.
	 */
	T visitColumnName(@NotNull CQLParser.ColumnNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#groupByList}.
	 */
	T visitGroupByList(@NotNull CQLParser.GroupByListContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeMinutes}.
	 */
	T visitRangeMinutes(@NotNull CQLParser.RangeMinutesContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#whereClause}.
	 */
	T visitWhereClause(@NotNull CQLParser.WhereClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#dropFunctionStatement}.
	 */
	T visitDropFunctionStatement(@NotNull CQLParser.DropFunctionStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#constant}.
	 */
	T visitConstant(@NotNull CQLParser.ConstantContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#streamBody}.
	 */
	T visitStreamBody(@NotNull CQLParser.StreamBodyContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#createDataSourceStatement}.
	 */
	T visitCreateDataSourceStatement(@NotNull CQLParser.CreateDataSourceStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#partitionbyDeterminer}.
	 */
	T visitPartitionbyDeterminer(@NotNull CQLParser.PartitionbyDeterminerContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#relationOperator}.
	 */
	T visitRelationOperator(@NotNull CQLParser.RelationOperatorContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#caseHeadExpression}.
	 */
	T visitCaseHeadExpression(@NotNull CQLParser.CaseHeadExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#limitAll}.
	 */
	T visitLimitAll(@NotNull CQLParser.LimitAllContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#confName}.
	 */
	T visitConfName(@NotNull CQLParser.ConfNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#castExpression}.
	 */
	T visitCastExpression(@NotNull CQLParser.CastExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#columnNameType}.
	 */
	T visitColumnNameType(@NotNull CQLParser.ColumnNameTypeContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#function}.
	 */
	T visitFunction(@NotNull CQLParser.FunctionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#groupByClause}.
	 */
	T visitGroupByClause(@NotNull CQLParser.GroupByClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeTime}.
	 */
	T visitRangeTime(@NotNull CQLParser.RangeTimeContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#selectAlias}.
	 */
	T visitSelectAlias(@NotNull CQLParser.SelectAliasContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#constIntegerValue}.
	 */
	T visitConstIntegerValue(@NotNull CQLParser.ConstIntegerValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#windowName}.
	 */
	T visitWindowName(@NotNull CQLParser.WindowNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#selectStatement}.
	 */
	T visitSelectStatement(@NotNull CQLParser.SelectStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#sinkClause}.
	 */
	T visitSinkClause(@NotNull CQLParser.SinkClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#getStatement}.
	 */
	T visitGetStatement(@NotNull CQLParser.GetStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expressionBetweenMinValue}.
	 */
	T visitExpressionBetweenMinValue(@NotNull CQLParser.ExpressionBetweenMinValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#identifierNot}.
	 */
	T visitIdentifierNot(@NotNull CQLParser.IdentifierNotContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#datasourceBody}.
	 */
	T visitDatasourceBody(@NotNull CQLParser.DatasourceBodyContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#caseWhenBodyWhenBody}.
	 */
	T visitCaseWhenBodyWhenBody(@NotNull CQLParser.CaseWhenBodyWhenBodyContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#showApplications}.
	 */
	T visitShowApplications(@NotNull CQLParser.ShowApplicationsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#insertClause}.
	 */
	T visitInsertClause(@NotNull CQLParser.InsertClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#submitApplication}.
	 */
	T visitSubmitApplication(@NotNull CQLParser.SubmitApplicationContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expressions}.
	 */
	T visitExpressions(@NotNull CQLParser.ExpressionsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#binaryExpression}.
	 */
	T visitBinaryExpression(@NotNull CQLParser.BinaryExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#explainStatement}.
	 */
	T visitExplainStatement(@NotNull CQLParser.ExplainStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#createFunctionStatement}.
	 */
	T visitCreateFunctionStatement(@NotNull CQLParser.CreateFunctionStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#joinSource}.
	 */
	T visitJoinSource(@NotNull CQLParser.JoinSourceContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#datasourceQueryArguments}.
	 */
	T visitDatasourceQueryArguments(@NotNull CQLParser.DatasourceQueryArgumentsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#leftJoin}.
	 */
	T visitLeftJoin(@NotNull CQLParser.LeftJoinContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#booleanValue}.
	 */
	T visitBooleanValue(@NotNull CQLParser.BooleanValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#unidirection}.
	 */
	T visitUnidirection(@NotNull CQLParser.UnidirectionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expressionIn}.
	 */
	T visitExpressionIn(@NotNull CQLParser.ExpressionInContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#groupByExpression}.
	 */
	T visitGroupByExpression(@NotNull CQLParser.GroupByExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#sourceAlias}.
	 */
	T visitSourceAlias(@NotNull CQLParser.SourceAliasContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rowsWindow}.
	 */
	T visitRowsWindow(@NotNull CQLParser.RowsWindowContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#columnOrder}.
	 */
	T visitColumnOrder(@NotNull CQLParser.ColumnOrderContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#datasourceArguments}.
	 */
	T visitDatasourceArguments(@NotNull CQLParser.DatasourceArgumentsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#deactiveApplication}.
	 */
	T visitDeactiveApplication(@NotNull CQLParser.DeactiveApplicationContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#combineCondition}.
	 */
	T visitCombineCondition(@NotNull CQLParser.CombineConditionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeUnBound}.
	 */
	T visitRangeUnBound(@NotNull CQLParser.RangeUnBoundContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#distinct}.
	 */
	T visitDistinct(@NotNull CQLParser.DistinctContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#streamSource}.
	 */
	T visitStreamSource(@NotNull CQLParser.StreamSourceContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#arithmeticPlusOperator}.
	 */
	T visitArithmeticPlusOperator(@NotNull CQLParser.ArithmeticPlusOperatorContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#sinkProperties}.
	 */
	T visitSinkProperties(@NotNull CQLParser.SinkPropertiesContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expressionBetweenMaxValue}.
	 */
	T visitExpressionBetweenMaxValue(@NotNull CQLParser.ExpressionBetweenMaxValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#crossJoin}.
	 */
	T visitCrossJoin(@NotNull CQLParser.CrossJoinContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#selectExpression}.
	 */
	T visitSelectExpression(@NotNull CQLParser.SelectExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#expressionExists}.
	 */
	T visitExpressionExists(@NotNull CQLParser.ExpressionExistsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#naturalJoin}.
	 */
	T visitNaturalJoin(@NotNull CQLParser.NaturalJoinContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeSeconds}.
	 */
	T visitRangeSeconds(@NotNull CQLParser.RangeSecondsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#columnNameOrderList}.
	 */
	T visitColumnNameOrderList(@NotNull CQLParser.ColumnNameOrderListContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#caseExpression}.
	 */
	T visitCaseExpression(@NotNull CQLParser.CaseExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#statement}.
	 */
	T visitStatement(@NotNull CQLParser.StatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#activeApplication}.
	 */
	T visitActiveApplication(@NotNull CQLParser.ActiveApplicationContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#addJARStatement}.
	 */
	T visitAddJARStatement(@NotNull CQLParser.AddJARStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#constBigDecimalValue}.
	 */
	T visitConstBigDecimalValue(@NotNull CQLParser.ConstBigDecimalValueContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#subQuerySource}.
	 */
	T visitSubQuerySource(@NotNull CQLParser.SubQuerySourceContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#primitiveType}.
	 */
	T visitPrimitiveType(@NotNull CQLParser.PrimitiveTypeContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#addFileStatement}.
	 */
	T visitAddFileStatement(@NotNull CQLParser.AddFileStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#parallelClause}.
	 */
	T visitParallelClause(@NotNull CQLParser.ParallelClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#createOutputStreamStatement}.
	 */
	T visitCreateOutputStreamStatement(@NotNull CQLParser.CreateOutputStreamStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#functionName}.
	 */
	T visitFunctionName(@NotNull CQLParser.FunctionNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#loadStatement}.
	 */
	T visitLoadStatement(@NotNull CQLParser.LoadStatementContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#streamAlias}.
	 */
	T visitStreamAlias(@NotNull CQLParser.StreamAliasContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#limitClause}.
	 */
	T visitLimitClause(@NotNull CQLParser.LimitClauseContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeHour}.
	 */
	T visitRangeHour(@NotNull CQLParser.RangeHourContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#cqlIdentifier}.
	 */
	T visitCqlIdentifier(@NotNull CQLParser.CqlIdentifierContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#streamName}.
	 */
	T visitStreamName(@NotNull CQLParser.StreamNameContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#dropApplication}.
	 */
	T visitDropApplication(@NotNull CQLParser.DropApplicationContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#arithmeticPlusMinusExpression}.
	 */
	T visitArithmeticPlusMinusExpression(@NotNull CQLParser.ArithmeticPlusMinusExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#caseWhenBodyThenBody}.
	 */
	T visitCaseWhenBodyThenBody(@NotNull CQLParser.CaseWhenBodyThenBodyContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#selectList}.
	 */
	T visitSelectList(@NotNull CQLParser.SelectListContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#streamAllColumns}.
	 */
	T visitStreamAllColumns(@NotNull CQLParser.StreamAllColumnsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#commentString}.
	 */
	T visitCommentString(@NotNull CQLParser.CommentStringContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#singleAlias}.
	 */
	T visitSingleAlias(@NotNull CQLParser.SingleAliasContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#bitExpression}.
	 */
	T visitBitExpression(@NotNull CQLParser.BitExpressionContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#rangeMilliSeconds}.
	 */
	T visitRangeMilliSeconds(@NotNull CQLParser.RangeMilliSecondsContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#sinkDefine}.
	 */
	T visitSinkDefine(@NotNull CQLParser.SinkDefineContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#comment}.
	 */
	T visitComment(@NotNull CQLParser.CommentContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#filterBeforeWindow}.
	 */
	T visitFilterBeforeWindow(@NotNull CQLParser.FilterBeforeWindowContext ctx);

	/**
	 * Visit a parse tree produced by {@link CQLParser#arithmeticStarOperator}.
	 */
	T visitArithmeticStarOperator(@NotNull CQLParser.ArithmeticStarOperatorContext ctx);
}
