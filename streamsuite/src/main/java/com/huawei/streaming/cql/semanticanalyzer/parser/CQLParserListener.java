// Generated from CQLParser.g4 by ANTLR 4.1

package com.huawei.streaming.cql.semanticanalyzer.parser;

import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link CQLParser}.
 */
public interface CQLParserListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link CQLParser#joinToken}.
	 */
	void enterJoinToken(@NotNull CQLParser.JoinTokenContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#joinToken}.
	 */
	void exitJoinToken(@NotNull CQLParser.JoinTokenContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#unaryOperator}.
	 */
	void enterUnaryOperator(@NotNull CQLParser.UnaryOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#unaryOperator}.
	 */
	void exitUnaryOperator(@NotNull CQLParser.UnaryOperatorContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#sourceDefine}.
	 */
	void enterSourceDefine(@NotNull CQLParser.SourceDefineContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#sourceDefine}.
	 */
	void exitSourceDefine(@NotNull CQLParser.SourceDefineContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#fullJoin}.
	 */
	void enterFullJoin(@NotNull CQLParser.FullJoinContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#fullJoin}.
	 */
	void exitFullJoin(@NotNull CQLParser.FullJoinContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#whenExpression}.
	 */
	void enterWhenExpression(@NotNull CQLParser.WhenExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#whenExpression}.
	 */
	void exitWhenExpression(@NotNull CQLParser.WhenExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#multiInsertStatement}.
	 */
	void enterMultiInsertStatement(@NotNull CQLParser.MultiInsertStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#multiInsertStatement}.
	 */
	void exitMultiInsertStatement(@NotNull CQLParser.MultiInsertStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#caseWhenElse}.
	 */
	void enterCaseWhenElse(@NotNull CQLParser.CaseWhenElseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#caseWhenElse}.
	 */
	void exitCaseWhenElse(@NotNull CQLParser.CaseWhenElseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeDay}.
	 */
	void enterRangeDay(@NotNull CQLParser.RangeDayContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeDay}.
	 */
	void exitRangeDay(@NotNull CQLParser.RangeDayContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#innerClassName}.
	 */
	void enterInnerClassName(@NotNull CQLParser.InnerClassNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#innerClassName}.
	 */
	void exitInnerClassName(@NotNull CQLParser.InnerClassNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rebalanceApplication}.
	 */
	void enterRebalanceApplication(@NotNull CQLParser.RebalanceApplicationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rebalanceApplication}.
	 */
	void exitRebalanceApplication(@NotNull CQLParser.RebalanceApplicationContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#path}.
	 */
	void enterPath(@NotNull CQLParser.PathContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#path}.
	 */
	void exitPath(@NotNull CQLParser.PathContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rightJoin}.
	 */
	void enterRightJoin(@NotNull CQLParser.RightJoinContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rightJoin}.
	 */
	void exitRightJoin(@NotNull CQLParser.RightJoinContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#constDoubleValue}.
	 */
	void enterConstDoubleValue(@NotNull CQLParser.ConstDoubleValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#constDoubleValue}.
	 */
	void exitConstDoubleValue(@NotNull CQLParser.ConstDoubleValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#fieldExpression}.
	 */
	void enterFieldExpression(@NotNull CQLParser.FieldExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#fieldExpression}.
	 */
	void exitFieldExpression(@NotNull CQLParser.FieldExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#ifNotExists}.
	 */
	void enterIfNotExists(@NotNull CQLParser.IfNotExistsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#ifNotExists}.
	 */
	void exitIfNotExists(@NotNull CQLParser.IfNotExistsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#logicExpressionOr}.
	 */
	void enterLogicExpressionOr(@NotNull CQLParser.LogicExpressionOrContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#logicExpressionOr}.
	 */
	void exitLogicExpressionOr(@NotNull CQLParser.LogicExpressionOrContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#orderByClause}.
	 */
	void enterOrderByClause(@NotNull CQLParser.OrderByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#orderByClause}.
	 */
	void exitOrderByClause(@NotNull CQLParser.OrderByClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#streamPropertiesList}.
	 */
	void enterStreamPropertiesList(@NotNull CQLParser.StreamPropertiesListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#streamPropertiesList}.
	 */
	void exitStreamPropertiesList(@NotNull CQLParser.StreamPropertiesListContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#datasourceQuery}.
	 */
	void enterDatasourceQuery(@NotNull CQLParser.DatasourceQueryContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#datasourceQuery}.
	 */
	void exitDatasourceQuery(@NotNull CQLParser.DatasourceQueryContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#execStatement}.
	 */
	void enterExecStatement(@NotNull CQLParser.ExecStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#execStatement}.
	 */
	void exitExecStatement(@NotNull CQLParser.ExecStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#inputSchemaStatement}.
	 */
	void enterInputSchemaStatement(@NotNull CQLParser.InputSchemaStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#inputSchemaStatement}.
	 */
	void exitInputSchemaStatement(@NotNull CQLParser.InputSchemaStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#keyValueProperty}.
	 */
	void enterKeyValueProperty(@NotNull CQLParser.KeyValuePropertyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#keyValueProperty}.
	 */
	void exitKeyValueProperty(@NotNull CQLParser.KeyValuePropertyContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#innerJoin}.
	 */
	void enterInnerJoin(@NotNull CQLParser.InnerJoinContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#innerJoin}.
	 */
	void exitInnerJoin(@NotNull CQLParser.InnerJoinContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#multialias}.
	 */
	void enterMultialias(@NotNull CQLParser.MultialiasContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#multialias}.
	 */
	void exitMultialias(@NotNull CQLParser.MultialiasContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#precedenceEqualNegatableOperator}.
	 */
	void enterPrecedenceEqualNegatableOperator(@NotNull CQLParser.PrecedenceEqualNegatableOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#precedenceEqualNegatableOperator}.
	 */
	void exitPrecedenceEqualNegatableOperator(@NotNull CQLParser.PrecedenceEqualNegatableOperatorContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#distributeClause}.
	 */
	void enterDistributeClause(@NotNull CQLParser.DistributeClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#distributeClause}.
	 */
	void exitDistributeClause(@NotNull CQLParser.DistributeClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#columnNameTypeList}.
	 */
	void enterColumnNameTypeList(@NotNull CQLParser.ColumnNameTypeListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#columnNameTypeList}.
	 */
	void exitColumnNameTypeList(@NotNull CQLParser.ColumnNameTypeListContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expressionLike}.
	 */
	void enterExpressionLike(@NotNull CQLParser.ExpressionLikeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expressionLike}.
	 */
	void exitExpressionLike(@NotNull CQLParser.ExpressionLikeContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#sortbyDeterminer}.
	 */
	void enterSortbyDeterminer(@NotNull CQLParser.SortbyDeterminerContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#sortbyDeterminer}.
	 */
	void exitSortbyDeterminer(@NotNull CQLParser.SortbyDeterminerContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#subSelectClause}.
	 */
	void enterSubSelectClause(@NotNull CQLParser.SubSelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#subSelectClause}.
	 */
	void exitSubSelectClause(@NotNull CQLParser.SubSelectClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expressionWithLaparen}.
	 */
	void enterExpressionWithLaparen(@NotNull CQLParser.ExpressionWithLaparenContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expressionWithLaparen}.
	 */
	void exitExpressionWithLaparen(@NotNull CQLParser.ExpressionWithLaparenContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#strValue}.
	 */
	void enterStrValue(@NotNull CQLParser.StrValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#strValue}.
	 */
	void exitStrValue(@NotNull CQLParser.StrValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#havingCondition}.
	 */
	void enterHavingCondition(@NotNull CQLParser.HavingConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#havingCondition}.
	 */
	void exitHavingCondition(@NotNull CQLParser.HavingConditionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#outputSchemaStatement}.
	 */
	void enterOutputSchemaStatement(@NotNull CQLParser.OutputSchemaStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#outputSchemaStatement}.
	 */
	void exitOutputSchemaStatement(@NotNull CQLParser.OutputSchemaStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#onCondition}.
	 */
	void enterOnCondition(@NotNull CQLParser.OnConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#onCondition}.
	 */
	void exitOnCondition(@NotNull CQLParser.OnConditionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#insertStatement}.
	 */
	void enterInsertStatement(@NotNull CQLParser.InsertStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#insertStatement}.
	 */
	void exitInsertStatement(@NotNull CQLParser.InsertStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#sourceClause}.
	 */
	void enterSourceClause(@NotNull CQLParser.SourceClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#sourceClause}.
	 */
	void exitSourceClause(@NotNull CQLParser.SourceClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#datasourceSchema}.
	 */
	void enterDatasourceSchema(@NotNull CQLParser.DatasourceSchemaContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#datasourceSchema}.
	 */
	void exitDatasourceSchema(@NotNull CQLParser.DatasourceSchemaContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#triggerbyDeterminer}.
	 */
	void enterTriggerbyDeterminer(@NotNull CQLParser.TriggerbyDeterminerContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#triggerbyDeterminer}.
	 */
	void exitTriggerbyDeterminer(@NotNull CQLParser.TriggerbyDeterminerContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeToday}.
	 */
	void enterRangeToday(@NotNull CQLParser.RangeTodayContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeToday}.
	 */
	void exitRangeToday(@NotNull CQLParser.RangeTodayContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#createInputStreamStatement}.
	 */
	void enterCreateInputStreamStatement(@NotNull CQLParser.CreateInputStreamStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#createInputStreamStatement}.
	 */
	void exitCreateInputStreamStatement(@NotNull CQLParser.CreateInputStreamStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#windowDeterminer}.
	 */
	void enterWindowDeterminer(@NotNull CQLParser.WindowDeterminerContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#windowDeterminer}.
	 */
	void exitWindowDeterminer(@NotNull CQLParser.WindowDeterminerContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#ddlStatement}.
	 */
	void enterDdlStatement(@NotNull CQLParser.DdlStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#ddlStatement}.
	 */
	void exitDdlStatement(@NotNull CQLParser.DdlStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#excludeNowDeterminer}.
	 */
	void enterExcludeNowDeterminer(@NotNull CQLParser.ExcludeNowDeterminerContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#excludeNowDeterminer}.
	 */
	void exitExcludeNowDeterminer(@NotNull CQLParser.ExcludeNowDeterminerContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#isNullLikeInExpressions}.
	 */
	void enterIsNullLikeInExpressions(@NotNull CQLParser.IsNullLikeInExpressionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#isNullLikeInExpressions}.
	 */
	void exitIsNullLikeInExpressions(@NotNull CQLParser.IsNullLikeInExpressionsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#fromSource}.
	 */
	void enterFromSource(@NotNull CQLParser.FromSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#fromSource}.
	 */
	void exitFromSource(@NotNull CQLParser.FromSourceContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#serdeDefine}.
	 */
	void enterSerdeDefine(@NotNull CQLParser.SerdeDefineContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#serdeDefine}.
	 */
	void exitSerdeDefine(@NotNull CQLParser.SerdeDefineContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#multiSelect}.
	 */
	void enterMultiSelect(@NotNull CQLParser.MultiSelectContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#multiSelect}.
	 */
	void exitMultiSelect(@NotNull CQLParser.MultiSelectContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#userDefinedClassName}.
	 */
	void enterUserDefinedClassName(@NotNull CQLParser.UserDefinedClassNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#userDefinedClassName}.
	 */
	void exitUserDefinedClassName(@NotNull CQLParser.UserDefinedClassNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#streamNameOrAlias}.
	 */
	void enterStreamNameOrAlias(@NotNull CQLParser.StreamNameOrAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#streamNameOrAlias}.
	 */
	void exitStreamNameOrAlias(@NotNull CQLParser.StreamNameOrAliasContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#columnALias}.
	 */
	void enterColumnALias(@NotNull CQLParser.ColumnALiasContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#columnALias}.
	 */
	void exitColumnALias(@NotNull CQLParser.ColumnALiasContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#sourceProperties}.
	 */
	void enterSourceProperties(@NotNull CQLParser.SourcePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#sourceProperties}.
	 */
	void exitSourceProperties(@NotNull CQLParser.SourcePropertiesContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#nullCondition}.
	 */
	void enterNullCondition(@NotNull CQLParser.NullConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#nullCondition}.
	 */
	void exitNullCondition(@NotNull CQLParser.NullConditionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#arithmeticStarExpression}.
	 */
	void enterArithmeticStarExpression(@NotNull CQLParser.ArithmeticStarExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#arithmeticStarExpression}.
	 */
	void exitArithmeticStarExpression(@NotNull CQLParser.ArithmeticStarExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#constNull}.
	 */
	void enterConstNull(@NotNull CQLParser.ConstNullContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#constNull}.
	 */
	void exitConstNull(@NotNull CQLParser.ConstNullContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#columnNameOrder}.
	 */
	void enterColumnNameOrder(@NotNull CQLParser.ColumnNameOrderContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#columnNameOrder}.
	 */
	void exitColumnNameOrder(@NotNull CQLParser.ColumnNameOrderContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#constLongValue}.
	 */
	void enterConstLongValue(@NotNull CQLParser.ConstLongValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#constLongValue}.
	 */
	void exitConstLongValue(@NotNull CQLParser.ConstLongValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#multiInsert}.
	 */
	void enterMultiInsert(@NotNull CQLParser.MultiInsertContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#multiInsert}.
	 */
	void exitMultiInsert(@NotNull CQLParser.MultiInsertContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#relationExpression}.
	 */
	void enterRelationExpression(@NotNull CQLParser.RelationExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#relationExpression}.
	 */
	void exitRelationExpression(@NotNull CQLParser.RelationExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#className}.
	 */
	void enterClassName(@NotNull CQLParser.ClassNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#className}.
	 */
	void exitClassName(@NotNull CQLParser.ClassNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#operatorName}.
	 */
	void enterOperatorName(@NotNull CQLParser.OperatorNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#operatorName}.
	 */
	void exitOperatorName(@NotNull CQLParser.OperatorNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#windowBody}.
	 */
	void enterWindowBody(@NotNull CQLParser.WindowBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#windowBody}.
	 */
	void exitWindowBody(@NotNull CQLParser.WindowBodyContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#extended}.
	 */
	void enterExtended(@NotNull CQLParser.ExtendedContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#extended}.
	 */
	void exitExtended(@NotNull CQLParser.ExtendedContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#logicExpressionNot}.
	 */
	void enterLogicExpressionNot(@NotNull CQLParser.LogicExpressionNotContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#logicExpressionNot}.
	 */
	void exitLogicExpressionNot(@NotNull CQLParser.LogicExpressionNotContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#confValue}.
	 */
	void enterConfValue(@NotNull CQLParser.ConfValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#confValue}.
	 */
	void exitConfValue(@NotNull CQLParser.ConfValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#constStingValue}.
	 */
	void enterConstStingValue(@NotNull CQLParser.ConstStingValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#constStingValue}.
	 */
	void exitConstStingValue(@NotNull CQLParser.ConstStingValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#selectClause}.
	 */
	void enterSelectClause(@NotNull CQLParser.SelectClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#selectClause}.
	 */
	void exitSelectClause(@NotNull CQLParser.SelectClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#setStatement}.
	 */
	void enterSetStatement(@NotNull CQLParser.SetStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#setStatement}.
	 */
	void exitSetStatement(@NotNull CQLParser.SetStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#atomExpression}.
	 */
	void enterAtomExpression(@NotNull CQLParser.AtomExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#atomExpression}.
	 */
	void exitAtomExpression(@NotNull CQLParser.AtomExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#colType}.
	 */
	void enterColType(@NotNull CQLParser.ColTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#colType}.
	 */
	void exitColType(@NotNull CQLParser.ColTypeContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#constFloatValue}.
	 */
	void enterConstFloatValue(@NotNull CQLParser.ConstFloatValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#constFloatValue}.
	 */
	void exitConstFloatValue(@NotNull CQLParser.ConstFloatValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#usingStatement}.
	 */
	void enterUsingStatement(@NotNull CQLParser.UsingStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#usingStatement}.
	 */
	void exitUsingStatement(@NotNull CQLParser.UsingStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#isForce}.
	 */
	void enterIsForce(@NotNull CQLParser.IsForceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#isForce}.
	 */
	void exitIsForce(@NotNull CQLParser.IsForceContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#serdeProperties}.
	 */
	void enterSerdeProperties(@NotNull CQLParser.SerdePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#serdeProperties}.
	 */
	void exitSerdeProperties(@NotNull CQLParser.SerdePropertiesContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#subQueryExpression}.
	 */
	void enterSubQueryExpression(@NotNull CQLParser.SubQueryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#subQueryExpression}.
	 */
	void exitSubQueryExpression(@NotNull CQLParser.SubQueryExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#applicationName}.
	 */
	void enterApplicationName(@NotNull CQLParser.ApplicationNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#applicationName}.
	 */
	void exitApplicationName(@NotNull CQLParser.ApplicationNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#havingClause}.
	 */
	void enterHavingClause(@NotNull CQLParser.HavingClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#havingClause}.
	 */
	void exitHavingClause(@NotNull CQLParser.HavingClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expression}.
	 */
	void enterExpression(@NotNull CQLParser.ExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expression}.
	 */
	void exitExpression(@NotNull CQLParser.ExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#bitOperator}.
	 */
	void enterBitOperator(@NotNull CQLParser.BitOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#bitOperator}.
	 */
	void exitBitOperator(@NotNull CQLParser.BitOperatorContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#fromClause}.
	 */
	void enterFromClause(@NotNull CQLParser.FromClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#fromClause}.
	 */
	void exitFromClause(@NotNull CQLParser.FromClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#windowSource}.
	 */
	void enterWindowSource(@NotNull CQLParser.WindowSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#windowSource}.
	 */
	void exitWindowSource(@NotNull CQLParser.WindowSourceContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#streamProperties}.
	 */
	void enterStreamProperties(@NotNull CQLParser.StreamPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#streamProperties}.
	 */
	void exitStreamProperties(@NotNull CQLParser.StreamPropertiesContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#searchCondition}.
	 */
	void enterSearchCondition(@NotNull CQLParser.SearchConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#searchCondition}.
	 */
	void exitSearchCondition(@NotNull CQLParser.SearchConditionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#selectItem}.
	 */
	void enterSelectItem(@NotNull CQLParser.SelectItemContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#selectItem}.
	 */
	void exitSelectItem(@NotNull CQLParser.SelectItemContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#createOperatorStatement}.
	 */
	void enterCreateOperatorStatement(@NotNull CQLParser.CreateOperatorStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#createOperatorStatement}.
	 */
	void exitCreateOperatorStatement(@NotNull CQLParser.CreateOperatorStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#joinRigthBody}.
	 */
	void enterJoinRigthBody(@NotNull CQLParser.JoinRigthBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#joinRigthBody}.
	 */
	void exitJoinRigthBody(@NotNull CQLParser.JoinRigthBodyContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expressionBetween}.
	 */
	void enterExpressionBetween(@NotNull CQLParser.ExpressionBetweenContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expressionBetween}.
	 */
	void exitExpressionBetween(@NotNull CQLParser.ExpressionBetweenContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#insertUserOperatorStatement}.
	 */
	void enterInsertUserOperatorStatement(@NotNull CQLParser.InsertUserOperatorStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#insertUserOperatorStatement}.
	 */
	void exitInsertUserOperatorStatement(@NotNull CQLParser.InsertUserOperatorStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#dataSourceName}.
	 */
	void enterDataSourceName(@NotNull CQLParser.DataSourceNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#dataSourceName}.
	 */
	void exitDataSourceName(@NotNull CQLParser.DataSourceNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#logicExpressionAnd}.
	 */
	void enterLogicExpressionAnd(@NotNull CQLParser.LogicExpressionAndContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#logicExpressionAnd}.
	 */
	void exitLogicExpressionAnd(@NotNull CQLParser.LogicExpressionAndContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#datasourceProperties}.
	 */
	void enterDatasourceProperties(@NotNull CQLParser.DatasourcePropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#datasourceProperties}.
	 */
	void exitDatasourceProperties(@NotNull CQLParser.DatasourcePropertiesContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeBound}.
	 */
	void enterRangeBound(@NotNull CQLParser.RangeBoundContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeBound}.
	 */
	void exitRangeBound(@NotNull CQLParser.RangeBoundContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#limitRow}.
	 */
	void enterLimitRow(@NotNull CQLParser.LimitRowContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#limitRow}.
	 */
	void exitLimitRow(@NotNull CQLParser.LimitRowContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expressionPrevious}.
	 */
	void enterExpressionPrevious(@NotNull CQLParser.ExpressionPreviousContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expressionPrevious}.
	 */
	void exitExpressionPrevious(@NotNull CQLParser.ExpressionPreviousContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#ifExists}.
	 */
	void enterIfExists(@NotNull CQLParser.IfExistsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#ifExists}.
	 */
	void exitIfExists(@NotNull CQLParser.IfExistsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#serdeClass}.
	 */
	void enterSerdeClass(@NotNull CQLParser.SerdeClassContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#serdeClass}.
	 */
	void exitSerdeClass(@NotNull CQLParser.SerdeClassContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#createPipeStreamStatement}.
	 */
	void enterCreatePipeStreamStatement(@NotNull CQLParser.CreatePipeStreamStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#createPipeStreamStatement}.
	 */
	void exitCreatePipeStreamStatement(@NotNull CQLParser.CreatePipeStreamStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#showFunctions}.
	 */
	void enterShowFunctions(@NotNull CQLParser.ShowFunctionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#showFunctions}.
	 */
	void exitShowFunctions(@NotNull CQLParser.ShowFunctionsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#equalRelationExpression}.
	 */
	void enterEqualRelationExpression(@NotNull CQLParser.EqualRelationExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#equalRelationExpression}.
	 */
	void exitEqualRelationExpression(@NotNull CQLParser.EqualRelationExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#windowProperties}.
	 */
	void enterWindowProperties(@NotNull CQLParser.WindowPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#windowProperties}.
	 */
	void exitWindowProperties(@NotNull CQLParser.WindowPropertiesContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeWindow}.
	 */
	void enterRangeWindow(@NotNull CQLParser.RangeWindowContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeWindow}.
	 */
	void exitRangeWindow(@NotNull CQLParser.RangeWindowContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#columnName}.
	 */
	void enterColumnName(@NotNull CQLParser.ColumnNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#columnName}.
	 */
	void exitColumnName(@NotNull CQLParser.ColumnNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#groupByList}.
	 */
	void enterGroupByList(@NotNull CQLParser.GroupByListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#groupByList}.
	 */
	void exitGroupByList(@NotNull CQLParser.GroupByListContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeMinutes}.
	 */
	void enterRangeMinutes(@NotNull CQLParser.RangeMinutesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeMinutes}.
	 */
	void exitRangeMinutes(@NotNull CQLParser.RangeMinutesContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#whereClause}.
	 */
	void enterWhereClause(@NotNull CQLParser.WhereClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#whereClause}.
	 */
	void exitWhereClause(@NotNull CQLParser.WhereClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#dropFunctionStatement}.
	 */
	void enterDropFunctionStatement(@NotNull CQLParser.DropFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#dropFunctionStatement}.
	 */
	void exitDropFunctionStatement(@NotNull CQLParser.DropFunctionStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#constant}.
	 */
	void enterConstant(@NotNull CQLParser.ConstantContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#constant}.
	 */
	void exitConstant(@NotNull CQLParser.ConstantContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#streamBody}.
	 */
	void enterStreamBody(@NotNull CQLParser.StreamBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#streamBody}.
	 */
	void exitStreamBody(@NotNull CQLParser.StreamBodyContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#createDataSourceStatement}.
	 */
	void enterCreateDataSourceStatement(@NotNull CQLParser.CreateDataSourceStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#createDataSourceStatement}.
	 */
	void exitCreateDataSourceStatement(@NotNull CQLParser.CreateDataSourceStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#partitionbyDeterminer}.
	 */
	void enterPartitionbyDeterminer(@NotNull CQLParser.PartitionbyDeterminerContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#partitionbyDeterminer}.
	 */
	void exitPartitionbyDeterminer(@NotNull CQLParser.PartitionbyDeterminerContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#relationOperator}.
	 */
	void enterRelationOperator(@NotNull CQLParser.RelationOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#relationOperator}.
	 */
	void exitRelationOperator(@NotNull CQLParser.RelationOperatorContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#caseHeadExpression}.
	 */
	void enterCaseHeadExpression(@NotNull CQLParser.CaseHeadExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#caseHeadExpression}.
	 */
	void exitCaseHeadExpression(@NotNull CQLParser.CaseHeadExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#limitAll}.
	 */
	void enterLimitAll(@NotNull CQLParser.LimitAllContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#limitAll}.
	 */
	void exitLimitAll(@NotNull CQLParser.LimitAllContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#confName}.
	 */
	void enterConfName(@NotNull CQLParser.ConfNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#confName}.
	 */
	void exitConfName(@NotNull CQLParser.ConfNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#castExpression}.
	 */
	void enterCastExpression(@NotNull CQLParser.CastExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#castExpression}.
	 */
	void exitCastExpression(@NotNull CQLParser.CastExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#columnNameType}.
	 */
	void enterColumnNameType(@NotNull CQLParser.ColumnNameTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#columnNameType}.
	 */
	void exitColumnNameType(@NotNull CQLParser.ColumnNameTypeContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#function}.
	 */
	void enterFunction(@NotNull CQLParser.FunctionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#function}.
	 */
	void exitFunction(@NotNull CQLParser.FunctionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#groupByClause}.
	 */
	void enterGroupByClause(@NotNull CQLParser.GroupByClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#groupByClause}.
	 */
	void exitGroupByClause(@NotNull CQLParser.GroupByClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeTime}.
	 */
	void enterRangeTime(@NotNull CQLParser.RangeTimeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeTime}.
	 */
	void exitRangeTime(@NotNull CQLParser.RangeTimeContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#selectAlias}.
	 */
	void enterSelectAlias(@NotNull CQLParser.SelectAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#selectAlias}.
	 */
	void exitSelectAlias(@NotNull CQLParser.SelectAliasContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#constIntegerValue}.
	 */
	void enterConstIntegerValue(@NotNull CQLParser.ConstIntegerValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#constIntegerValue}.
	 */
	void exitConstIntegerValue(@NotNull CQLParser.ConstIntegerValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#windowName}.
	 */
	void enterWindowName(@NotNull CQLParser.WindowNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#windowName}.
	 */
	void exitWindowName(@NotNull CQLParser.WindowNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#selectStatement}.
	 */
	void enterSelectStatement(@NotNull CQLParser.SelectStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#selectStatement}.
	 */
	void exitSelectStatement(@NotNull CQLParser.SelectStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#sinkClause}.
	 */
	void enterSinkClause(@NotNull CQLParser.SinkClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#sinkClause}.
	 */
	void exitSinkClause(@NotNull CQLParser.SinkClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#getStatement}.
	 */
	void enterGetStatement(@NotNull CQLParser.GetStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#getStatement}.
	 */
	void exitGetStatement(@NotNull CQLParser.GetStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expressionBetweenMinValue}.
	 */
	void enterExpressionBetweenMinValue(@NotNull CQLParser.ExpressionBetweenMinValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expressionBetweenMinValue}.
	 */
	void exitExpressionBetweenMinValue(@NotNull CQLParser.ExpressionBetweenMinValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#identifierNot}.
	 */
	void enterIdentifierNot(@NotNull CQLParser.IdentifierNotContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#identifierNot}.
	 */
	void exitIdentifierNot(@NotNull CQLParser.IdentifierNotContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#datasourceBody}.
	 */
	void enterDatasourceBody(@NotNull CQLParser.DatasourceBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#datasourceBody}.
	 */
	void exitDatasourceBody(@NotNull CQLParser.DatasourceBodyContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#caseWhenBodyWhenBody}.
	 */
	void enterCaseWhenBodyWhenBody(@NotNull CQLParser.CaseWhenBodyWhenBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#caseWhenBodyWhenBody}.
	 */
	void exitCaseWhenBodyWhenBody(@NotNull CQLParser.CaseWhenBodyWhenBodyContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#showApplications}.
	 */
	void enterShowApplications(@NotNull CQLParser.ShowApplicationsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#showApplications}.
	 */
	void exitShowApplications(@NotNull CQLParser.ShowApplicationsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#insertClause}.
	 */
	void enterInsertClause(@NotNull CQLParser.InsertClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#insertClause}.
	 */
	void exitInsertClause(@NotNull CQLParser.InsertClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#submitApplication}.
	 */
	void enterSubmitApplication(@NotNull CQLParser.SubmitApplicationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#submitApplication}.
	 */
	void exitSubmitApplication(@NotNull CQLParser.SubmitApplicationContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expressions}.
	 */
	void enterExpressions(@NotNull CQLParser.ExpressionsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expressions}.
	 */
	void exitExpressions(@NotNull CQLParser.ExpressionsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#binaryExpression}.
	 */
	void enterBinaryExpression(@NotNull CQLParser.BinaryExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#binaryExpression}.
	 */
	void exitBinaryExpression(@NotNull CQLParser.BinaryExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#explainStatement}.
	 */
	void enterExplainStatement(@NotNull CQLParser.ExplainStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#explainStatement}.
	 */
	void exitExplainStatement(@NotNull CQLParser.ExplainStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#createFunctionStatement}.
	 */
	void enterCreateFunctionStatement(@NotNull CQLParser.CreateFunctionStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#createFunctionStatement}.
	 */
	void exitCreateFunctionStatement(@NotNull CQLParser.CreateFunctionStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#joinSource}.
	 */
	void enterJoinSource(@NotNull CQLParser.JoinSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#joinSource}.
	 */
	void exitJoinSource(@NotNull CQLParser.JoinSourceContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#datasourceQueryArguments}.
	 */
	void enterDatasourceQueryArguments(@NotNull CQLParser.DatasourceQueryArgumentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#datasourceQueryArguments}.
	 */
	void exitDatasourceQueryArguments(@NotNull CQLParser.DatasourceQueryArgumentsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#leftJoin}.
	 */
	void enterLeftJoin(@NotNull CQLParser.LeftJoinContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#leftJoin}.
	 */
	void exitLeftJoin(@NotNull CQLParser.LeftJoinContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#booleanValue}.
	 */
	void enterBooleanValue(@NotNull CQLParser.BooleanValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#booleanValue}.
	 */
	void exitBooleanValue(@NotNull CQLParser.BooleanValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#unidirection}.
	 */
	void enterUnidirection(@NotNull CQLParser.UnidirectionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#unidirection}.
	 */
	void exitUnidirection(@NotNull CQLParser.UnidirectionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expressionIn}.
	 */
	void enterExpressionIn(@NotNull CQLParser.ExpressionInContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expressionIn}.
	 */
	void exitExpressionIn(@NotNull CQLParser.ExpressionInContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#groupByExpression}.
	 */
	void enterGroupByExpression(@NotNull CQLParser.GroupByExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#groupByExpression}.
	 */
	void exitGroupByExpression(@NotNull CQLParser.GroupByExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#sourceAlias}.
	 */
	void enterSourceAlias(@NotNull CQLParser.SourceAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#sourceAlias}.
	 */
	void exitSourceAlias(@NotNull CQLParser.SourceAliasContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rowsWindow}.
	 */
	void enterRowsWindow(@NotNull CQLParser.RowsWindowContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rowsWindow}.
	 */
	void exitRowsWindow(@NotNull CQLParser.RowsWindowContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#columnOrder}.
	 */
	void enterColumnOrder(@NotNull CQLParser.ColumnOrderContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#columnOrder}.
	 */
	void exitColumnOrder(@NotNull CQLParser.ColumnOrderContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#datasourceArguments}.
	 */
	void enterDatasourceArguments(@NotNull CQLParser.DatasourceArgumentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#datasourceArguments}.
	 */
	void exitDatasourceArguments(@NotNull CQLParser.DatasourceArgumentsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#deactiveApplication}.
	 */
	void enterDeactiveApplication(@NotNull CQLParser.DeactiveApplicationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#deactiveApplication}.
	 */
	void exitDeactiveApplication(@NotNull CQLParser.DeactiveApplicationContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#combineCondition}.
	 */
	void enterCombineCondition(@NotNull CQLParser.CombineConditionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#combineCondition}.
	 */
	void exitCombineCondition(@NotNull CQLParser.CombineConditionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeUnBound}.
	 */
	void enterRangeUnBound(@NotNull CQLParser.RangeUnBoundContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeUnBound}.
	 */
	void exitRangeUnBound(@NotNull CQLParser.RangeUnBoundContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#distinct}.
	 */
	void enterDistinct(@NotNull CQLParser.DistinctContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#distinct}.
	 */
	void exitDistinct(@NotNull CQLParser.DistinctContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#streamSource}.
	 */
	void enterStreamSource(@NotNull CQLParser.StreamSourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#streamSource}.
	 */
	void exitStreamSource(@NotNull CQLParser.StreamSourceContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#arithmeticPlusOperator}.
	 */
	void enterArithmeticPlusOperator(@NotNull CQLParser.ArithmeticPlusOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#arithmeticPlusOperator}.
	 */
	void exitArithmeticPlusOperator(@NotNull CQLParser.ArithmeticPlusOperatorContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#sinkProperties}.
	 */
	void enterSinkProperties(@NotNull CQLParser.SinkPropertiesContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#sinkProperties}.
	 */
	void exitSinkProperties(@NotNull CQLParser.SinkPropertiesContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expressionBetweenMaxValue}.
	 */
	void enterExpressionBetweenMaxValue(@NotNull CQLParser.ExpressionBetweenMaxValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expressionBetweenMaxValue}.
	 */
	void exitExpressionBetweenMaxValue(@NotNull CQLParser.ExpressionBetweenMaxValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#crossJoin}.
	 */
	void enterCrossJoin(@NotNull CQLParser.CrossJoinContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#crossJoin}.
	 */
	void exitCrossJoin(@NotNull CQLParser.CrossJoinContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#selectExpression}.
	 */
	void enterSelectExpression(@NotNull CQLParser.SelectExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#selectExpression}.
	 */
	void exitSelectExpression(@NotNull CQLParser.SelectExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#expressionExists}.
	 */
	void enterExpressionExists(@NotNull CQLParser.ExpressionExistsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#expressionExists}.
	 */
	void exitExpressionExists(@NotNull CQLParser.ExpressionExistsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#naturalJoin}.
	 */
	void enterNaturalJoin(@NotNull CQLParser.NaturalJoinContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#naturalJoin}.
	 */
	void exitNaturalJoin(@NotNull CQLParser.NaturalJoinContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeSeconds}.
	 */
	void enterRangeSeconds(@NotNull CQLParser.RangeSecondsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeSeconds}.
	 */
	void exitRangeSeconds(@NotNull CQLParser.RangeSecondsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#columnNameOrderList}.
	 */
	void enterColumnNameOrderList(@NotNull CQLParser.ColumnNameOrderListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#columnNameOrderList}.
	 */
	void exitColumnNameOrderList(@NotNull CQLParser.ColumnNameOrderListContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#caseExpression}.
	 */
	void enterCaseExpression(@NotNull CQLParser.CaseExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#caseExpression}.
	 */
	void exitCaseExpression(@NotNull CQLParser.CaseExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#statement}.
	 */
	void enterStatement(@NotNull CQLParser.StatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#statement}.
	 */
	void exitStatement(@NotNull CQLParser.StatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#activeApplication}.
	 */
	void enterActiveApplication(@NotNull CQLParser.ActiveApplicationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#activeApplication}.
	 */
	void exitActiveApplication(@NotNull CQLParser.ActiveApplicationContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#addJARStatement}.
	 */
	void enterAddJARStatement(@NotNull CQLParser.AddJARStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#addJARStatement}.
	 */
	void exitAddJARStatement(@NotNull CQLParser.AddJARStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#constBigDecimalValue}.
	 */
	void enterConstBigDecimalValue(@NotNull CQLParser.ConstBigDecimalValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#constBigDecimalValue}.
	 */
	void exitConstBigDecimalValue(@NotNull CQLParser.ConstBigDecimalValueContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#subQuerySource}.
	 */
	void enterSubQuerySource(@NotNull CQLParser.SubQuerySourceContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#subQuerySource}.
	 */
	void exitSubQuerySource(@NotNull CQLParser.SubQuerySourceContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#primitiveType}.
	 */
	void enterPrimitiveType(@NotNull CQLParser.PrimitiveTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#primitiveType}.
	 */
	void exitPrimitiveType(@NotNull CQLParser.PrimitiveTypeContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#addFileStatement}.
	 */
	void enterAddFileStatement(@NotNull CQLParser.AddFileStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#addFileStatement}.
	 */
	void exitAddFileStatement(@NotNull CQLParser.AddFileStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#parallelClause}.
	 */
	void enterParallelClause(@NotNull CQLParser.ParallelClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#parallelClause}.
	 */
	void exitParallelClause(@NotNull CQLParser.ParallelClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#createOutputStreamStatement}.
	 */
	void enterCreateOutputStreamStatement(@NotNull CQLParser.CreateOutputStreamStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#createOutputStreamStatement}.
	 */
	void exitCreateOutputStreamStatement(@NotNull CQLParser.CreateOutputStreamStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#functionName}.
	 */
	void enterFunctionName(@NotNull CQLParser.FunctionNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#functionName}.
	 */
	void exitFunctionName(@NotNull CQLParser.FunctionNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#loadStatement}.
	 */
	void enterLoadStatement(@NotNull CQLParser.LoadStatementContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#loadStatement}.
	 */
	void exitLoadStatement(@NotNull CQLParser.LoadStatementContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#streamAlias}.
	 */
	void enterStreamAlias(@NotNull CQLParser.StreamAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#streamAlias}.
	 */
	void exitStreamAlias(@NotNull CQLParser.StreamAliasContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#limitClause}.
	 */
	void enterLimitClause(@NotNull CQLParser.LimitClauseContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#limitClause}.
	 */
	void exitLimitClause(@NotNull CQLParser.LimitClauseContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeHour}.
	 */
	void enterRangeHour(@NotNull CQLParser.RangeHourContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeHour}.
	 */
	void exitRangeHour(@NotNull CQLParser.RangeHourContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#cqlIdentifier}.
	 */
	void enterCqlIdentifier(@NotNull CQLParser.CqlIdentifierContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#cqlIdentifier}.
	 */
	void exitCqlIdentifier(@NotNull CQLParser.CqlIdentifierContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#streamName}.
	 */
	void enterStreamName(@NotNull CQLParser.StreamNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#streamName}.
	 */
	void exitStreamName(@NotNull CQLParser.StreamNameContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#dropApplication}.
	 */
	void enterDropApplication(@NotNull CQLParser.DropApplicationContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#dropApplication}.
	 */
	void exitDropApplication(@NotNull CQLParser.DropApplicationContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#arithmeticPlusMinusExpression}.
	 */
	void enterArithmeticPlusMinusExpression(@NotNull CQLParser.ArithmeticPlusMinusExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#arithmeticPlusMinusExpression}.
	 */
	void exitArithmeticPlusMinusExpression(@NotNull CQLParser.ArithmeticPlusMinusExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#caseWhenBodyThenBody}.
	 */
	void enterCaseWhenBodyThenBody(@NotNull CQLParser.CaseWhenBodyThenBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#caseWhenBodyThenBody}.
	 */
	void exitCaseWhenBodyThenBody(@NotNull CQLParser.CaseWhenBodyThenBodyContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#selectList}.
	 */
	void enterSelectList(@NotNull CQLParser.SelectListContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#selectList}.
	 */
	void exitSelectList(@NotNull CQLParser.SelectListContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#streamAllColumns}.
	 */
	void enterStreamAllColumns(@NotNull CQLParser.StreamAllColumnsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#streamAllColumns}.
	 */
	void exitStreamAllColumns(@NotNull CQLParser.StreamAllColumnsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#commentString}.
	 */
	void enterCommentString(@NotNull CQLParser.CommentStringContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#commentString}.
	 */
	void exitCommentString(@NotNull CQLParser.CommentStringContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#singleAlias}.
	 */
	void enterSingleAlias(@NotNull CQLParser.SingleAliasContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#singleAlias}.
	 */
	void exitSingleAlias(@NotNull CQLParser.SingleAliasContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#bitExpression}.
	 */
	void enterBitExpression(@NotNull CQLParser.BitExpressionContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#bitExpression}.
	 */
	void exitBitExpression(@NotNull CQLParser.BitExpressionContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#rangeMilliSeconds}.
	 */
	void enterRangeMilliSeconds(@NotNull CQLParser.RangeMilliSecondsContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#rangeMilliSeconds}.
	 */
	void exitRangeMilliSeconds(@NotNull CQLParser.RangeMilliSecondsContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#sinkDefine}.
	 */
	void enterSinkDefine(@NotNull CQLParser.SinkDefineContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#sinkDefine}.
	 */
	void exitSinkDefine(@NotNull CQLParser.SinkDefineContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#comment}.
	 */
	void enterComment(@NotNull CQLParser.CommentContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#comment}.
	 */
	void exitComment(@NotNull CQLParser.CommentContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#filterBeforeWindow}.
	 */
	void enterFilterBeforeWindow(@NotNull CQLParser.FilterBeforeWindowContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#filterBeforeWindow}.
	 */
	void exitFilterBeforeWindow(@NotNull CQLParser.FilterBeforeWindowContext ctx);

	/**
	 * Enter a parse tree produced by {@link CQLParser#arithmeticStarOperator}.
	 */
	void enterArithmeticStarOperator(@NotNull CQLParser.ArithmeticStarOperatorContext ctx);
	/**
	 * Exit a parse tree produced by {@link CQLParser#arithmeticStarOperator}.
	 */
	void exitArithmeticStarOperator(@NotNull CQLParser.ArithmeticStarOperatorContext ctx);
}
