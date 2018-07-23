/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.huawei.streaming.jstorm;

import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.huawei.streaming.application.ApplicationResults;

import backtype.storm.generated.TopologySummary;

/**
 * Storm 应用程序查询结果
 *
 */
public class JStormApplicationResults implements ApplicationResults {

	private static final Logger LOG = LoggerFactory.getLogger(JStormApplicationResults.class);

	private static final String FORMATTER = "%-20s %-10s %-12s %-10s";

	private static final String[] RESULTSHEAD = { "applicationName", "Status", "Num_workers", "Uptime_secs" };

	private static final String ANY_CHAR_IN_CQL = "*";

	private static final String REGULAR_ANY_CHAR = ".*";

	private List<String[]> results;

	public JStormApplicationResults(List<TopologySummary> topologies) {
		results = Lists.newArrayList();
		parseResults(topologies);
	}

	/**
	 * 获取列格式化字符串
	 *
	 */
	@Override public String getFormatter() {
		return FORMATTER;
	}

	/**
	 * 获取应用程序查询结果的标题头
	 *
	 */
	@Override public String[] getResultHeader() {
		return RESULTSHEAD;
	}

	/**
	 * 获取查询结果
	 * 查询结果的列数量必须和标题头的数组数量一致
	 *
	 */
	@Override public List<String[]> getResults(String container) {
		List<String[]> filteredResults = Lists.newArrayList();

		if (Strings.isNullOrEmpty(container)) {
			return results;
		}

		Pattern funcPattern = null;
		try {
			funcPattern = Pattern.compile(container.replace(ANY_CHAR_IN_CQL, REGULAR_ANY_CHAR));
		} catch (PatternSyntaxException e) {
			LOG.error("Failed to compile " + container + " to pattern", e);
			return filteredResults;
		}

		for (String[] result : results) {
			String name = result[0];
			if (funcPattern.matcher(name).matches()) {
				filteredResults.add(result);
			}

		}

		return filteredResults;
	}

	private void parseResults(List<TopologySummary> topologies) {
		if (topologies == null) {
			return;
		}

		for (TopologySummary topology : topologies) {
			String[] result = new String[RESULTSHEAD.length];
			result[0] = topology.get_name();
			result[1] = topology.get_status();
			result[2] = String.valueOf(topology.get_numWorkers());
			result[3] = String.valueOf(topology.get_uptimeSecs());
			results.add(result);
		}
	}

}
