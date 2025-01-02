//A PathFilter for excluding paths that match a regular expression 

//The filter passes only those files that don't match the regular expression.
//After the glob picks out an initial set of files to include, the filter is 
//used to refine the result.

package com.wace.data.farm.com.HadoopDocProject.ch3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class RegexExcludePathFilter implements PathFilter {
	private final String regex;
	
	public RegexExcludePathFilter(String regex) {
		this.regex = regex;
	}
	
	public boolean accept (Path path) {
		return !path.toString().matches(regex);
	}
	
}
