package com.newnius.code4hadoop.book.ch3;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created by newnius on 12/7/16.
 *
 */
class RegexExcludePathFilter implements PathFilter{
    private final String regex;

    RegexExcludePathFilter(String regex){
        this.regex = regex;
    }

    @Override
    public boolean accept(Path path) {
        return !path.toString().matches(regex);
    }
}
