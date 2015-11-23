package com.iisquare.jw.frame.view;

import org.springframework.web.servlet.view.freemarker.FreeMarkerConfigurer;

import com.iisquare.jw.frame.ApplicationConfiguration;

/**
 * FreeMarker自定义函数管理器
 */
public class FreemarkerTemplateManager {
	
	private ApplicationConfiguration configuration;
	
	public ApplicationConfiguration getConfiguration() {
		return configuration;
	}

	public void setConfiguration(ApplicationConfiguration configuration) {
		this.configuration = configuration;
	}

	public void setFreeMarkerConfigurer(FreeMarkerConfigurer freeMarkerConfigurer) {
		freemarker.template.Configuration fmConfiguration = freeMarkerConfigurer.getConfiguration();
		fmConfiguration.setTagSyntax(freemarker.template.Configuration.SQUARE_BRACKET_TAG_SYNTAX);
		fmConfiguration.setSharedVariable("millisToDateTime",
        		new FreemarkerMillisToDateTimeModel(this.configuration));
		fmConfiguration.setSharedVariable("empty", new FreemarkerEmptyModel());
		fmConfiguration.setSharedVariable("escapeHtml", new FreemarkerEscapeHtmlModel());
		fmConfiguration.setSharedVariable("unescapeHtml", new FreemarkerUnescapeHtmlModel());
		fmConfiguration.setSharedVariable("subStringWithByte", new FreemarkerSubStringWithByteModel());
		fmConfiguration.setSharedVariable("processPagination", new FreemarkerProcessPaginationModel());
	}
	
	public FreemarkerTemplateManager() {}
}
