/*
 * Copyright 2011-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.agaveapi.ops.speedtest.cli.commands;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

/**
 * @author Jarred Li
 *
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class AgaveOpsBannerProvider extends DefaultBannerProvider  {

	public String getBanner() {
		StringBuffer buf = new StringBuffer();
		buf.append("==============================================================================================" + OsUtils.LINE_SEPARATOR);
		buf.append("*   /$$$$$$                                                 /$$$$$$  /$$$$$$$   /$$$$$$      *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*   /$$__  $$                                               /$$__  $$| $$__  $$ /$$__  $$    *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*   | $$  \\ $$  /$$$$$$   /$$$$$$  /$$    /$$ /$$$$$$       | $$  \\ $$| $$  \\ $$| $$  \\__/   *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*   | $$$$$$$$ /$$__  $$ |____  $$|  $$  /$$//$$__  $$      | $$  | $$| $$$$$$$/|  $$$$$$    *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*   | $$__  $$| $$  \\ $$  /$$$$$$$ \\  $$/$$/| $$$$$$$$      | $$  | $$| $$____/  \\____  $$   *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*   | $$  | $$| $$  | $$ /$$__  $$  \\  $$$/ | $$_____/      | $$  | $$| $$       /$$  \\ $$   *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*   | $$  | $$|  $$$$$$$|  $$$$$$$   \\  $/  |  $$$$$$$      |  $$$$$$/| $$      |  $$$$$$/   *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*   |__/  |__/ \\____  $$ \\_______/    \\_/    \\_______/       \\______/ |__/       \\______/    *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*            /$$  \\ $$                                                                       *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*           |  $$$$$$/                                                                       *"+ OsUtils.LINE_SEPARATOR);
		buf.append("*            \\______/                                                                       *"+ OsUtils.LINE_SEPARATOR);
		buf.append("==============================================================================================" + OsUtils.LINE_SEPARATOR);
		buf.append("Version:" + this.getVersion());
		return buf.toString();
	}

	public String getVersion() {
		return "0.1.0";
	}

	public String getWelcomeMessage() {
		return "Welcome to Agave Ops CLI";
	}
	
	@Override
	public String getProviderName() {
		return "Agave Ops provider";
	}
}