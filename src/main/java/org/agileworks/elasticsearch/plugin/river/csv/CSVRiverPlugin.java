/*
 *   This software is licensed under the Apache 2 license, quoted below.
 *
 *   Copyright 2012-2013 Martin Bednar
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *   use this file except in compliance with the License. You may obtain a copy of
 *   the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations under
 *   the License.
 */

package org.agileworks.elasticsearch.plugin.river.csv;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.agileworks.elasticsearch.river.csv.CSVRiverModule;

/**
 *
 */
public class CSVRiverPlugin extends AbstractPlugin {

    @Inject
    public CSVRiverPlugin() {
    }

    @Override
    public String name() {
        return "river-csv";
    }

    @Override
    public String description() {
        return "River CSV Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("csv", CSVRiverModule.class);
    }
}
