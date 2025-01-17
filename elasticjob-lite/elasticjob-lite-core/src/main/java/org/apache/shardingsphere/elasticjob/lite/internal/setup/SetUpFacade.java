/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.elasticjob.lite.internal.setup;

import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.infra.listener.ElasticJobListener;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.election.LeaderService;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceService;
import org.apache.shardingsphere.elasticjob.lite.internal.listener.ListenerManager;
import org.apache.shardingsphere.elasticjob.lite.internal.reconcile.ReconcileService;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.Collection;

/**
 * Set up facade.
 */
public final class SetUpFacade {
    
    private final ConfigurationService configService;
    
    private final LeaderService leaderService;
    
    private final ServerService serverService;
    
    private final InstanceService instanceService;
    
    private final ReconcileService reconcileService;
    
    private final ListenerManager listenerManager;
    
    public SetUpFacade(final CoordinatorRegistryCenter regCenter, final String jobName, final Collection<ElasticJobListener> elasticJobListeners) {
        configService = new ConfigurationService(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        serverService = new ServerService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        reconcileService = new ReconcileService(regCenter, jobName);
        listenerManager = new ListenerManager(regCenter, jobName, elasticJobListeners);
    }
    
    /**
     * Set up job configuration.
     *
     * @param jobClassName job class name
     * @param jobConfig job configuration to be updated
     * @return accepted job configuration
     */
    public JobConfiguration setUpJobConfiguration(final String jobClassName, final JobConfiguration jobConfig) {
        return configService.setUpJobConfiguration(jobClassName, jobConfig);
    }
    
    /**
     * Register start up info.
     * 
     * @param enabled enable job on startup
     */
    public void registerStartUpInfo(final boolean enabled) {
        // 启动监听器/针对注册中心
        listenerManager.startAllListeners();
        // 当前节点尝试竞选Leader
        leaderService.electLeader();
        // 获取当前在线的服务节点
        serverService.persistOnline(enabled);
        // 注册当前节点
        instanceService.persistOnline();

        // 当本地状态和注册中心状态不一致时，设置需要重新同步/重新分片的flag
        if (!reconcileService.isRunning()) {
            reconcileService.startAsync();
        }
    }
    
    /**
     * Tear down.
     */
    public void tearDown() {
        if (reconcileService.isRunning()) {
            reconcileService.stopAsync();
        }
    }
}
