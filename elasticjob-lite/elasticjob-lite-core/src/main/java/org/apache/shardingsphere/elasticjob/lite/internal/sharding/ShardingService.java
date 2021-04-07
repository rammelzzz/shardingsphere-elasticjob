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

package org.apache.shardingsphere.elasticjob.lite.internal.sharding;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.framework.api.transaction.TransactionOp;
import org.apache.shardingsphere.elasticjob.api.JobConfiguration;
import org.apache.shardingsphere.elasticjob.infra.concurrent.BlockUtils;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobInstance;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobShardingStrategy;
import org.apache.shardingsphere.elasticjob.infra.handler.sharding.JobShardingStrategyFactory;
import org.apache.shardingsphere.elasticjob.infra.yaml.YamlEngine;
import org.apache.shardingsphere.elasticjob.lite.internal.config.ConfigurationService;
import org.apache.shardingsphere.elasticjob.lite.internal.election.LeaderService;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceNode;
import org.apache.shardingsphere.elasticjob.lite.internal.instance.InstanceService;
import org.apache.shardingsphere.elasticjob.lite.internal.schedule.JobRegistry;
import org.apache.shardingsphere.elasticjob.lite.internal.server.ServerService;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodePath;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.JobNodeStorage;
import org.apache.shardingsphere.elasticjob.lite.internal.storage.TransactionExecutionCallback;
import org.apache.shardingsphere.elasticjob.reg.base.CoordinatorRegistryCenter;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Sharding service.
 */
@Slf4j
public final class ShardingService {
    
    private final String jobName;
    
    private final JobNodeStorage jobNodeStorage;
    
    private final LeaderService leaderService;
    
    private final ConfigurationService configService;
    
    private final InstanceService instanceService;
    
    private final InstanceNode instanceNode;
    
    private final ServerService serverService;
    
    private final ExecutionService executionService;

    private final JobNodePath jobNodePath;
    
    public ShardingService(final CoordinatorRegistryCenter regCenter, final String jobName) {
        this.jobName = jobName;
        jobNodeStorage = new JobNodeStorage(regCenter, jobName);
        leaderService = new LeaderService(regCenter, jobName);
        configService = new ConfigurationService(regCenter, jobName);
        instanceService = new InstanceService(regCenter, jobName);
        instanceNode = new InstanceNode(jobName);
        serverService = new ServerService(regCenter, jobName);
        executionService = new ExecutionService(regCenter, jobName);
        jobNodePath = new JobNodePath(jobName);
    }
    
    /**
     * Set resharding flag.
     */
    public void setReshardingFlag() {
        if (!leaderService.isLeaderUntilBlock()) {
            return;
        }
        jobNodeStorage.createJobNodeIfNeeded(ShardingNode.NECESSARY);
    }
    
    /**
     * Judge is need resharding or not.
     * 
     * @return is need resharding or not
     */
    public boolean isNeedSharding() {
        return jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY);
    }
    
    /**
     * Sharding if necessary.
     * 
     * <p>
     * Sharding if current job server is leader server;
     * Do not sharding if no available job server. 
     * </p>
     */
    public void shardingIfNecessary() {
        // 获取集群服务集合
        List<JobInstance> availableJobInstances = instanceService.getAvailableJobInstances();
        if (!isNeedSharding() || availableJobInstances.isEmpty()) {
            return;
        }
        if (!leaderService.isLeaderUntilBlock()) {
            blockUntilShardingCompleted();
            return;
        }

        /* ********************   Leader执行sharding的过程  ****************/

        // 等待上一次分片的任务执行完毕
        waitingOtherShardingItemCompleted();
        JobConfiguration jobConfig = configService.load(false);


        // 获取总分片数
        int shardingTotalCount = jobConfig.getShardingTotalCount();
        log.debug("Job '{}' sharding begin.", jobName);
        jobNodeStorage.fillEphemeralJobNode(ShardingNode.PROCESSING, "");

        // 重置分片信息
        // 创建了一堆/jobName/sharding/${shardingNumber}节点，节点数目由shardingTotalCount决定
        resetShardingInfo(shardingTotalCount);

        // 获取Job分片策略
        JobShardingStrategy jobShardingStrategy = JobShardingStrategyFactory.getStrategy(jobConfig.getJobShardingStrategyType());

        // 将分片信息放到事务中执行，注册到注册中心
        jobNodeStorage.executeInTransaction(new PersistShardingInfoTransactionExecutionCallback(jobShardingStrategy.sharding(availableJobInstances, jobName, shardingTotalCount)));
        log.debug("Job '{}' sharding complete.", jobName);
    }
    
    private void blockUntilShardingCompleted() {
        while (!leaderService.isLeaderUntilBlock() && (jobNodeStorage.isJobNodeExisted(ShardingNode.NECESSARY) || jobNodeStorage.isJobNodeExisted(ShardingNode.PROCESSING))) {
            log.debug("Job '{}' sleep short time until sharding completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }
    
    private void waitingOtherShardingItemCompleted() {
        while (executionService.hasRunningItems()) {
            log.debug("Job '{}' sleep short time until other job completed.", jobName);
            BlockUtils.waitingShortTime();
        }
    }
    
    private void resetShardingInfo(final int shardingTotalCount) {
        for (int i = 0; i < shardingTotalCount; i++) {
            // 如果某个分片存在服务节点，则移除
            jobNodeStorage.removeJobNodeIfExisted(ShardingNode.getInstanceNode(i));

            jobNodeStorage.createJobNodeIfNeeded(ShardingNode.ROOT + "/" + i);
        }

        // 查看实际上的创建成功的分片数目
        // 即查看/jobName/sharding/ 下的children数目
        int actualShardingTotalCount = jobNodeStorage.getJobNodeChildrenKeys(ShardingNode.ROOT).size();
        if (actualShardingTotalCount > shardingTotalCount) {
            for (int i = shardingTotalCount; i < actualShardingTotalCount; i++) {
                jobNodeStorage.removeJobNodeIfExisted(ShardingNode.ROOT + "/" + i);
            }
        }
    }
    
    /**
     * Get sharding items.
     *
     * @param jobInstanceId job instance ID
     * @return sharding items
     */
    public List<Integer> getShardingItems(final String jobInstanceId) {
        JobInstance jobInstance = YamlEngine.unmarshal(jobNodeStorage.getJobNodeData(instanceNode.getInstancePath(jobInstanceId)), JobInstance.class);
        if (!serverService.isAvailableServer(jobInstance.getServerIp())) {
            return Collections.emptyList();
        }
        List<Integer> result = new LinkedList<>();
        int shardingTotalCount = configService.load(true).getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (jobInstance.getJobInstanceId().equals(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                result.add(i);
            }
        }
        return result;
    }
    
    /**
     * Get sharding items from localhost job server.
     * 
     * @return sharding items from localhost job server
     */
    public List<Integer> getLocalShardingItems() {
        if (JobRegistry.getInstance().isShutdown(jobName) || !serverService.isAvailableServer(JobRegistry.getInstance().getJobInstance(jobName).getServerIp())) {
            return Collections.emptyList();
        }
        // 这里获取到的实际上是本机对应的全部shardingItems
        return getShardingItems(JobRegistry.getInstance().getJobInstance(jobName).getJobInstanceId());
    }
    
    /**
     * Query has sharding info in offline servers or not.
     * 
     * @return has sharding info in offline servers or not
     */
    public boolean hasShardingInfoInOfflineServers() {
        List<String> onlineInstances = jobNodeStorage.getJobNodeChildrenKeys(InstanceNode.ROOT);
        int shardingTotalCount = configService.load(true).getShardingTotalCount();
        for (int i = 0; i < shardingTotalCount; i++) {
            if (!onlineInstances.contains(jobNodeStorage.getJobNodeData(ShardingNode.getInstanceNode(i)))) {
                return true;
            }
        }
        return false;
    }
    
    @RequiredArgsConstructor
    class PersistShardingInfoTransactionExecutionCallback implements TransactionExecutionCallback {
        
        private final Map<JobInstance, List<Integer>> shardingResults;
        
        @Override
        public List<CuratorOp> createCuratorOperators(final TransactionOp transactionOp) throws Exception {
            List<CuratorOp> result = new LinkedList<>();
            for (Entry<JobInstance, List<Integer>> entry : shardingResults.entrySet()) {
                for (int shardingItem : entry.getValue()) {
                    // 将/jobName/sharding/${shardingNumber}/instance 的值置为分到分片的服务节点的instanceId
                    result.add(transactionOp.create().forPath(jobNodePath.getFullPath(ShardingNode.getInstanceNode(shardingItem)), entry.getKey().getJobInstanceId().getBytes()));
                }
            }
            // 移除需要分片的/jobName/leader/sharding/necessary
            result.add(transactionOp.delete().forPath(jobNodePath.getFullPath(ShardingNode.NECESSARY)));
            // 移除/jobName/leader/sharding/processing 节点，意为分片结束
            result.add(transactionOp.delete().forPath(jobNodePath.getFullPath(ShardingNode.PROCESSING)));
            return result;
        }
    }
}
