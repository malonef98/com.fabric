/*
 *  Copyright 2018 Aliyun.com All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.fabric.baas;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hyperledger.fabric.sdk.*;
import org.hyperledger.fabric.sdk.exception.InvalidArgumentException;
import org.hyperledger.fabric.sdk.exception.ProposalException;
import org.hyperledger.fabric.sdk.exception.ServiceDiscoveryException;

public class ChaincodeExecuter {
    private static final Log logger = LogFactory.getLog(ChaincodeExecuter.class);

    private String chaincodeName;
    private String version;
    private ChaincodeID ccId;
    // waitTime can be adjusted to avoid timeout for connection to external network
    private long waitTime = 10000;

    public ChaincodeExecuter(String chaincodeName, String version) {
        this.chaincodeName = chaincodeName;
        this.version = version;

        ChaincodeID.Builder chaincodeIDBuilder = ChaincodeID.newBuilder()
                .setName(chaincodeName)
                .setVersion(version);
        ccId = chaincodeIDBuilder.build();
    }

    public String getChaincodeName() {
        return chaincodeName;
    }

    public void setChaincodeName(String chaincodeName) {
        this.chaincodeName = chaincodeName;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public long getWaitTime() {
        return waitTime;
    }

    public void setWaitTime(long waitTime) {
        this.waitTime = waitTime;
    }

    public void executeTransaction(HFClient client, Channel channel, boolean invoke, String func, String... args) throws InvalidArgumentException, ProposalException, UnsupportedEncodingException, ServiceDiscoveryException, InterruptedException, ExecutionException, TimeoutException {
        TransactionProposalRequest transactionProposalRequest = client.newTransactionProposalRequest();
        transactionProposalRequest.setChaincodeID(ccId);
        transactionProposalRequest.setChaincodeLanguage(TransactionRequest.Type.GO_LANG);

        transactionProposalRequest.setFcn(func);
        transactionProposalRequest.setArgs(args);
        transactionProposalRequest.setProposalWaitTime(waitTime);


        List<ProposalResponse> successful = new LinkedList<ProposalResponse>();
        List<ProposalResponse> failed = new LinkedList<ProposalResponse>();

        // Java sdk will send transaction proposal to all peers, if some peer down but the response still meet the endorsement policy of chaincode,
        // there is no need to retry. If not, you should re-send the transaction proposal.
        // Collection<ProposalResponse> transactionPropResp = channel.sendTransactionProposal(transactionProposalRequest, channel.getPeers());

        Collection<ProposalResponse> transactionPropResp;
        if (channel.getPeers(EnumSet.of(Peer.PeerRole.SERVICE_DISCOVERY)).size() > 0) {
            // If we have discovery service in network, use discovery to find endorsing peers
            // Java sdk will send transaction proposal to necessary peers, if some peer down but the response still meet the endorsement policy of chaincode,
            // there is no need to retry. If not, you should re-send the transaction proposal.
            Channel.DiscoveryOptions discoveryOptions = Channel.DiscoveryOptions.createDiscoveryOptions();
            // Random select a peer group which satisfied the chaincode endorsement policy
            discoveryOptions.setEndorsementSelector(ServiceDiscovery.EndorsementSelector.ENDORSEMENT_SELECTION_RANDOM);
            // Use discovery cache in java sdk, cache refresh endorsement policy every two minutes by default
            discoveryOptions.setForceDiscovery(false);
            // Set inspect results to true if we wanna handle response error by ourselves
            discoveryOptions.setInspectResults(true);
            transactionPropResp = channel.sendTransactionProposalToEndorsers(transactionProposalRequest, discoveryOptions);
        } else {
            // Send proposal to all endorsing peers which configured in connection-profile. It's highly recommended
            // to use service discovery in your application, details see
            // 中文: https://help.aliyun.com/document_detail/141500.html
            transactionPropResp = channel.sendTransactionProposal(transactionProposalRequest, channel.getPeers(EnumSet.of(Peer.PeerRole.ENDORSING_PEER)));
        }

        for (ProposalResponse response : transactionPropResp) {

            if (response.getStatus() == ProposalResponse.Status.SUCCESS) {
                String payload = new String(response.getChaincodeActionResponsePayload());
                logger.info(String.format("[√] Got success response from peer %s => payload: %s", response.getPeer().getName(), payload));
                successful.add(response);
            } else {
                String status = response.getStatus().toString();
                String msg = response.getMessage();
                logger.warn(String.format("[×] Got failed response from peer %s => %s: %s ", response.getPeer().getName(), status, msg));
                failed.add(response);
            }
        }

        if (invoke) {
            Channel.TransactionOptions opts = new Channel.TransactionOptions();
            Channel.NOfEvents nOfEvents = Channel.NOfEvents.createNofEvents();
            // Java sdk will listening all peer event which eventSource is set true in connection profile
            nOfEvents.addPeers(channel.getPeers(EnumSet.of(Peer.PeerRole.EVENT_SOURCE)));
            // Set option NofEvents to 1, sendTransaction will return success after receive 1 transaction valid event from any eventing peer
            nOfEvents.setN(1);
            opts.nOfEvents(nOfEvents);
            logger.info("Sending transaction to orderers...");
            // Java sdk tries all orderers to send transaction, so don't worry about one orderer gone.
            channel.sendTransaction(successful, opts).thenApply(transactionEvent -> {
                logger.info("Orderer response: txid" + transactionEvent.getTransactionID());
                logger.info("Orderer response: block number: " + transactionEvent.getBlockEvent().getBlockNumber());
                return null;
            }).exceptionally(e -> {
                logger.error("Orderer exception happened: ", e);
                return null;
            }).get(waitTime, TimeUnit.SECONDS);
        }
    }
}
