package net.corda.notary.transactionservice

import net.corda.core.context.InvocationContext
import net.corda.core.flows.FlowLogic
import net.corda.core.flows.StartableByService
import net.corda.core.internal.FlowStateMachine
import net.corda.core.messaging.FlowHandle
import net.corda.core.messaging.FlowHandleImpl
import net.corda.core.messaging.FlowProgressHandle
import net.corda.core.messaging.FlowProgressHandleImpl
import net.corda.core.node.AppServiceHub
import net.corda.core.node.ServiceHub
import net.corda.core.utilities.getOrThrow
import net.corda.node.services.api.StartedNodeServices
import net.corda.testing.node.internal.startFlow
import rx.Observable

/**
 * Recreates an application service hub that we can pass into corda services
 */
class TempHackedAppServiceHubImpl(private val serviceHubImpl: ServiceHub) : AppServiceHub, ServiceHub by serviceHubImpl {
    override fun <T> startTrackedFlow(flow: FlowLogic<T>): FlowProgressHandle<T> {
        val stateMachine = startFlowChecked(flow)
        return FlowProgressHandleImpl(
                id = stateMachine.id,
                returnValue = stateMachine.resultFuture,
                progress = stateMachine.logic.track()?.updates ?: Observable.empty()
        )
    }
    override fun <T> startFlow(flow: FlowLogic<T>): FlowHandle<T> {
        val sns = serviceHubImpl as StartedNodeServices
        val stateMachine = sns.startFlow(flow)
        return FlowHandleImpl(id = stateMachine.id, returnValue = stateMachine.resultFuture)
    }
    private fun <T> startFlowChecked(flow: FlowLogic<T>): FlowStateMachine<T> {
        val logicType = flow.javaClass
        require(logicType.isAnnotationPresent(StartableByService::class.java)) { "${logicType.name} was not designed for starting by a CordaService" }
        val context = InvocationContext.service("fakeServicehub", myInfo.legalIdentities[0].name)
        val sns = serviceHubImpl as StartedNodeServices
        return sns.startFlow(flow, context).getOrThrow()
    }
}