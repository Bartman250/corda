package net.corda.notary.transactionservice

import net.corda.core.contracts.StateAndRef
import net.corda.core.contracts.StateRef
import net.corda.core.flows.NotaryFlow
import net.corda.core.identity.CordaX500Name
import net.corda.core.identity.Party
import net.corda.core.internal.concurrent.map
import net.corda.core.node.AppServiceHub
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.transactions.TransactionBuilder
import net.corda.core.utilities.getOrThrow
import net.corda.testing.contracts.DummyContract
import net.corda.testing.core.DUMMY_BANK_A_NAME
import net.corda.testing.core.dummyCommand
import net.corda.testing.core.singleIdentity
import net.corda.testing.driver.DriverParameters
import net.corda.testing.driver.InProcess
import net.corda.testing.driver.driver
import net.corda.testing.driver.internal.InProcessImpl
import net.corda.testing.node.NotarySpec
import org.junit.Test
import java.time.Instant
import java.util.Random

/**
 * An example Corda (pseudo) service consuming the notary transaction api
 * It takes the callback and puts the data on a queue - in this case a simple thread synced queue - but this
 * could equally be an artemis queue so that listening to the data can happen external to the notary.
 * The test here acts as the ultimated consumer of the data.
 */
class NotaryTransactionConsumer(private val serviceHub: AppServiceHub) : SingletonSerializeAsToken() {
    private val txQueue = SimpleMessageQueue<NotaryTransaction>()
    private var notaryTransactionRequest: NotaryTransactionRequest

    init {
        notaryTransactionRequest = NotaryTransactionRequest(Instant.now()) { notaryTx: NotaryTransaction ->
            println(notaryTx)
            txQueue.stuffOnQueue(notaryTx)
        }
        notaryTransactionService().getTransactionsFrom(notaryTransactionRequest)
    }

    fun getOffQueue(): NotaryTransaction? {
        return txQueue.getOffQueue()
    }

    private fun notaryTransactionService() = serviceHub.cordaService(SimpleNotaryTransactionService::class.java)
}

/**
 * This test installs the Notary Transaction Consumer on notary and then pulls the transactions
 * off the queue that get generated with the test transaction
 */
class SimpleNotaryTransactionServiceTest {
    private val notaryName = CordaX500Name("My Simple Notary", "London", "GB")

    @Test
    fun `we can get notarised transactions`() {

        driver(DriverParameters(
                startNodesInProcess = true,
                extraCordappPackagesToScan = listOf("net.corda.testing.contracts"),
                notarySpecs = listOf(NotarySpec(notaryName, false))
        )) {
            val bankA = startNode(providedName = DUMMY_BANK_A_NAME).map { (it as InProcess) }.getOrThrow()

            val notaryNodeHandle = notaryHandles.first().nodeHandles.get().first()
            val notaryInProcess = notaryNodeHandle as InProcessImpl
            val myCordaService = NotaryTransactionConsumer(TempHackedAppServiceHubImpl(notaryInProcess.services))

            val inputState = issueState(bankA, defaultNotaryIdentity)

            val firstTxBuilder = TransactionBuilder(defaultNotaryIdentity)
                    .addInputState(inputState)
                    .addCommand(dummyCommand(bankA.services.myInfo.singleIdentity().owningKey))

            val firstSpendTx = bankA.services.signInitialTransaction(firstTxBuilder)
            val firstSpend = bankA.startFlow(NotaryFlow.Client(firstSpendTx))
            firstSpend.getOrThrow()

            var notaryTx: NotaryTransaction? = null
            var retries = 0
            while (notaryTx == null && retries < 50) {
                notaryTx = myCordaService.getOffQueue()
                notaryTx?.apply {
                    println(this)
                }
                Thread.sleep(250)
                retries++
            }
            assert(notaryTx != null) { println("no tx found") }

            assert(notaryTx!!.consumingParty.owningKey == bankA.nodeInfo.legalIdentities.first().owningKey)

            notaryTx.transactionMetaData.forEach{
                txMetaData ->
                    when(txMetaData) {
                        is TransactionPriority -> assert(txMetaData.priority>-1)
                        is TransactionNumberOfStates -> assert(txMetaData.numberOfStates >0)
                        is TransactionSigners -> assert(txMetaData.signers.size>0)
                    }
            }
        }
    }
    private fun issueState(nodeHandle: InProcess, notary: Party): StateAndRef<*> {
        val builder = DummyContract.generateInitial(Random().nextInt(), notary, nodeHandle.services.myInfo.singleIdentity().ref(0))
        val stx = nodeHandle.services.signInitialTransaction(builder)
        nodeHandle.services.recordTransactions(stx)
        return StateAndRef(stx.coreTransaction.outputs.first(), StateRef(stx.id, 0))
    }
}