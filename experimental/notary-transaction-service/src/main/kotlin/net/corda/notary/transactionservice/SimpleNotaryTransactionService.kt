package net.corda.notary.transactionservice

import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.SignatureMetadata
import net.corda.core.crypto.TransactionSignature
import net.corda.core.identity.Party
import net.corda.core.node.AppServiceHub
import net.corda.core.node.services.CordaService
import net.corda.core.serialization.SingletonSerializeAsToken
import net.corda.core.utilities.loggerFor
import java.sql.ResultSet
import java.time.Instant
import kotlin.concurrent.thread

/**
 * A very simple notary transaction extraction service that works for the single node notary
 * It takes client requests to retrieve notarised transactions from the specified timestamp and then
 * sends them back via a callback specified in the request
 * It can handle multiple client requests all specifying their own 'from' timestamp
 */
@CordaService
class SimpleNotaryTransactionService(private val serviceHub: AppServiceHub) : NotaryTransactionService, SingletonSerializeAsToken() {

    @Volatile
    private var running = true
    private val log = loggerFor<SimpleNotaryTransactionService>()
    private lateinit var transactionPollThread: Thread
    private val myIdentity = serviceHub.myInfo.legalIdentities.first()
    private val requestQueue = SimpleMessageQueue<NotaryTransactionRequest>()
    private val requests = mutableListOf<NotaryTransactionRequestInternal>()

    private val sqlForNotaryCommitLog = """
  SELECT nncl.consuming_transaction_id TRANSACTION_ID,
         nnrl.REQUESTING_PARTY_NAME REQUESTING_PARTY_NAME,
         nnrl.request_signature REQUEST_SIGNATURE,
         nnrl.request_timestamp REQUEST_TIMESTAMP
  FROM PUBLIC.node_notary_committed_states nncl
  JOIN PUBLIC.node_notary_request_log nnrl on nnrl.consuming_transaction_id = nncl.consuming_transaction_id
  WHERE nnrl.request_timestamp > '%s'
  ORDER BY REQUEST_TIMESTAMP
  """

    init {
        //Only run this if I am a notary
        if (serviceHub.networkMapCache.notaryIdentities.map { it -> it.name }.contains(myIdentity.name)) {
            println("I'm up and running ${myIdentity.name}")
            pollingForTransactions()
        }
    }

    //Client calls in with a request to get transactions from a particular UTC time
    override fun getTransactionsFrom(notaryTransactionRequest: NotaryTransactionRequest) {
        //Put the requests on a queue as we'll pick these up on the main polling
        requestQueue.stuffOnQueue(notaryTransactionRequest)
    }

    //The main processing loop - simply polls the database every second
    //obviously lots of better ways of doing this, but they do not have to be super performant
    //as metering transactions is a lazy task - nobody will be in a hurry to pay their bills.
    private fun pollingForTransactions() {
        val refreshInterval = 1000L
        transactionPollThread = thread(start = true) {
            while (running) {
                checkForRequests()
                getNotaryTransactions()
                Thread.sleep(refreshInterval)
            }
        }
    }

    //Check to see if we have any requests for transactions
    private fun checkForRequests() {
        while (requestQueue.size() > 0) {
            val request = requestQueue.getOffQueue()
            request?.apply { requests.add(NotaryTransactionRequestInternal(request)) }
        }
    }

    //Get the notary transaction for each client request - not entirely efficient, but in does mean
    //they get their own discrete sets.
    private fun getNotaryTransactions() {
        requests.forEach {
            getNotaryTransactionsByRequest(it)
        }
    }

    //Use the last timestamp in the request to retrieve notary transactions from the database
    //then update this timestamp with the date of the last transaction processed.
    private fun getNotaryTransactionsByRequest(notaryTransactionRequestInternal: NotaryTransactionRequestInternal) {
        val sqlTransactionQueryWithTime = sqlForNotaryCommitLog.format(notaryTransactionRequestInternal.lastProcessedTimeStamp)
        val notaryTransactions = mutableListOf<NotaryTransaction>()

        serviceHub.transaction {
            val session = serviceHub.jdbcSession()
            val prepStatement = session.prepareStatement(sqlTransactionQueryWithTime)
            val rs = prepStatement.executeQuery()

            while (rs.next()) {
                notaryTransactions.add(buildNotaryTransaction(rs))
            }
        }

        //Send out the transactions
        notaryTransactions.forEach { notaryTransaction ->
            notaryTransactionRequestInternal.notaryTransactionRequest.callback(notaryTransaction)
        }

        //Save the last transaction that we processed, so that we don't go back and retrieve the whole lot again
        if (notaryTransactions.size > 0) {
            val lastProcessedTimeStamp = notaryTransactions.last().transactionTime
            notaryTransactionRequestInternal.lastProcessedTimeStamp = lastProcessedTimeStamp
        }
    }

    private fun buildNotaryTransaction(rs: ResultSet): NotaryTransaction {

        /** TODO -
        *- hashed txid preserves privacy and the idea the receiving node
        would validate the signature to check it was theirs - see note below, not sure how this is gonna work */
        val hashedTx = getHashedTransactionId(rs.getString("TRANSACTION_ID"))
        val requestingPartyName = rs.getString("REQUESTING_PARTY_NAME")
        val party = getPartyFromX500String(requestingPartyName)
        val transactionDate = rs.getTimestamp("REQUEST_TIMESTAMP").toInstant()

        /** TODO -
         * I'm not sure if this is the correct way to do this and what should be put in the meta data and also how the receiver
         * of a transaction can verify this without having the original transaction Id as opposed to the hashed version
         */
        val transactionSignature = TransactionSignature(rs.getBytes("REQUEST_SIGNATURE"), party.owningKey, SignatureMetadata(1, 1))

        //TODO - calculate transaction size
        val transactionSize = 256L

        /** Example Transaction Metadata */
        val signers = TransactionSigners(listOf(myIdentity))
        val numberOfStates = TransactionNumberOfStates(2)
        val priority = TransactionPriority(1)

        val transactionMetaData = listOf(signers,numberOfStates,priority)

        val notaryTransaction = NotaryTransaction(hashedTx,
                transactionDate,
                party,
                transactionSignature,
                transactionSize,
                transactionMetaData)

        log.info("Notary Transaction $notaryTransaction")
        return notaryTransaction
    }

    private fun getHashedTransactionId(txId: String): SecureHash {
        return SecureHash.sha256(txId)
    }

    private fun getPartyFromX500String(x500String: String): Party {
        return serviceHub.networkMapCache.allNodes.flatMap { it -> it.legalIdentities }.first { it.name.toString() == x500String }
    }

    fun shutdown() {
        log.info("Polling thread shutting down")
        running = false
        transactionPollThread.join()
        log.info("Simple notary transaction service has closed")
    }
}

/**
 * This internal request keeps track of the last transaction retrieved so that
 * each time we poll the notary commit log we only get newer transactions.
 * It might be that one requester is asking for transactions from the beginning of time
 * so we could in theory run these in parallel and do other types of optimisation
 */
data class NotaryTransactionRequestInternal(val notaryTransactionRequest: NotaryTransactionRequest, var lastProcessedTimeStamp: Instant = notaryTransactionRequest.lastTransactionTime)
