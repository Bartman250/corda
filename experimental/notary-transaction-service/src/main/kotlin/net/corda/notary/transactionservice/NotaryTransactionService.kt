package net.corda.notary.transactionservice

import net.corda.core.crypto.SecureHash
import net.corda.core.crypto.TransactionSignature
import net.corda.core.identity.AbstractParty
import java.time.Instant
import java.util.*

/**
 * Specifies an interface that can be implemented for all notary types so that notarised
 * transactions can be retrieved in a uniform way
 */
interface NotaryTransactionService{
    fun getTransactionsFrom(notaryTransactionRequest: NotaryTransactionRequest)
}

data class NotaryTransaction(val hashedTx : SecureHash,
                             val transactionTime : Instant,
                             val consumingParty : AbstractParty,
                             val transactionSignature: TransactionSignature,
                             val transactionSize: Long,
                             val transactionMetaData: List<NotaryTransactionMetaData>)

data class NotaryTransactionRequest(val lastTransactionTime : Instant, val callback : (NotaryTransaction)-> Unit ){
    val requestId = UUID.randomUUID()
}

/** classes for providing meta data just have to implement this interface */
interface NotaryTransactionMetaData

data class TransactionSigners(val signers : List<AbstractParty>) : NotaryTransactionMetaData
data class TransactionNumberOfStates(val numberOfStates: Int) : NotaryTransactionMetaData
data class TransactionPriority(val priority: Int) : NotaryTransactionMetaData


