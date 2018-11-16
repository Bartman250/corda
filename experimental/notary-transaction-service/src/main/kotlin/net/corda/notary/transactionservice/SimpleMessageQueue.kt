package net.corda.notary.transactionservice

import java.util.*

/** Simple Synchronised Queue which could equally be an Artemis Queue for remote calling of the api
* This is used to pass requests into the service and pass notarised transactions out of the service */
class SimpleMessageQueue<T> {
    private val txQueue = ArrayDeque<T>()
    fun stuffOnQueue(t : T){
        synchronized(txQueue) {
            txQueue.add(t)
        }
    }
    fun getOffQueue() : T? {
        var queueItem : T? = null
        synchronized(txQueue) {
            if(txQueue.size>0)
            queueItem = txQueue.remove()
        }
        return queueItem
    }
    fun size(): Int {
        synchronized(txQueue){
            return txQueue.size
        }
    }
}