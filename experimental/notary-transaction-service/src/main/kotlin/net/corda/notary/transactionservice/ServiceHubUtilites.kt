package net.corda.notary.transactionservice

import net.corda.core.node.AppServiceHub
import net.corda.core.node.ServiceHub
import net.corda.node.services.api.ServiceHubInternal

/**
 * Provides service hub extension so that we can access database transactions
 */

fun <T> ServiceHub.transaction(fn: () -> T) : T {
    val cls = this.javaClass // should be AbstractNode$AppServiceHubImpl
    val database = when (this) {
        is AppServiceHub -> {
            val field = cls.getDeclaredField("serviceHub")
            field.isAccessible = true
            val serviceHub = field.get(this) as ServiceHubInternal
            serviceHub.database
        }
        is ServiceHubInternal -> {
            this.database
        }
        else -> {
            throw RuntimeException("this is not the kind of service hub you were looking for!")
        }
    }
    return database.transaction {
        fn()
    }
}