package com.example.stream

import cats.data.EitherT
import cats.effect.{Ref, Resource}
import cats.effect.kernel.Async
import cats.effect.std.Queue
import fs2.Stream
import org.typelevel.log4cats.Logger
import cats.syntax.all._
import cats.effect.kernel.Outcome.{Canceled, Errored, Succeeded}
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.PreparedQueries
import skunk._

import scala.concurrent.duration.FiniteDuration
import com.example.Main
import org.checkerframework.checker.units.qual.s
import scala.concurrent.duration._
import cats.instances.queue
import com.example.persistence.Queries
import java.rmi.UnexpectedException
import fs2.Chunk
import scala.collection.immutable
import cats.effect.kernel

// All SQL queries inside the Queries object are correct and should not be changed
final class TransactionStream[F[_]](
  operationTimer: FiniteDuration,
  orders: Queue[F, OrderRow],
  session: Resource[F, Session[F]],
  transactionCounter: Ref[F, Int], // updated if long IO succeeds
  stateManager: StateManager[F],   // utility for state management
  maxConcurrent: Int
)(implicit F: Async[F], logger: Logger[F]) {

  implicit val ordering: Ordering[BigDecimal] = Ordering.BigDecimal

  /** The purpose of the function is to aggregate orderRows having similar Ids (sorting them when necessary)
    */
  def windowing(state: Map[String, List[OrderRow]], input: List[OrderRow]): Map[String, List[OrderRow]] = {
    input match {
      case Nil => state
      case head :: next =>
        state.get(head.orderId) match {
          case None => windowing(state + (head.orderId -> List(head)), next)
          case Some(previous) =>
            val newSt = state - (previous.head.orderId)
            val newList =
              if (previous.exists(ord => ord.filled > head.filled)) previous // We ignore the incomming orderRow
              else (head :: previous).distinct.sortBy(_.filled)
            windowing(state + (head.orderId -> newList), next)
        }
    }
  }

  def stream: Stream[F, Unit] = {
    Stream
      .fromQueueUnterminated(orders)
      .groupWithin(100, 1.second) // little buffering to catch duplicates / unordered events
      .through {
        _.mapAccumulate(Map.empty[String, List[OrderRow]]) { case (resultMap, chunk) =>
          val newState = windowing(resultMap, chunk.toList)
          newState -> chunk
        }.flatMap { case (acc, _) => Stream.emits(acc.values.toSeq) }
      }
      .parEvalMap(maxConcurrent)(commonOrders =>
        commonOrders.traverse(processUpdate)
      ) // evaluating orders having same id sequentially
      .as(())
  }

  /** finalizer for our TransactionStream resource
    */
  def finalizeQueue: F[Unit] = {
    def go(queue: Queue[F, OrderRow]): F[Unit] = {
      queue.tryTake.flatMap {
        case Some(orderRow) => processUpdate(orderRow) *> go(queue)
        case _              => logger.debug("Finished the finalization!")
      }
    }
    go(orders)
  }

  // Application should shut down on error,
  // If performLongRunningOperation fails, we don't want to insert/update the records
  // Transactions always have positive amount
  // Order is executed if total == filled
  private def processUpdate(updatedOrder: OrderRow): F[Unit] = {

    def retryWithBackOff(sleep: Duration, effect: F[OrderRow]): F[OrderRow] = {
      F.sleep(sleep) *> effect.handleErrorWith(_ => retryWithBackOff(sleep, effect))
    }

    // Handling the case where the update comes too early
    def getOrderRowSafe(orderRow: OrderRow, queries: PreparedQueries[F]): F[OrderRow] = {
      val maxWaitEffect       = F.sleep(5.second) // business requirement
      val getOrderStateEffect = stateManager.getOrderState(orderRow, queries)
      val result              = retryWithBackOff(50.millis, getOrderStateEffect)
      F.race(maxWaitEffect, result).flatMap {
        case Left(_) =>
          F.raiseError(throw new UnexpectedException("Order not found in the state Dropping the update!!"))
        case Right(orderRow) => F.pure(orderRow)
      }
    }

    PreparedQueries(session)
      .use { queries =>
        def go(transaction: TransactionRow, params: BigDecimal *: String *: EmptyTuple) =
          performLongRunningOperation(transaction).value.flatMap { opsEither =>
            opsEither match {
              case Left(err) =>
                logger.error(err)(s"Got error when performing long running IO!") *> F.raiseError(err).void
              case Right(_) =>
                // update order with params
                queries.updateOrder.execute(params).void *>
                  // insert the transaction
                  F.whenA[Unit](transaction.amount > 0)(
                    queries.insertTransaction.execute(transaction).void
                  )
            }
          }
        for {
          // Get current known order state
          state <- getOrderRowSafe(updatedOrder, queries)
          transaction = TransactionRow(state = state, updated = updatedOrder)

          // parameters for order update
          updatedFill = updatedOrder.filled
          params      = updatedFill *: state.orderId *: EmptyTuple
          _ <- if (state.eqv(updatedOrder) || transaction.amount <= 0) logger.debug("No-Op")
               else
                 F.guaranteeCase(go(transaction, params))(out =>
                   out match {
                     case Canceled() =>
                       PreparedQueries(session)
                         .use { queries =>
                           // update order with params
                           queries.updateOrder.execute(params).void *>
                             // insert the transaction
                             queries.insertTransaction.execute(transaction).void *>
                             transactionCounter
                               .updateAndGet(_ + 1)
                               .void
                         }
                     case Errored(e) =>
                       logger.error(e)(s"Got error when performing long running IO!")
                     case Succeeded(fib) => fib
                   }
                 )
        } yield ()
      }
  }

  // represents some long running IO that can fail
  private def performLongRunningOperation(transaction: TransactionRow): EitherT[F, Throwable, Unit] = {
    EitherT.liftF[F, Throwable, Unit](
      F.sleep(operationTimer) *>
        stateManager.getSwitch.flatMap {
          case false =>
            if (transaction.amount <= 0) F.unit
            else
              transactionCounter
                .updateAndGet(_ + 1)
                .flatMap(count =>
                  logger.info(
                    s"Updated counter to $count by transaction with amount ${transaction.amount} for order ${transaction.orderId}!"
                  )
                )
          case true => F.raiseError(throw new Exception("Long running IO failed!"))
        }
    )
  }

  // helper methods for testing
  def publish(update: OrderRow): F[Unit]                                          = orders.offer(update)
  def getCounter: F[Int]                                                          = transactionCounter.get
  def setSwitch(value: Boolean): F[Unit]                                          = stateManager.setSwitch(value)
  def addNewOrder(order: OrderRow, insert: PreparedCommand[F, OrderRow]): F[Unit] = stateManager.add(order, insert)
  // helper methods for testing
  def queueSize: F[Int] = orders.size
}

object TransactionStream {

  def apply[F[_]: Async: Logger](
    operationTimer: FiniteDuration,
    session: Resource[F, Session[F]],
    maxConcurrent: Int = 10
  ): Resource[F, TransactionStream[F]] = {

    val acquire = for {
      counter      <- Ref.of(0)
      queue        <- Queue.unbounded[F, OrderRow]
      stateManager <- StateManager.apply
    } yield new TransactionStream[F](
      operationTimer,
      queue,
      session,
      counter,
      stateManager,
      maxConcurrent
    )
    Resource.make(acquire)(_.finalizeQueue)
  }
}
