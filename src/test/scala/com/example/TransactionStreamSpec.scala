package com.example

import cats.effect.{IO, Resource}
import cats.implicits.toTraverseOps
import com.example.model.{OrderRow, TransactionRow}
import com.example.persistence.Queries
import com.example.stream.TransactionStream
import org.scalatest.OptionValues
import org.scalatest.wordspec.FixtureAsyncWordSpec
import skunk._

import java.time.Instant
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import java.util.Calendar
import java.time.temporal.TemporalUnit
import java.time.temporal.ChronoUnit

// All SQL queries are correct and should not be changed
class TransactionStreamSpec extends FixtureAsyncWordSpec with BaseIOSpec with OptionValues {

  // we will check transactions counter and database state after the test
  case class Result(counter: Int, orders: List[OrderRow], transactions: List[TransactionRow])

  "TransactionsStream" when {

    "processing orders" must {

      // "T1: process one updates resulting in one transaction" in { fxt =>
      //   val ts = Instant.now
      //   val order = OrderRow(
      //     orderId = "example_id",
      //     market = "btc_eur",
      //     total = 0.8,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val firstUpdate = order.copy(filled = 0.8)

      //   val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
      //     for {
      //       // establish some state by adding new order
      //       _ <- stream.addNewOrder(order, insertO)
      //       // start the application
      //       streamFiber <- stream.stream.compile.drain.start
      //       // publish the update
      //       _ <- stream.publish(firstUpdate)
      //       // wait for processing
      //       _ <- IO.sleep(5.seconds)
      //       _ <- streamFiber.cancel
      //       // check result
      //       results <- getResults(stream, getO, getT)
      //     } yield results
      //   }
      //   test.map { case Result(counter, orders, transactions) =>
      //     val updated = orders.find(_.orderId == order.orderId).value
      //     val txn     = transactions.find(_.orderId == order.orderId).value

      //     counter shouldBe 1
      //     updated.filled shouldBe 0.8
      //     txn.amount shouldBe 0.8
      //   }
      // }

      // "T2: process zero amount update resulting in zero transactions" in { fxt =>
      //   val ts = Instant.now
      //   val order = OrderRow(
      //     orderId = "example_id",
      //     market = "btc_eur",
      //     total = 0.8,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val firstUpdate = order // zero amount filled, basically the same as in the state

      //   val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
      //     for {
      //       // establish state
      //       _ <- stream.addNewOrder(order, insertO)
      //       // run the stream and process the update
      //       streamFiber <- stream.stream.compile.drain.start
      //       _           <- stream.publish(firstUpdate)
      //       _           <- IO.sleep(3.seconds)
      //       _           <- streamFiber.cancel
      //       results     <- getResults(stream, getO, getT)
      //     } yield results
      //   }
      //   test.map { case Result(counter, orders, transactions) =>
      //     val updated = orders.find(_.orderId == order.orderId).value
      //     val txn     = transactions.find(_.orderId == order.orderId)

      //     counter shouldBe 0
      //     updated.filled shouldBe 0
      //     txn shouldBe empty
      //   }
      // }

      // "T3: process a case where the same message is delivered twice" in { fxt =>
      //   val ts = Instant.now
      //   val order = OrderRow(
      //     orderId = "example_id",
      //     market = "btc_eur",
      //     total = 0.8,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val update = order.copy(filled = 0.5)

      //   val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
      //     for {
      //       // establish state
      //       _ <- stream.addNewOrder(order, insertO)
      //       // run the stream and process the update
      //       streamFiber <- stream.stream.compile.drain.start
      //       // deliver the same update twice
      //       _       <- stream.publish(update)
      //       _       <- stream.publish(update)
      //       _       <- IO.sleep(3.seconds)
      //       _       <- streamFiber.cancel
      //       results <- getResults(stream, getO, getT)
      //     } yield results
      //   }
      //   test.map { case Result(counter, orders, transactions) =>
      //     val updated = orders.find(_.orderId == order.orderId).value
      //     val txn     = transactions.find(_.orderId == order.orderId).value

      //     // test proven wrong
      //     counter shouldBe 1
      //     updated.filled shouldBe 0.5
      //     txn.amount shouldBe 0.5
      //   }
      // }

      // "T4: process two updates resulting in two transactions" in { fxt =>
      //   val ts = Instant.now
      //   val order = OrderRow(
      //     orderId = "example_id",
      //     market = "btc_eur",
      //     total = 0.8,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val firstUpdate = order.copy(filled = 0.5)

      //   val secondUpdate = order.copy(filled = 0.8)

      //   val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
      //     for {
      //       _           <- stream.addNewOrder(order, insertO)
      //       streamFiber <- stream.stream.compile.drain.start
      //       _           <- stream.publish(firstUpdate)
      //       _           <- stream.publish(secondUpdate)
      //       _           <- IO.sleep(5.seconds)
      //       _           <- streamFiber.cancel
      //       results     <- getResults(stream, getO, getT)
      //     } yield results
      //   }
      //   test.map { case Result(counter, orders, transactions) =>
      //     val updated = orders.find(_.orderId == order.orderId).value
      //     val txn     = transactions.filter(_.orderId == order.orderId)

      //     counter shouldBe 2
      //     updated.filled shouldBe 0.8
      //     txn.size shouldBe 2
      //     txn.map(_.amount).sum shouldBe 0.8
      //   }
      // }

      // "T5: process three transactions from two independent orders" in { fxt =>
      //   val ts = Instant.now
      //   val order1 = OrderRow(
      //     orderId = "example_id_1",
      //     market = "btc_eur",
      //     total = 0.8,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val order2 = order1.copy(orderId = "example_id_2", total = 1, market = "btc_usd")

      //   val firstUpdate = order1.copy(filled = 0.5)

      //   val secondUpdate = order2.copy(filled = 1)

      //   val thirdUpdate = order1.copy(filled = 0.8)

      //   val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
      //     for {
      //       // establish state with two orders
      //       _           <- stream.addNewOrder(order1, insertO)
      //       _           <- stream.addNewOrder(order2, insertO)
      //       streamFiber <- stream.stream.compile.drain.start
      //       // updates go like order1 -> order2 -> order1
      //       _       <- stream.publish(firstUpdate)
      //       _       <- stream.publish(secondUpdate)
      //       _       <- stream.publish(thirdUpdate)
      //       _       <- IO.sleep(5.seconds)
      //       _       <- streamFiber.cancel
      //       results <- getResults(stream, getO, getT)
      //     } yield results
      //   }
      //   test.map { case Result(counter, orders, transactions) =>
      //     val updated1 = orders.find(_.orderId == order1.orderId).value
      //     val updated2 = orders.find(_.orderId == order2.orderId).value
      //     val txn1     = transactions.filter(_.orderId == order1.orderId)
      //     val txn2     = transactions.filter(_.orderId == order2.orderId)

      //     counter shouldBe 3

      //     updated1.filled shouldBe 0.8
      //     updated2.filled shouldBe 1

      //     txn1.size shouldBe 2
      //     txn2.size shouldBe 1

      //     txn1.map(_.amount).sum shouldBe 0.8
      //     txn2.map(_.amount).sum shouldBe 1
      //   }
      // }

      // "T6: not update records when long running IO fails" in { fxt =>
      //   val ts = Instant.now
      //   val order = OrderRow(
      //     orderId = "example_id",
      //     market = "btc_eur",
      //     total = 0.8,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val firstUpdate = order.copy(filled = 0.8)

      //   val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
      //     for {
      //       // establish state
      //       _ <- stream.addNewOrder(order, insertO)
      //       // start the app
      //       streamFiber <- stream.stream.compile.drain.start
      //       // set the switch so that performLongRunningOperation fails every time
      //       _ <- stream.setSwitch(true)
      //       // start processing by publishing the update
      //       _       <- stream.publish(firstUpdate)
      //       _       <- IO.sleep(5.seconds)
      //       _       <- streamFiber.cancel
      //       results <- getResults(stream, getO, getT)
      //     } yield results
      //   }
      //   test.map { case Result(counter, orders, transactions) =>
      //     val updated = orders.find(_.orderId == order.orderId).value
      //     val txn     = transactions.find(_.orderId == order.orderId)

      //     counter shouldBe 0
      //     updated.filled shouldBe 0
      //     txn shouldBe empty
      //   }
      // }

      // "T7: process current update on stream shutdown" in { fxt =>
      //   val ts = Instant.now
      //   val order = OrderRow(
      //     orderId = "example_id",
      //     market = "btc_eur",
      //     total = 0.8,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val firstUpdate = order.copy(filled = 0.8)

      //   val test = getResources(fxt, 5.second).use { case Resources(stream, getO, getT, insertO, _) =>
      //     for {
      //       // establish state
      //       _ <- stream.addNewOrder(order, insertO)
      //       // start the app + send first update
      //       streamFiber <- stream.stream.compile.drain.start
      //       _           <- stream.publish(firstUpdate)
      //       _           <- IO.sleep(5.second)
      //       // long running IO should run for 5 seconds and be completed
      //       _       <- streamFiber.cancel
      //       results <- getResults(stream, getO, getT)
      //     } yield results
      //   }
      //   test.map { case Result(counter, orders, transactions) =>
      //     val updated = orders.find(_.orderId == order.orderId).value
      //     val txn     = transactions.find(_.orderId == order.orderId).value

      //     counter shouldBe 1
      //     updated.filled shouldBe 0.8
      //     txn.amount shouldBe 0.8
      //   }
      // }

      // test setup is correct, don't change it!
      // "T8: process all remaining transactions inside the queue" in { fxt =>
      //   val ts = Instant.now
      //   val order = OrderRow(
      //     orderId = "example_id",
      //     market = "btc_eur",
      //     total = 100,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val update = order.copy(filled = 100)

      //   // split the update into 100 transactions of amount 1 and add sequence numbers
      //   val updates = (0 to 99).toList.map(i => update.copy(filled = 1 + i))

      //   // push the updates into the stream queue, updates will be processed on shutdown
      //   val runIOs = getResources(fxt, 100.millis).use { case Resources(stream, _, _, insertOrder, _) =>
      //     for {
      //       // establish state
      //       _ <- stream.addNewOrder(order, insertOrder)
      //       // push updates but don't start the stream processing!
      //       _ <- updates.traverse(stream.publish)
      //     } yield ()
      //   }

      //   // we want to check database state, so there's no truncate of all tables
      //   val test = runIOs *> getResources(fxt, 5.seconds, withTruncate = false).use {
      //     case Resources(stream, getO, getT, _, _) =>
      //       getResults(stream, getO, getT)
      //   }

      //   test
      //     .map { case Result(_, orders, transactions) =>
      //       val updated = orders.find(_.orderId == order.orderId).value
      //       val txn     = transactions.filter(_.orderId == order.orderId)

      //       updated.filled shouldBe 100
      //       txn.map(_.amount).sum shouldBe 100
      //       txn.size shouldBe 100
      //     }
      // }

      // // Hint: we can drop the update if it is older than 5 seconds and we don't have it in the state.
      // "T9: handle race condition where update arrives before addition to the state" in { fxt =>
      //   val ts = Instant.now
      //   val order = OrderRow(
      //     orderId = "example_id",
      //     market = "btc_eur",
      //     total = 0.8,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val firstUpdate = order.copy(filled = 0.8)

      //   val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
      //     for {
      //       // start the stream
      //       streamFiber <- stream.stream.compile.drain.start
      //       // publish first update
      //       _ <- stream.publish(firstUpdate)
      //       _ <- IO.sleep(1.second)
      //       // establish state and wait for processing
      //       _       <- stream.addNewOrder(order, insertO)
      //       _       <- IO.sleep(7.seconds)
      //       _       <- streamFiber.cancel
      //       results <- getResults(stream, getO, getT)
      //     } yield results
      //   }
      //   test.map { case Result(counter, orders, transactions) =>
      //     val updated = orders.find(_.orderId == order.orderId).value
      //     val txn     = transactions.find(_.orderId == order.orderId).value

      //     counter shouldBe 1
      //     updated.filled shouldBe 0.8
      //     txn.amount shouldBe 0.8
      //   }
      // }

      // "T10: handle race condition where bigger update arrives first" in { fxt =>
      //   val ts = Instant.now
      //   val order = OrderRow(
      //     orderId = "example_id",
      //     market = "btc_eur",
      //     total = 0.8,
      //     filled = 0,
      //     createdAt = ts,
      //     updatedAt = ts
      //   )

      //   val firstUpdate = order.copy(filled = 0.8)

      //   val secondUpdate = order.copy(filled = 0.5)

      //   val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
      //     for {
      //       _           <- stream.addNewOrder(order, insertO)
      //       streamFiber <- stream.stream.compile.drain.start
      //       // Hint: we can treat it as one big transaction, instead of splitting into two
      //       _       <- stream.publish(firstUpdate)
      //       _       <- stream.publish(secondUpdate)
      //       _       <- IO.sleep(5.seconds)
      //       _       <- streamFiber.cancel
      //       results <- getResults(stream, getO, getT)
      //     } yield results
      //   }
      //   test.map { case Result(counter, orders, transactions) =>
      //     val updated = orders.find(_.orderId == order.orderId).value
      //     val txn     = transactions.filter(_.orderId == order.orderId)

      //     counter shouldBe 1
      //     updated.filled shouldBe 0.8
      //     txn.size shouldBe 1
      //     txn.map(_.amount).sum shouldBe 0.8
      //   }
      // }

      "T11: process three transactions from two independent orders concurrently" in { fxt =>
        val ts = Instant.now
        val order1 = OrderRow(
          orderId = "example_id_1",
          market = "btc_eur",
          total = 0.8,
          filled = 0,
          createdAt = ts,
          updatedAt = ts
        )

        val order2 = order1.copy(orderId = "example_id_2", total = 1, market = "btc_usd")

        val firstUpdate = order1.copy(filled = 0.5)

        val secondUpdate = order2.copy(filled = 1)

        val thirdUpdate = order1.copy(filled = 0.8)

        val test = getResources(fxt, 100.millis).use { case Resources(stream, getO, getT, insertO, _) =>
          for {
            // establish state with two orders
            _           <- stream.addNewOrder(order1, insertO)
            _           <- stream.addNewOrder(order2, insertO)
            streamFiber <- stream.stream.compile.drain.start
            // updates go like order1 -> order2 -> order1
            _       <- stream.publish(firstUpdate)
            _       <- stream.publish(secondUpdate)
            _       <- stream.publish(thirdUpdate)
            _       <- IO.sleep(5.seconds)
            _       <- streamFiber.cancel
            results <- getResults(stream, getO, getT)
          } yield results
        }
        test.map { case Result(counter, orders, transactions) =>
          val updated1 = orders.find(_.orderId == order1.orderId).value
          val updated2 = orders.find(_.orderId == order2.orderId).value
          val txn1     = transactions.filter(_.orderId == order1.orderId)
          val txn2     = transactions.filter(_.orderId == order2.orderId)

          val trxTime1 = txn1.head.createdAt
          val trxTime2 = txn2.head.createdAt

          counter shouldBe 3

          updated1.filled shouldBe 0.8
          updated2.filled shouldBe 1

          txn1.size shouldBe 2
          txn2.size shouldBe 1

          txn1.map(_.amount).sum shouldBe 0.8
          txn2.map(_.amount).sum shouldBe 1

          val approximateTn1CreateTime = trxTime1.truncatedTo(ChronoUnit.MILLIS)
          val approximateTn2CreateTime = trxTime2.truncatedTo(ChronoUnit.MILLIS)

          approximateTn1CreateTime shouldEqual approximateTn2CreateTime

        }
      }

      // }
    }
  }
  // timer for long IO operation
  def getResources(
    fxt: FixtureParam,
    timer: FiniteDuration,
    withTruncate: Boolean = true,
    maxConcurrent: Int = 10
  ): Resource[IO, Resources] = {
    for {
      _                 <- Resource.eval(IO.whenA(withTruncate)(truncateAllTables(fxt.databasePool)))
      selectOrder       <- fxt.databasePool.sessionResource.evalMap(_.prepare(Queries.getAllOrders))
      selectTransaction <- fxt.databasePool.sessionResource.evalMap(_.prepare(Queries.getAllTransactions))
      insertOrder       <- fxt.databasePool.sessionResource.evalMap(_.prepare(Queries.insertOrder))
      insertTransaction <- fxt.databasePool.sessionResource.evalMap(_.prepare(Queries.insertTransaction))
      stream            <- TransactionStream.apply(timer, fxt.databasePool.sessionResource, maxConcurrent)
    } yield Resources(stream, selectOrder, selectTransaction, insertOrder, insertTransaction)
  }

  def measure[A](fa: IO[A]): IO[(A, FiniteDuration)] = {
    for {
      start  <- IO.monotonic
      result <- fa
      finish <- IO.monotonic
    } yield (result, finish.-(start))
  }

  private def getResults(
    stream: TransactionStream[IO],
    getO: PreparedQuery[IO, Void, OrderRow],
    getT: PreparedQuery[IO, Void, TransactionRow]
  ): IO[Result] = {
    for {
      counterValue <- stream.getCounter
      orders       <- getO.stream(skunk.Void, 50).compile.toList
      transactions <- getT.stream(skunk.Void, 50).compile.toList
    } yield Result(counterValue, orders, transactions)
  }

  case class Resources(
    transactionsStream: TransactionStream[IO],
    getAllOrders: PreparedQuery[IO, Void, OrderRow],
    getAllTransactions: PreparedQuery[IO, Void, TransactionRow],
    insertOrder: PreparedCommand[IO, OrderRow],
    insertTransaction: PreparedCommand[IO, TransactionRow]
  )
}
