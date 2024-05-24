package com.example.model

import java.time.Instant

import cats.kernel.Eq
import cats.syntax.all._

case class OrderRow(
  orderId: String,
  market: String,
  total: BigDecimal,
  filled: BigDecimal, // state of completion of the order
  createdAt: Instant,
  updatedAt: Instant
)

object OrderRow {
  implicit val eqOrder: Eq[OrderRow] = Eq.fromUniversalEquals
}
