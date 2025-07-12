package models

import java.sql.Timestamp

case class NewsArticle(
  id: Option[Int],
  title: String,
  description: Option[String],
  url: String,
  author: Option[String],
  publishedAt: String,
  content: Option[String],
  source: String,
  category: String,
  country: String,
  language: String,
  timestamp: Long,
  createdAt: Option[Timestamp]
)
