package org.avanhecken.tpcds.query

import java.io.File

import org.avanhecken.tpcds.ArgumentParser.Args
import org.avanhecken.tpcds.statement.Statement

import scala.io.{Codec, Source}

/**
  * Factory to create the 99 queries of the TPC-DS benchmark.
  */
object QueryFactory {
  case class RawQuery(id: Short, businessQuestion: String)

  final val rawQueries: Array[RawQuery] = Array(
    RawQuery(1, """Find customers who have returned items more than 20% more often than the average customer returns for a store in a given state for a given year."""),
    RawQuery(2, """Report the ratios of weekly web and catalog sales increases from one year to the next year for each week.  That is, compute the increase of Monday, Tuesday, ... Sunday sales from one year to the following."""),
    RawQuery(3, """Report the total extended sales price per item brand of a specific manufacturer for all sales in a specific month of the year."""),
    RawQuery(4, """Find customers who spend more money via catalog than in stores. Identify preferred customers and their country of origin."""),
    RawQuery(5, """Report sales, profit, return amount, and net loss in the store, catalog, and web channels for a 14-day window. Rollup results by sales channel and channel specific sales method (store for store sales, catalog page for catalog sales and web site for web sales)"""),
    RawQuery(6, """List all the states with at least 10 customers who during a given month bought items with the price tag at least 20% higher than the average price of items in the same category."""),
    RawQuery(7, """Compute the average quantity, list price, discount, and sales price for promotional items sold in stores where the promotion is not offered by mail or a special event. Restrict the results to a specific gender, marital and educational status."""),
    RawQuery(8, """Compute the net profit of stores located in 400 Metropolitan areas with more than 10 preferred customers."""),
    RawQuery(9, """Categorize store sales transactions into 5 buckets according to the number of items sold.  Each bucket contains the average discount amount, sales price, list price, tax, net paid, paid price including tax, or net profit."""),
    RawQuery(10,"""Count the customers with the same gender, marital status, education status, purchase estimate, credit rating, dependent count, employed dependent count and college dependent count who live in certain counties and who have purchased from both stores and another sales channel during a three month time period of a given year."""),
    RawQuery(11, """Find customers whose increase in spending was large over the web than in stores this year compared to last year."""),
    RawQuery(12, """Compute the revenue ratios across item classes:  For each item in a list of given categories, during a 30 day time period, sold through the web channel compute the ratio of sales of that item to the sum of all of the sales in that item's class."""),
    RawQuery(13, """Calculate the average sales quantity, average sales price, average wholesale cost, total wholesale cost for store sales of different customer types (e.g., based on marital status, education status) including their household demographics, sales price and different combinations of state and sales profit for a given year."""),
    RawQuery(14, """This query contains multiple iterations:
Iteration 1: First identify items in the same brand, class and category that are sold in all three sales channels in two consecutive years. Then compute the average sales (quantity*list price), across all sales of all three sales channels in the same three years (average sales),. Finally, compute the total sales and the total number of sales rolled up for each channel, brand, class and category.  Only consider sales of cross channel sales that had sales larger than the average sale.
Iteration 2: Based on the previous query compare December store sales."""),
    RawQuery(15, """Report the total catalog sales for customers in selected geographical regions or who made large purchases for a given year and quarter."""),
    RawQuery(16, """Report number of orders,  total shipping costs and profits from catalog sales of particular counties and states for a given 60 day period for non-returned sales filled from an alternate warehouse."""),
    RawQuery(17, """Analyze, for each state, all items that were sold in stores in a particular quarter and returned in the next three quarters and then re-purchased by the customer through the catalog channel in the three following quarters."""),
    RawQuery(18, """Compute, for each county, the average quantity, list price, coupon amount, sales price, net profit, age, and number of dependents for all items purchased through catalog sales in a given year by customers who were born in a given list of six months and living in a given list of seven states and who also belong to a given gender and education demographic."""),
    RawQuery(19, """Select the top revenue generating products bought by out of zip code customers for a given year, month and manager."""),
    RawQuery(20, """Compute the total revenue and the ratio of total revenue to revenue by item class for specified item categories and time periods."""),
    RawQuery(21, """For all items whose price was changed on a given date, compute the percentage change in inventory between the 30-day period BEFORE the price change and the 30-day period AFTER the change. Group this information by warehouse."""),
    RawQuery(22, """For each product name, brand, class, category, calculate the average quantity on hand.  Rollup data by  product name, brand, class and category."""),
    RawQuery(23, """This query contains multiple, related iterations:
Find frequently sold items that were sold more than 4 times on any day during four consecutive years through the store sales channel. Compute the maximum store sales made by any given customer in a period of four consecutive years (same as above),. Compute the best store customers that are in the 5th percentile of sales.
Finally, compute the total web and catalog sales in a particular month made by our best store customers buying our most frequent store items."""),
    RawQuery(24, """This query contains multiple, related iterations:
Iteration 1: Calculate the total specified  monetary value of items in a specific color for store sales transactions by customer name and store, in a specific market, from customers who currently don’t live in their birth  countries and in the neighborhood of the store, and list only those customers for whom the total specified monetary value is greater than 5% of the average value.
Iteration 2: Calculate the total specified  monetary value of items in a specific color for store sales transactions by customer name and store, in a specific market, from customers who currently don’t live in their birth countries and in the neighborhood of the store, and list only those customers for whom the total specified monetary value is greater than 5% of the average value."""),
    RawQuery(25, """Get all items that were
For these items, compute the sum of net profit of store sales, net loss of store loss and net profit of catalog . Group this information by item and store."""),
    RawQuery(26, """Computes the average quantity, list price, discount, sales price for promotional items sold through the catalog channel where the promotion was not offered by mail or in an event for given gender, marital status and educational status."""),
    RawQuery(27, """For all items sold in stores located in six states during a given year, find the average quantity, average list price, average list sales price, average coupon amount for a given gender, marital status, education and customer demographic."""),
    RawQuery(28, """Calculate the average list price, number of non empty (null), list prices and number of distinct list prices of six different sales buckets of the store sales channel.  Each bucket is defined by a range of distinct items and information about list price, coupon amount and wholesale cost."""),
    RawQuery(29, """Get all items that were sold in stores in a specific month and year and which were returned in the next six months of the same year and re-purchased by the returning customer afterwards through the catalog sales channel in the following three years.
For those these items, compute the total quantity sold through the store, the quantity returned and the quantity purchased through the catalog. Group this information by item and store."""),
    RawQuery(30, """Find customers and their detailed customer data who have returned items, which they bought on the web, for an amount that is 20% higher than the average amount a customer returns in a given state in a given time period across all items.  Order the output by customer data."""),
    RawQuery(31, """List counties where the percentage growth in web sales is consistently higher compared to the percentage growth in store sales in the first three consecutive quarters for a given year."""),
    RawQuery(32, """Compute the total discounted amount for a particular manufacturer in a particular 90 day period for catalog sales whose discounts exceeded the average discount by at least 30%."""),
    RawQuery(33, """What is the monthly sales figure based on extended price for a specific month in a specific year, for manufacturers in a specific category in a given time zone.  Group sales by manufacturer identifier and sort output by sales amount, by channel, and give Total sales."""),
    RawQuery(34, """Display all customers with specific buy potentials and whose dependent count to vehicle count ratio is larger than 1.2, who in three consecutive years made purchases with between 15 and 20 items in the beginning or the end of each month in stores located in 8 counties."""),
    RawQuery(35, """For the groups of customers living in the same state, having the same gender and marital status who have purchased from stores and from either the catalog or the web during a given year, display the following:
• state, gender, marital status, count of customers
• min, max, avg, count distinct of the customer’s dependent count
• min, max, avg, count distinct of the customer’s employed dependent count
• min, max, avg, count distinct of the customer’s dependents in college count
Display / calculate the “count of customers” multiple times to emulate a potential reporting tool scenario."""),
    RawQuery(36, """Compute store sales gross profit margin ranking for items in a given year for a given list of states."""),
    RawQuery(37, """List all items and current prices sold through the catalog channel from certain manufacturers in a given $30 price range and consistently had a quantity between 100 and 500 on hand in a 60-day period."""),
    RawQuery(38, """Display count of customers with purchases from all 3 channels in a given year."""),
    RawQuery(39, """This query contains multiple, related iterations:
Iteration 1: Calculate the coefficient of variation and mean of every item and warehouse of two consecutive months
Iteration 2: Find items that had a coefficient of variation in the first months of 1.5 or large"""),
    RawQuery(40, """Compute the impact of an item price change on the sales by computing the total sales for items in a 30 day period before and after the price change.  Group the items by location of warehouse where they were delivered from."""),
    RawQuery(41, """How many items do we carry with specific combinations of color, units, size and category."""),
    RawQuery(42, """For each item and a specific year and month calculate the sum of the extended sales price of store transactions."""),
    RawQuery(43, """Report the sum of all sales from Sunday to Saturday for stores in a given data range by stores."""),
    RawQuery(44, """List the best and worst performing products measured by net profit."""),
    RawQuery(45, """Report the total web sales for customers in specific zip codes, cities, counties or states, or specific items  for a given year and quarter."""),
    RawQuery(46, """Compute the per-customer coupon amount and net profit of all "out of town" customers buying from stores located in 5 cities on weekends in three consecutive years. The customers need to fit the profile of having a specific dependent count and vehicle count.  For all these customers print the city they lived in at the time of purchase, the city in which the store is located, the coupon amount and net profit."""),
    RawQuery(47, """Find the item brands and categories for each store and company, the monthly sales figures for a specified year, where the monthly sales figure deviated more than 10% of the average monthly sales for the year, sorted by deviation and store.  Report deviation of sales from the previous and the following monthly sales."""),
    RawQuery(48, """Calculate the total sales by different types of customers (e.g., based on marital status, education status),, sales price and different combinations of state and sales profit."""),
    RawQuery(49, """Report the worst return ratios (sales to returns), of all items for each channel by quantity and currency sorted by ratio. Quantity ratio is defined as total number of sales to total number of returns. Currency ratio is defined as sum of return amount to sum of net paid."""),
    RawQuery(50, """For each store count the number of items in a specified month that were returned after 30, 60, 90, 120 and more than 120 days from the day of purchase."""),
    RawQuery(51, """Compute the count of store sales resulting from promotions, the count of all store sales and their ratio for specific categories in a particular time zone and for a given year and month."""),
    RawQuery(52, """Report the total of extended sales price for all items of a specific brand in a specific year and month."""),
    RawQuery(53, """Find the ID, quarterly sales and yearly sales of those manufacturers who produce items with specific characteristics and whose average monthly sales are larger than 10% of their monthly sales."""),
    RawQuery(54, """Find all customers who purchased items of a given category and class on the web or through catalog in a given month and year that was followed by an in-store purchase at a store near their residence in the three consecutive months.  Calculate a histogram of the revenue by these customers in $50 segments showing the number of customers in each of these revenue generated segments."""),
    RawQuery(55, """For a given year, month and store manager calculate the total store sales of any combination all brands."""),
    RawQuery(56, """Compute the monthly sales amount for a specific month in a specific year, for items with three specific colors across all sales channels.  Only consider sales of customers residing in a specific time zone.  Group sales by item and sort output by sales amount."""),
    RawQuery(57, """Find the item brands and categories for each call center and their monthly sales figures for a specified year, where the monthly sales figure deviated more than 10% of the average monthly sales for the year, sorted by deviation and call center.  Report the sales deviation from the previous and following month."""),
    RawQuery(58, """Retrieve the items generating the highest revenue and which had a revenue that was approximately equivalent across all of store, catalog and web within the week ending a given date."""),
    RawQuery(59, """Report the increase of weekly store sales from one year to the next year for each store and day of the week."""),
    RawQuery(60, """What is the monthly sales amount for a specific month in a specific year, for items in a specific category, purchased by customers residing in a specific time zone.  Group sales by item and sort output by sales amount."""),
    RawQuery(61, """Find the ratio of items sold with and without promotions in a given month and year.  Only items in certain categories sold to customers living in a specific time zone are considered."""),
    RawQuery(62, """For web sales, create a report showing the counts of orders shipped within 30 days, from 31 to 60 days, from 61 to 90 days, from 91 to 120 days and over 120 days within a given year, grouped by warehouse, shipping mode and web site."""),
    RawQuery(63, """For a given year calculate the monthly sales of items of specific categories, classes and brands that were sold in stores and group the results by store manager.  Additionally, for every month and manager print the yearly average sales of those items."""),
    RawQuery(64, """Find those stores that sold more cross-sales items from one year to another.  Cross-sale items are items that are sold over the Internet, by catalog and in store."""),
    RawQuery(65, """In a given period, for each store, report the list of items with revenue less than 10% the average revenue for all the items in that store."""),
    RawQuery(66, """Compute web and catalog sales and profits by warehouse.  Report results by month for a given year during a given 8-hour period."""),
    RawQuery(67, """Find top stores for each category based on store sales in a specific year."""),
    RawQuery(68, """Compute the per customer extended sales price, extended list price and extended tax for "out of town" shoppers buying from stores located in two cities in the first two days of each month of three consecutive years. Only consider customers with specific dependent and vehicle counts."""),
    RawQuery(69, """Count the customers with the same gender, marital status, education status, education status, purchase estimate and credit rating who live in certain states and who have purchased from stores but neither form the catalog nor from the web during a two month time period of a given year."""),
    RawQuery(70, """Compute store sales net profit ranking by state and county for a given year and determine the five most profitable states."""),
    RawQuery(71, """Select the top revenue generating products, sold during breakfast or dinner time for one month managed by a given manager across all three sales channels."""),
    RawQuery(72, """For each item, warehouse and week combination count the number of sales with and without promotion.
  omment: The adding of the scalar number 5 to d1.d_date in the predicate “d3.d_date > d1.d_date + 5” means that 5 days are added to d1.d_date."""),
    RawQuery(73, """Count the number of customers with specific buy potentials and whose dependent count to vehicle count ratio is larger than 1 and who in three consecutive years bought in stores located in 4 counties between 1 and 5 items in one purchase.  Only purchases in the first 2 days of the months are considered."""),
    RawQuery(74, """Display customers with both store and web sales in consecutive years for whom the increase in web sales exceeds the increase in store sales for a specified year."""),
    RawQuery(75, """For two consecutive years track the sales of items by brand, class and category."""),
    RawQuery(76, """Computes the average quantity, list price, discount, sales price for promotional items sold through the web channel where the promotion is not offered by mail or in an event for given gender, marital status and educational status."""),
    RawQuery(77, """Report the total sales, returns and profit for all three sales channels for a given 30 day period.  Roll up the results by channel and a unique channel location identifier."""),
    RawQuery(78, """Report the top customer / item combinations having the highest ratio of store channel sales to all other channel sales (minimum 2 to 1 ratio),, for combinations with at least one store sale and one other channel sale.  Order the output by highest ratio."""),
    RawQuery(79, """Compute the per customer coupon amount and net profit of Monday shoppers. Only purchases of three consecutive years made on Mondays in large stores by customers with a certain dependent count and with a large vehicle count are considered."""),
    RawQuery(80, """Report extended sales, extended net profit and returns in the store, catalog, and web channels for a 30 day window for items with prices larger than $50 not promoted on television, rollup results by sales channel and channel specific sales means (store for store sales, catalog page for catalog sales and web site for web sales),"""),
    RawQuery(81, """Find customers and their detailed customer data who have returned items bought from the catalog more than 20 percent the average customer returns for customers in a given state in a given time period.  Order output by customer data."""),
    RawQuery(82, """Find customers who tend to spend more money (net-paid), on-line than in stores."""),
    RawQuery(83, """Retrieve the items with the highest number of returns where the number of returns was approximately equivalent across all store, catalog and web channels (within a tolerance of +/- 10%),, within the week ending a given date."""),
    RawQuery(84, """List all customers living in a specified city, with an income between 2 values."""),
    RawQuery(85, """For all web return reason calculate the average sales, average refunded cash and average return fee by different combinations of customer and sales types (e.g., based on marital status, education status, state and sales profit),."""),
    RawQuery(86, """Rollup the web sales for a given year by category and class, and rank the sales among peers within the parent, for each group compute  sum of sales, location with the hierarchy and rank within the group."""),
    RawQuery(87, """Count how many customers have ordered on the same day items on the web and the catalog and on the same day have bought items in a store."""),
    RawQuery(88, """How many items do we sell between pacific times of a day in certain stores to customers with one dependent count and 2 or less vehicles registered or 2 dependents with 4 or fewer vehicles registered or 3 dependents and five or less vehicles registered.  In one row break the counts into sells from 8:30 to 9, 9 to 9:30, 9:30 to 10 ... 12 to 12:30"""),
    RawQuery(89, """Within a year list all month and combination of item categories, classes and brands that have had monthly sales larger than 0.1 percent of the total yearly sales."""),
    RawQuery(90, """What is the ratio between the number of items sold over the internet in the morning (8 to 9am), to the number of items sold in the evening (7 to 8pm), of customers with a specified number of dependents. Consider only websites with a high amount of content."""),
    RawQuery(91, """Display total returns of catalog sales by call center and manager in a particular month for male customers of unknown education or female customers with advanced degrees with a specified buy potential and from a particular time zone."""),
    RawQuery(92, """Compute the total discount on web sales of items from a given manufacturer over a particular 90 day period for sales whose discount exceeded 30% over the average discount of items from that manufacturer in that period of time."""),
    RawQuery(93, """For a given merchandise return reason, report on customers’ total cost of purchases minus the cost of returned items."""),
    RawQuery(94, """Produce a count of web sales and total shipping cost and net profit in a given 60 day period to customers in a given state from a named web site for non returned orders shipped from more than one warehouse."""),
    RawQuery(95, """Produce a count of web sales and total shipping cost and net profit in a given 60 day period to customers in a given state from a named web site for returned orders shipped from more than one warehouse."""),
    RawQuery(96, """Compute a count of sales from a named store to customers with a given number of dependents made in a specified half hour period of the day."""),
    RawQuery(97, """Generate counts of promotional sales and total sales, and their ratio from the web channel for a particular item category and month to customers in a given time zone."""),
    RawQuery(98, """Report on items sold in a given 30 day period, belonging to the specified category."""),
    RawQuery(99, """For catalog sales, create a report showing the counts of orders shipped within 30 days, from 31 to 60 days, from 61 to 90 days, from 91 to 120 days and over 120 days within a given year, grouped by warehouse, call center and shipping mode.""")
  )

  final val ids: Array[Short] = rawQueries.map(_.id)

  /**
    * Generate a list of queries based on a list of ids passed on as argument but when not provided then return all queries.
    *
    * @param args
    * @return
    */
  def generateQueries(name: String, args: Args): Array[Query] = {
    def statements(name: String, queryId: Short, args: Args): Array[Statement] = {
      val resourceLocation: String = args("resource_location")
      val sqlFileLocation: File = new File(s"$resourceLocation/queries/query$queryId.sql")

      implicit val codec = Codec("UTF-8")
      Source.fromFile(sqlFileLocation.getPath)
        .mkString
        .trim
        .split(";")
        .zipWithIndex
        .map {
          case (text, statementId) =>
            val id: String = s"$name.$queryId.$statementId"
            Statement(id, text)
        }
    }

    val queryIds: Option[Set[Short]] = args
      .get("ids")
      .map(_.split(",").map(_.toShort).toSet)

    /** Map ids to queries.  If no ids are given then return all queries. */
    queryIds
      .map(ids => rawQueries.filter(rawQuery => ids.contains(rawQuery.id)))
      .getOrElse(rawQueries)
      .map {
        rawQuery =>
          Query(rawQuery.id, rawQuery.businessQuestion, UNKNOWN, statements(name, rawQuery.id, args))
      }
  }
}

