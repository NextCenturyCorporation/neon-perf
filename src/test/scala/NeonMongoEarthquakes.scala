
import scala.concurrent.duration._

import io.gatling.core.Predef._
import io.gatling.http.Predef._
import io.gatling.jdbc.Predef._
import scala.util.Random
import org.joda.time.{Period, DateTime}

object Joda {
    implicit def dateTimeOrdering: Ordering[DateTime] = Ordering.fromLessThan(_ isBefore _)
}
import Joda._


class NeonMongoEarthquakes extends Simulation {

    val serverUri = "http://ec2-54-174-201-125.compute-1.amazonaws.com:8080"
	val httpProtocol = http
		.baseURL(serverUri)
		.inferHtmlResources(BlackList(""".*\.js""", """.*\.css""", """.*\.png""", """.*\.html""", """.*\.gif""", """.*\.jpeg""", """.*\.jpg""", """.*\.ico""", """.*\.woff""", """.*\.(t|o)tf"""), WhiteList())

	val headers_0 = Map(
		"Accept" -> "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Pragma" -> "no-cache")

	val headers_1 = Map("X-Requested-With" -> "XMLHttpRequest")

	val headers_2 = Map(
		"Pragma" -> "no-cache",
        "Content-Type" -> "application/json",
		"X-Requested-With" -> "XMLHttpRequest")

	val headers_3 = Map(
		"Accept" -> "*/*",
		"X-Requested-With" -> "XMLHttpRequest")

	val headers_22 = Map(
		"Content-Type" -> "text/plain; charset=UTF-8",
		"Pragma" -> "no-cache",
		"X-Requested-With" -> "XMLHttpRequest")

    val uri1 = serverUri + "/neon"

    val databaseType = "elasticsearch"

    val countBody = StringBody("""
      {
        "sortClauses": [],
        "filter": {
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [],
        "isDistinct": false,
        "aggregates": [
          {
            "name": "count",
            "field": "*",
            "operation": "count"
          }
        ]
      }
    """)
    val countRequest = http("count")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(countBody)

    val groupByNet = StringBody("""
      {
        "limitClause": {
          "limit": 16000
        },
        "sortClauses": [
          {
            "sortOrder": -1,
            "fieldName": "count"
          }
        ],
        "filter": {
          "whereClause": {
            "rhs": null,
            "operator": "!=",
            "lhs": "net",
            "type": "where"
          },
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [
          "countby-test-earthquakes-1a33b7f7-4944-4232-a1f6-31eb283a395f"
        ],
        "groupByClauses": [
          {
            "field": "net",
            "type": "single"
          }
        ],
        "isDistinct": false,
        "aggregates": [
          {
            "name": "count",
            "field": "*",
            "operation": "count"
          }
        ]
      }
    """)
    val groupByNetRequest = http("groupByNet")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType + "?ignoredFilterIds=countby-test-earthquakes-1a33b7f7-4944-4232-a1f6-31eb283a395f")
			.headers(headers_2)
			.body(groupByNet)

    val groupByNetBar = StringBody("""
      {
        "limitClause": {
          "limit": 150
        },
        "sortClauses": [
          {
            "sortOrder": -1,
            "fieldName": "Count"
          }
        ],
        "filter": {
          "whereClause": {
            "rhs": null,
            "operator": "!=",
            "lhs": "net",
            "type": "where"
          },
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [
          "barchart-test-earthquakes-2b74b1f7-15a1-4124-80bd-46f99eb484c6"
        ],
        "groupByClauses": [
          {
            "field": "net",
            "type": "single"
          }
        ],
        "isDistinct": false,
        "aggregates": [
          {
            "name": "Count",
            "field": "*",
            "operation": "count"
          }
        ]
      }
    """)
    val groupByNetBarRequest = http("groupByNetBar")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType + "?ignoredFilterIds=barchart-test-earthquakes-2b74b1f7-15a1-4124-80bd-46f99eb484c6")
			.headers(headers_2)
			.body(groupByNetBar)

    val timeSeries = StringBody("""
      {
        "sortClauses": [
          {
            "sortOrder": 1,
            "fieldName": "date"
          }
        ],
        "filter": {
          "whereClause": {
            "rhs": null,
            "operator": "!=",
            "lhs": "time",
            "type": "where"
          },
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [
          {
            "name": "year",
            "field": "time",
            "operation": "year",
            "type": "function"
          },
          {
            "name": "month",
            "field": "time",
            "operation": "month",
            "type": "function"
          },
          {
            "name": "day",
            "field": "time",
            "operation": "dayOfMonth",
            "type": "function"
          },
          {
            "field": "type",
            "type": "single"
          }
        ],
        "isDistinct": false,
        "aggregates": [
          {
            "name": "value",
            "field": "*",
            "operation": "count"
          },
          {
            "name": "date",
            "field": "time",
            "operation": "min"
          }
        ]
      }
    """)
    val timeSeriesRequest = http("timeSeries")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(timeSeries)

    val timeSeriesExtended = StringBody("""
      {
        "sortClauses": [
          {
            "sortOrder": 1,
            "fieldName": "date"
          }
        ],
        "filter": {
          "whereClause": {
            "rhs": null,
            "operator": "!=",
            "lhs": "time",
            "type": "where"
          },
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [
          "date-test-earthquakes-46d60c31-3f27-40c7-80cf-61a581af5545"
        ],
        "groupByClauses": [
          {
            "name": "year",
            "field": "time",
            "operation": "year",
            "type": "function"
          },
          {
            "name": "month",
            "field": "time",
            "operation": "month",
            "type": "function"
          },
          {
            "name": "day",
            "field": "time",
            "operation": "dayOfMonth",
            "type": "function"
          }
        ],
        "isDistinct": false,
        "aggregates": [
          {
            "name": "count",
            "field": "*",
            "operation": "count"
          },
          {
            "name": "date",
            "field": "time",
            "operation": "min"
          }
        ]
      }
    """)
    val timeSeriesExtendedRequest = http("timeSeriesExtended")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType + "?ignoredFilterIds=date-test-earthquakes-46d60c31-3f27-40c7-80cf-61a581af5545")
			.headers(headers_2)
			.body(timeSeriesExtended)

    val get500 = StringBody("""
      {
        "limitClause": {
          "limit": 500
        },
        "sortClauses": [
          {
            "sortOrder": 1,
            "fieldName": "time"
          }
        ],
        "filter": {
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [],
        "isDistinct": false,
        "aggregates": []
      }
    """)
    val get500Request = http("get500")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(get500)

    val get5000 = StringBody("""
      {
        "limitClause": {
          "limit": 5000
        },
        "sortClauses": [],
        "filter": {
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [],
        "isDistinct": false,
        "aggregates": []
      }
    """)
    val get5000Request = http("get5000")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(get5000)

    val newestRecord = StringBody("""
      {
        "limitClause": {
          "limit": 1
        },
        "sortClauses": [
          {
            "sortOrder": -1,
            "fieldName": "time"
          }
        ],
        "filter": {
          "whereClause": {
            "rhs": null,
            "operator": "!=",
            "lhs": "time",
            "type": "where"
          },
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": true,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [],
        "isDistinct": false,
        "aggregates": []
      }
    """)

    val oldestRecord = StringBody("""
      {
        "limitClause": {
          "limit": 1
        },
        "sortClauses": [
          {
            "sortOrder": 1,
            "fieldName": "time"
          }
        ],
        "filter": {
          "whereClause": {
            "rhs": null,
            "operator": "!=",
            "lhs": "time",
            "type": "where"
          },
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": true,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [],
        "isDistinct": false,
        "aggregates": []
      }
    """)

    val opsClock = StringBody("""
      {
        "sortClauses": [],
        "filter": {
          "whereClause": {
            "rhs": null,
            "operator": "!=",
            "lhs": "time",
            "type": "where"
          },
          "tableName": "earthquakes",
          "databaseName": "test"
        },
        "fields": [
          "*"
        ],
        "ignoreFilters_": false,
        "selectionOnly_": false,
        "ignoredFilterIds_": [],
        "groupByClauses": [
          {
            "name": "day",
            "field": "time",
            "operation": "dayOfWeek",
            "type": "function"
          },
          {
            "name": "hour",
            "field": "time",
            "operation": "hour",
            "type": "function"
          }
        ],
        "isDistinct": false,
        "aggregates": [
          {
            "name": "count",
            "field": "*",
            "operation": "count"
          }
        ]
      }
    """)
    val opsClockRequest = http("opsClock")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType)
			.headers(headers_2)
			.body(opsClock)
    val getFiltersRequest = http("getFilters")
			.get(uri1 + "/services/filterservice/filters/*/*")
			.headers(headers_1)

    val timeFilter = StringBody("""
      {
        "filter": {
          "whereClause": {
            "whereClauses": [
              {
                "rhs": "${startDate}",
                "operator": ">=",
                "lhs": "time",
                "type": "where"
              },
              {
                "rhs": "${endDate}",
                "operator": "<",
                "lhs": "time",
                "type": "where"
              }
            ],
            "type": "and"
          },
          "tableName": "earthquakes",
          "databaseName": "test",
          "filterName": "Timeline - Earthquakes: 5/24/2014 to 6/6/2014"
        },
        "id": "date-test-earthquakes-46d60c31-3f27-40c7-80cf-61a581af5545"
      }
    """)
    val startDate = DateTime.parse("2014-05-12T00:00:00.000Z")
    val endDate = DateTime.parse("2014-06-12T00:00:00.000Z")
    val daily = new Period(0,0,0,1,0,0,0,0)
    def dateRange(from: DateTime, to: DateTime, step: Period): Iterator[DateTime]=Iterator.iterate(from)(_.plus(step)).takeWhile(!_.isAfter(to))

    def reservoirSample[A:Manifest](iterator:Iterator[A], k:Int ): List[A] = {
      val result = new Array[A](k)

      for ((item, index) <- iterator.zipWithIndex) {
        if (index < k) {
          result(index) = item
        } else {
          val s = Random.nextInt(index)
          if (s < k) {
            result(s) = item
          }
        }
      }
      return result.toList
    }

    val timeFeeder = Iterator.continually({
      val validDates = dateRange(startDate, endDate, daily)
      val range = reservoirSample(validDates, 2).sorted
      Map(
        "startDate" -> range.head.toString(),
        "endDate" -> range.tail.head.toString()
      )
    })

    val mapFilter = StringBody("""
      {
        "filter": {
          "whereClause": {
            "whereClauses": [
              {
                "rhs": ${westLongitude},
                "operator": ">=",
                "lhs": "longitude",
                "type": "where"
              },
              {
                "rhs": ${eastLongitude},
                "operator": "<=",
                "lhs": "longitude",
                "type": "where"
              },
              {
                "rhs": ${southLatitude},
                "operator": ">=",
                "lhs": "latitude",
                "type": "where"
              },
              {
                "rhs": ${northLatitude},
                "operator": "<=",
                "lhs": "latitude",
                "type": "where"
              }
            ],
            "type": "and"
          },
          "tableName": "earthquakes",
          "databaseName": "test",
          "filterName": "Map - Earthquakes"
        },
        "id": "map-test-earthquakes-945f3210-882a-4014-83e9-08b5e34b3fe9"
      }
    """)
    val northMin = 47
    val southMax = 30
    val westMax = -125
    val eastMin = -66
    val mapFeeder = Iterator.continually(Map(
      "northLatitude" -> (Random.nextDouble() * (90-northMin) + northMin),
      "southLatitude" -> (Random.nextDouble() * (-90-southMax) + southMax),
      "westLongitude" -> (Random.nextDouble() * (-180-westMax) + westMax),
      "eastLongitude" -> (Random.nextDouble() * (180-eastMin) + eastMin)
    ))

    val barChartFilter = StringBody("""
      {
        "filter": {
          "whereClause": {
            "rhs": "${netType}",
            "operator": "=",
            "lhs": "net",
            "type": "where"
          },
          "tableName": "earthquakes",
          "databaseName": "test",
          "filterName": "BarChart - Earthquakes: Net = ak"
        },
        "id": "barchart-test-earthquakes-2b74b1f7-15a1-4124-80bd-46f99eb484c6"
      }
    """)

    val barChartFeeder = Array(
      Map("netType" -> "ak"),
      Map("netType" -> "ci"),
      Map("netType" -> "hv"),
      Map("netType" -> "ld"),
      Map("netType" -> "mb"),
      Map("netType" -> "nc"),
      Map("netType" -> "nm"),
      Map("netType" -> "nn"),
      Map("netType" -> "pr"),
      Map("netType" -> "se"),
      Map("netType" -> "us"),
      Map("netType" -> "uu"),
      Map("netType" -> "uw")
    ).random

    val updateResources = Seq(
      getFiltersRequest,
      timeSeriesExtendedRequest,
      timeSeriesRequest,
      get5000Request,
      get500Request,
      groupByNetBarRequest,
      groupByNetRequest,
      opsClockRequest,
      countRequest
    )

    val updateResourcesWithoutTime = updateResources.filterNot(e => ((e eq timeSeriesExtendedRequest) || (e eq timeSeriesRequest)))
    val updateResourcesWithoutBarChart = updateResources.filterNot(e => e eq groupByNetBarRequest)

	val scn = scenario("NeonMongoEarthquakes")
		// Initial load
		.exec(http("Load page")
			.get("/neon-gtd/app/")
			.headers(headers_0))
		.pause(4)
		.exec(http("Get table and fields")
			.get("/neon/services/queryservice/tablesandfields/localhost/" + databaseType + "/test")
			.headers(headers_1)
			.resources(http("Clear filters")
			.post(uri1 + "/services/filterservice/clearfilters")
			.headers(headers_2)))
		.pause(1)
		.exec(http("Get filter builder id")
			.get("/neon/services/widgetservice/instanceid?qualifier=filterBuilder")
			.headers(headers_3)
			.resources(Seq(timeSeriesExtendedRequest,
            get500Request,
            http("newestRecord")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType + "?ignoreFilters=true")
			.headers(headers_2)
			.body(newestRecord),
            http("oldestRecord")
			.post(uri1 + "/services/queryservice/query/localhost/" + databaseType + "?ignoreFilters=true")
			.headers(headers_2)
			.body(oldestRecord),
            countRequest,
            opsClockRequest,
            groupByNetBarRequest,
            groupByNetRequest,
            timeSeriesRequest,
            get5000Request):_*))
		.pause(12)
		// Time Filter
        .feed(timeFeeder)
		.exec(http("timeFilter")
			.post("/neon/services/filterservice/replacefilter")
			.headers(headers_2)
			.body(timeFilter)
			.resources(updateResourcesWithoutTime:_*))
		.pause(10)
		// Remove Time Filter
		.exec(http("request_22")
			.post("/neon/services/filterservice/removefilter")
			.headers(headers_22)
			.body(StringBody("date-test-earthquakes-46d60c31-3f27-40c7-80cf-61a581af5545"))
			.resources(updateResourcesWithoutTime:_*))
		.pause(7)
		// Map Filter
        .feed(mapFeeder)
		.exec(http("request_30")
			.post("/neon/services/filterservice/replacefilter")
			.headers(headers_2)
			.body(mapFilter)
			.resources(updateResources:_*))
		.pause(8)
		// Remove Map FIlter
		.exec(http("request_48")
			.post("/neon/services/filterservice/removefilter")
			.headers(headers_22)
			.body(StringBody("map-test-earthquakes-945f3210-882a-4014-83e9-08b5e34b3fe9"))
			.resources(updateResources:_*))
		.pause(15)
		// Network Filter
        .feed(barChartFeeder)
		.exec(http("request_62")
			.post("/neon/services/filterservice/addfilter")
			.headers(headers_2)
			.body(barChartFilter)
			.resources(updateResourcesWithoutBarChart:_*))
		.pause(7)
		// Remove Network Filter
		.exec(http("request_71")
			.post("/neon/services/filterservice/removefilter")
			.headers(headers_22)
			.body(StringBody("barchart-test-earthquakes-2b74b1f7-15a1-4124-80bd-46f99eb484c6"))
			.resources(updateResourcesWithoutBarChart:_*))
		.pause(60)
		// Second Map Filter
        .feed(mapFeeder)
		.exec(http("request_80")
			.post("/neon/services/filterservice/replacefilter")
			.headers(headers_2)
			.body(mapFilter)
			.resources(updateResources:_*))
		.pause(7)
		// Map and Time Filter
        .feed(timeFeeder)
		.exec(http("request_98")
			.post("/neon/services/filterservice/replacefilter")
			.headers(headers_2)
			.body(timeFilter)
			.resources(updateResourcesWithoutTime:_*))
		.pause(10)
        .feed(barChartFeeder)
		// Map Time and Network Filter
		.exec(http("request_106")
			.post("/neon/services/filterservice/addfilter")
			.headers(headers_2)
			.body(barChartFilter)
			.resources(updateResourcesWithoutBarChart:_*))

	//setUp(scn.inject(rampUsers(228) over (2 minutes))).protocols(httpProtocol)
	setUp(scn.inject(atOnceUsers(1))).protocols(httpProtocol)
}
